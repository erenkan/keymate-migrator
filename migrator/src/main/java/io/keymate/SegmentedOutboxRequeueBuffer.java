package io.keymate;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Disk-backed segmented JSONL outbox used as a safety buffer for requeue operations.
 *
 * <p>All blocking file I/O is executed on the worker pool. Each segment has an offset file so
 * the drainer can resume after restarts without reprocessing already-drained lines.</p>
 *
 * <p>Purpose: keep requeue durable even when the database is temporarily unavailable, while
 * maintaining bounded retry semantics.</p>
 */
@ApplicationScoped
public class SegmentedOutboxRequeueBuffer {

    private static final Logger LOG = Logger.getLogger(SegmentedOutboxRequeueBuffer.class);

    final QueueRepository repo;

    public SegmentedOutboxRequeueBuffer(QueueRepository repo) {
        this.repo = repo;
    }

    // ==== Config ====
    @ConfigProperty(name = "app.outbox.dir", defaultValue = "data/outbox")
    String outboxDir;

    @ConfigProperty(name = "app.outbox.segment-max-bytes", defaultValue = "67108864") // 64MB
    long segmentMaxBytes;

    @ConfigProperty(name = "app.outbox.drain-interval-ms", defaultValue = "5000")
    long drainIntervalMs;

    @ConfigProperty(name = "app.outbox.drain-batch-size", defaultValue = "500")
    int drainBatchSize;

    // Limit the number of parallel upserts to the DB (to avoid overloading the pool)
    @ConfigProperty(name = "app.outbox.db-concurrency", defaultValue = "16")
    int dbConcurrency;

    // ==== Jackson ====
    private final ObjectMapper om = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);

    // ==== Drainer Lock ====
    private FileChannel lockChannel;
    private FileLock lock;

    private final AtomicReference<Cancellable> drainSub = new AtomicReference<>();

    // ==== DTO ====
    public static class Req {
        public String t;
        public long pk;
        public String payload;
        public int attempts;
        public OffsetDateTime next;
        public String err;

        public Req() {
        }

        public Req(String t, long pk, String payload, int attempts, OffsetDateTime next, String err) {
            this.t = t;
            this.pk = pk;
            this.payload = payload;
            this.attempts = attempts;
            this.next = next;
            this.err = err;
        }
    }

    private record Line(Req req, int byteLen) {
    }

    private record Batch(Path seg, Path off, long startOffset, List<Line> items, long segSize) {
    }

    // ===== Lifecycle =====
    public void start() {
        LOG.info("SegmentedOutbox starting...");

        // 1) Create the directory + get a drainer lock (in the worker pool)
        ioRunOnWorker(() -> {
            Files.createDirectories(Path.of(outboxDir));
            acquireDrainerLock(); // file lock: blocking I/O
        }).subscribe().with(
                v -> {
                    boolean canDrain = (lock != null && lock.isValid());
                    LOG.infof("SegmentedOutbox started (dir=%s, drain=%s)",
                            outboxDir, canDrain ? "ENABLED" : "DISABLED");

                    if (!canDrain) return; // Don't start the drain if there is no lock

                    // 2) Drainer loop (single subscribe)
                    Cancellable sub = Multi.createFrom().ticks()
                            .every(java.time.Duration.ofMillis(drainIntervalMs))
                            .onOverflow().drop()
                            // Side effect only: run drain once per tick, item does not affect
                            .call(t -> drainOnce()
                                    .onFailure().invoke(err -> LOG.warn("Drain tick failed", err))
                                    .onFailure().recoverWithUni(Uni.createFrom().voidItem())
                            )
                            .subscribe().with(
                                    v1 -> { /* every tick no-op */ },
                                    e -> LOG.error("Drain stream failed", e)
                            );
                    
                    drainSub.set(sub);
                },
                e -> LOG.error("SegmentedOutbox init failed", e)
        );
    }

    public void stop() {
        Cancellable s = drainSub.getAndSet(null);
        if (s != null) s.cancel();

        // Best-effort final drain → if an error occurs, log it and continue the process
        drainOnce()
                .onFailure().invoke(err -> LOG.warn("Final drain failed", err))
                .onFailure().recoverWithUni(Uni.createFrom().voidItem())
                // Leave Lock in the worker pool (in the same reactive chain)
                .call(() -> ioRunOnWorker(this::releaseDrainerLock))
                .subscribe().with(
                        v -> LOG.info("SegmentedOutbox stopped"),
                        e -> LOG.warn("Lock release failed", e)
                );
    }

    /**
     * Appends a requeue request to the local outbox.
     *
     * <p>This method is non-blocking from the event-loop perspective; actual file I/O is performed
     * on the worker pool.</p>
     */
    @SuppressWarnings("java:S2245")
    public Uni<Void> offer(String table, long pk, String payload, int currentAttempt, String error) {
        int backoffMins = ThreadLocalRandom.current().nextInt(1, 3);
        var nextRetry = OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(backoffMins);
        var req = new Req(table, pk, payload, currentAttempt + 1, nextRetry, error);

        return ioRunOnWorker(() -> appendJsonLineUnsafe(req))
                .onFailure(IOException.class).invoke(e -> LOG.warn("Outbox append IO error", e));
    }

    /**
     * Drains a single batch from the oldest segment and executes corresponding DB operations.
     *
     * <p>Uses an inter-process lock to ensure only one drainer is active.</p>
     */
    private Uni<Void> drainOnce() {
        if (lock == null || !lock.isValid()) return Uni.createFrom().voidItem();

        return ioOnWorker(this::readBatchUnsafe)
                .onFailure(IOException.class).invoke(e -> LOG.warn("readBatch IO error", e))
                .onFailure().recoverWithItem((Batch) null)
                .onItem().transformToUni(batch -> {
                    if (batch == null || batch.items().isEmpty()) return Uni.createFrom().voidItem();

                    return Multi.createFrom().iterable(batch.items())
                            .group().intoLists().of(dbConcurrency)              // bounded parallel
                            .onItem().transformToUniAndConcatenate(chunk ->
                                    Multi.createFrom().iterable(chunk)
                                            .onItem().transformToUniAndMerge(line -> {
                                                if (line == null) return Uni.createFrom().item(0);
                                                if (line.req() == null) {
                                                    // Corrupted/unparsable row: No DB, just advance offset
                                                    return Uni.createFrom().item(line.byteLen());
                                                }
                                                return repo.requeueParams(
                                                                line.req().t, line.req().pk, line.req().payload,
                                                                line.req().attempts, line.req().next, line.req().err)
                                                        .onItem().transform(v -> line.byteLen())
                                                        .onFailure().recoverWithItem(-1); // DB hatası
                                            })
                                            .collect().asList()
                                            // PARTIAL PROGRESS: simply sum the positives, count errors as 0.
                                            .onItem().transform(list -> {
                                                int sum = 0;
                                                for (int b : list) if (b > 0) sum += b;
                                                return sum;
                                            })
                            )
                            .collect().asList()
                            .onItem().transform(list -> {
                                int sum = 0;
                                for (int v : list) sum += v;
                                return sum;
                            })
                            .onItem().invoke(advance ->
                                    LOG.debugf("outbox advance=%d bytes (seg=%s off=%s start=%d)",
                                            advance, batch.seg().getFileName(), batch.off().getFileName(), batch.startOffset())
                            )
                            .onItem().transformToUni(advance -> {
                                if (advance <= 0) {
                                    // to diagnose, log the first element
                                    var first = batch.items().isEmpty() ? null : batch.items().getFirst();
                                    if (first == null) LOG.warn("stuck on: null line");
                                    else if (first.req() == null) LOG.warn("stuck on: parse-failed/null req");
                                    else LOG.warnf("stuck on: %s:%d attempts=%d",
                                                first.req().t, first.req().pk, first.req().attempts);
                                    return Uni.createFrom().voidItem();
                                }
                                return ioRunOnWorker(() -> {
                                    long newOff = batch.startOffset() + advance;
                                    writeOffsetAtomic(batch.off(), newOff);
                                    long segSizeNow = Files.size(batch.seg());
                                    if (newOff >= segSizeNow) deleteSegmentAndOffset(batch.seg(), batch.off());
                                })
                                        .onFailure(IOException.class).invoke(e -> LOG.warn("advanceOffset IO error", e))
                                        .onFailure().recoverWithUni(Uni.createFrom().voidItem());
                            });
                });
    }

    // ===== File I/O (UNSAFE blocking helpers; always call via runSubscriptionOn) =====

    @FunctionalInterface
    interface IORunnable {
        void run() throws IOException;
    }

    private <T> Uni<T> ioOnWorker(java.util.concurrent.Callable<T> supplier) {
        return Uni.createFrom().<T>emitter(em -> {
                    try {
                        em.complete(supplier.call());
                    } catch (Throwable t) {
                        em.fail(t);
                    }
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private Uni<Void> ioRunOnWorker(IORunnable r) {
        return Uni.createFrom().<Void>emitter(em -> {
                    try {
                        r.run();
                        em.complete(null);
                    } catch (Throwable t) {
                        em.fail(t);
                    }
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .replaceWithVoid();
    }

    private Batch readBatchUnsafe() throws IOException {
        Optional<Path> segOpt = findOldestSegmentUnsafe();
        if (segOpt.isEmpty()) return null;

        Path seg = segOpt.get();
        Path off = offsetPath(seg);

        long fileSize = Files.size(seg);
        // If the offset file does not exist, create it with 0
        if (!Files.exists(off)) {
            Files.writeString(off, "0", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
        long startOffset = readOffsetUnsafe(off);
        if (startOffset >= fileSize) {
            deleteSegmentAndOffset(seg, off);
            return null;
        }

        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            long remaining = fileSize - startOffset;
            var map = ch.map(FileChannel.MapMode.READ_ONLY, startOffset, remaining);

            List<Line> lines = new ArrayList<>(drainBatchSize);
            int taken = 0;

            byte[] buf = new byte[8192];
            int len;

            while (taken < drainBatchSize && map.hasRemaining()) {
                len = 0;
                boolean sawLF = false;

                while (map.hasRemaining()) {
                    byte b = map.get();
                    if (b == '\n') {
                        sawLF = true;
                        break;
                    }
                    if (len == buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
                    buf[len++] = b;
                }

                // CRLF, on the other hand, subtracts CR from the string but counts CR+LF in byte calculations
                boolean sawCR = (len > 0 && buf[len - 1] == '\r' && sawLF);
                if (sawCR) len -= 1;

                // Total bytes consumed for this line (body + optional CR + optional LF)
                int byteLen = len + (sawCR ? 1 : 0) + (sawLF ? 1 : 0);

                String lineStr = (len == 0) ? "" : new String(buf, 0, len, StandardCharsets.UTF_8);

                Req req = null;
                try {
                    if (!lineStr.isBlank()) {
                        if (lineStr.charAt(0) == '{') {
                            req = om.readValue(lineStr, Req.class);
                        } else if (lineStr.indexOf('|') > 0) {
                            req = parseLegacyPipeLine(lineStr);
                        } else {
                            throw new IllegalArgumentException("Unknown line format");
                        }
                    }
                } catch (Exception ex) {
                    LOG.warnf(ex, "Malformed line; quarantining");
                    quarantineBadLine(lineStr);
                }

                lines.add(new Line(req, byteLen));
                taken++;

                // end of file and if \n does not exist: last line also processed, exit
                if (!sawLF && !map.hasRemaining()) break;
            }

            return new Batch(seg, off, startOffset, lines, fileSize);
        }
    }

    private Optional<Path> findOldestSegmentUnsafe() throws IOException {
        try (var s = Files.list(Path.of(outboxDir))) {
            return s.filter(this::isSegment)
                    .min(Comparator.comparingInt(this::segmentIndex));
        }
    }

    private boolean isSegment(Path p) {
        String n = p.getFileName().toString();
        return n.startsWith("outbox-") && n.endsWith(".log");
    }

    private Path segmentPath(int idx) {
        return Path.of(outboxDir, String.format("outbox-%06d.log", idx));
    }

    private int segmentIndex(Path seg) {
        String name = seg.getFileName().toString();
        String num = name.substring("outbox-".length(), name.length() - ".log".length());
        return Integer.parseInt(num);
    }

    private Path offsetPath(Path seg) {
        return seg.getParent().resolve(seg.getFileName().toString().replace(".log", ".offset"));
    }

    private long readOffsetUnsafe(Path off) throws IOException {
        if (!Files.exists(off)) return 0L;
        String s = Files.readString(off, StandardCharsets.UTF_8).trim();
        if (s.isEmpty()) return 0L;
        return Long.parseLong(s);
    }

    private void writeOffsetAtomic(Path off, long value) throws IOException {
        Path dir = off.getParent();
        if (dir != null) Files.createDirectories(dir);

        Path tmp = off.resolveSibling(off.getFileName().toString() + ".tmp");
        Files.writeString(tmp, Long.toString(value), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        try {
            Files.move(tmp, off, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // Safe fallback if FS doesn't support atomic transport
            Files.move(tmp, off, StandardCopyOption.REPLACE_EXISTING);
        }
    }


    private void deleteSegmentAndOffset(Path seg, Path off) throws IOException {
        Files.deleteIfExists(seg);
        Files.deleteIfExists(off);
        LOG.infof("Outbox segment deleted: %s", seg.getFileName());
    }

    private Req parseLegacyPipeLine(String line) {
        String[] parts = line.split("\\|", 6);
        if (parts.length < 5) throw new IllegalArgumentException("legacy line too short");
        String table = parts[0];
        long pk = Long.parseLong(parts[1]);
        String payload = parts[2];
        int attempts = Integer.parseInt(parts[3]);
        OffsetDateTime next = OffsetDateTime.parse(parts[4]);
        String err = parts.length >= 6 ? parts[5] : null;
        return new Req(table, pk, payload, attempts, next, err);
    }

    private void quarantineBadLine(String raw) {
        try {
            Path bad = Path.of(outboxDir, "outbox.bad");
            Files.writeString(bad, raw + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignore) {
            // Writing the quarantine report is optional; let's not disrupt the process if it fails
        }
    }

    private final java.util.concurrent.locks.Lock appendLock = new java.util.concurrent.locks.ReentrantLock();

    private void appendJsonLineUnsafe(Req r) throws IOException {
        final String json;
        try {
            json = om.writeValueAsString(r);
        } catch (Exception e) {
            throw new IOException("serialize failed", e);
        }
        final byte[] data = (json + "\n").getBytes(StandardCharsets.UTF_8);

        final Path seg = ensureActiveSegmentUnsafe(data.length);

        appendLock.lock();
        try (FileChannel ch = FileChannel.open(
                seg,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {

            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                int written = ch.write(buf);
                if (written == 0) {
                    Thread.onSpinWait();
                }
            }
        } finally {
            appendLock.unlock();
        }
    }

    private Path ensureActiveSegmentUnsafe(int appendBytes) throws IOException {
        Optional<Path> newest = findNewestSegmentUnsafe();
        if (newest.isEmpty()) {
            Path first = segmentPath(1);
            Files.createFile(first);
            return first;
        }
        Path seg = newest.get();
        long size = Files.size(seg);
        if (size + appendBytes <= segmentMaxBytes) return seg;

        int nextIdx = segmentIndex(seg) + 1;
        Path next = segmentPath(nextIdx);
        Files.createFile(next);
        return next;
    }

    private Optional<Path> findNewestSegmentUnsafe() throws IOException {
        try (var s = Files.list(Path.of(outboxDir))) {
            return s.filter(this::isSegment)
                    .max(Comparator.comparingInt(this::segmentIndex));
        }
    }

    // ===== Lock (blocking; only used in start/stop) =====
    private void acquireDrainerLock() throws IOException {
        Path lockPath = Path.of(outboxDir, "drainer.lock");
        lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            lock = lockChannel.tryLock();
            if (lock != null) LOG.info("Outbox drainer lock acquired.");
            else LOG.warn("Outbox drainer lock NOT acquired. This instance will not drain.");
        } catch (IOException e) {
            LOG.warn("Outbox drainer lock failed", e);
        }
    }

    private void releaseDrainerLock() {
        try {
            if (lock != null && lock.isValid()) lock.release();
        } catch (IOException ignore) {
        }
        try {
            if (lockChannel != null && lockChannel.isOpen()) lockChannel.close();
        } catch (IOException ignore) {
        }
    }
}
