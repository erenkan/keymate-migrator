package io.keymate;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main migration processing loop.
 *
 * <p>Continuously claims work items from {@link QueueRepository}, processes them with bounded
 * parallelism (Mutiny), and marks each item as success or requeues it with retry metadata.</p>
 *
 * <p>Designed to be restartable and observable; avoids uncontrolled retries and prevents silent
 * data loss by persisting failure state and next-retry timestamps.</p>
 */
@ApplicationScoped
public class ProcessingLoop {

    private static final Logger LOG = Logger.getLogger(ProcessingLoop.class);

    final QueueRepository repo;
    final JobRouter router;
    final SegmentedOutboxRequeueBuffer outboxBuffer;

    public ProcessingLoop(QueueRepository repo,
                          JobRouter router,
                          SegmentedOutboxRequeueBuffer outboxBuffer) {
        this.repo = repo;
        this.router = router;
        this.outboxBuffer = outboxBuffer;

    }

    @ConfigProperty(name = "app.fetch-size", defaultValue = "2000")
    int fetchSize;

    @ConfigProperty(name = "app.concurrency", defaultValue = "64")
    int concurrency;

    @ConfigProperty(name = "app.idle-ms", defaultValue = "2000")
    long idleMs;

    @ConfigProperty(name = "app.claimers", defaultValue = "1")
    int claimers;

    private final List<Cancellable> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Starts the processing loop (typically triggered by Quarkus {@code StartupEvent}).
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("ProcessingLoop already running; start() ignored.");
            return;
        }
        for (int i = 0; i < Math.max(1, claimers); i++) {
            startRunner(i);
        }
        LOG.infof("ProcessingLoop started with %d runner(s), fetch=%d, concurrency=%d, idleMs=%d",
                Math.max(1, claimers), fetchSize, concurrency, idleMs);
    }

    /**
     * Stops the processing loop (typically triggered by Quarkus {@code ShutdownEvent}).
     */
    public void stop() {
        running.set(false);
        subscriptions.forEach(Cancellable::cancel);
        subscriptions.clear();
        LOG.info("ProcessingLoop stopped (all runners cancelled).");
    }

    private void startRunner(int idx) {
        Cancellable c = Multi.createBy().repeating()
                .uni(this::runBatch)
                .indefinitely()
                .subscribe().with(
                        v -> {
                        },
                        err -> {
                            LOG.errorf(err, "Runner %d crashed", idx);
                            // Optional auto-restart: only if still running
                            if (running.get()) {
                                LOG.warnf("Restarting runner %d after crash...", idx);
                                startRunner(idx);
                            }
                        }
                );
        subscriptions.add(c);
        LOG.infof("Runner %d started.", idx);
    }

    /**
     * /**
     *  * Claims a batch of jobs and processes them with bounded concurrency.
     *  *
     *  * @return a {@link Uni} that completes when the current batch has been processed
     */
    private Uni<Void> runBatch() {
        return repo.claimBatch(fetchSize)
                .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)

                // If claimBatch fails (DB error etc.): log it and continue with an empty list
                .onFailure().invoke(e -> LOG.error("claimBatch failed", e))
                .onFailure().recoverWithItem(java.util.Collections.emptyList())

                .onItem().transformToUni(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().voidItem()
                                .onItem().delayIt().by(Duration.ofMillis(idleMs));
                    }

                    // Mutiny 3: chunking for bounded parallelism
                    return Multi.createFrom().iterable(list)
                            // 1) Split jobs into chunks of size concurrency
                            .group().intoLists().of(concurrency)
                            // 2) Process each chunk in parallel (up to concurrency at a time)
                            .onItem().transformToUniAndConcatenate(chunk ->
                                    // 3) Combine jobs in parallel within the chunk (bounded: chunk size)
                                    Multi.createFrom().iterable(chunk)
                                            .select().where(line -> line.payload() != null)   // <-- discard those that are null
                                            .onItem().transformToUniAndMerge(job ->
                                                    router.process(job)
                                                            .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)

                                                            // OK if successful
                                                            .call(() -> repo.markOk(job))
                                                            .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)

                                                            // If REST or typing OK fails: try requeue
                                                            .onFailure().call(err -> repo.requeue(job, err)
                                                                    .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                                                                    .onFailure().call(error ->
                                                                            outboxBuffer.offer(job.table(), job.pk(), job.payload(), job.attempt(), errMessage(error))
                                                                    )
                                                                    .onFailure().invoke(e2 ->
                                                                            LOG.errorf(e2, "Requeue also failed for %s:%d", job.table(), job.pk())
                                                                    )
                                                                    // Even if the requeue fails, continue the flow
                                                                    .onFailure().recoverWithNull().replaceWithVoid()
                                                            )
                                                            .onFailure().invoke(e ->
                                                                    LOG.warnf("Job failed (will continue): %s:%d, err: %s", job.table(), job.pk(), e.getMessage())
                                                            )
                                                            .onFailure().recoverWithUni(Uni.createFrom().voidItem())
                                                            .replaceWithVoid()
                                            )
                                            .collect().asList()
                                            .replaceWithVoid()
                            )
                            .collect().asList()
                            .replaceWithVoid();
                });
    }

    private String errMessage(Throwable t) {
        String m = t == null ? "error" : t.getMessage();
        if (m == null) return "error";
        return m.length() > 500 ? m.substring(0, 500) : m;
    }
}