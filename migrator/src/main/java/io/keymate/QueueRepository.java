package io.keymate;

import io.smallrye.mutiny.Uni;
import io.keymate.model.InvoiceJob;
import io.keymate.model.Job;
import io.keymate.model.OrderJob;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-backed work-queue repository.
 *
 * <p>Provides atomic "claim", "ack", and "requeue" operations for migration jobs.
 * Requeue persists error context and schedules the next retry time.</p>
 */
@ApplicationScoped
public class QueueRepository {

    private static final Logger LOG = Logger.getLogger(QueueRepository.class);
    final Pool pg;

    public QueueRepository(Pool pg) {
        this.pg = pg;
    }

    @ConfigProperty(name = "app.max-attempts", defaultValue = "10")
    int maxAttempts;

    /**
     * Atomically claims up to {@code size} jobs that are ready to run.
     *
     * <p>Claiming should be implemented to avoid double-processing (e.g. using SKIP LOCKED).</p>
     *
     * @param size maximum number of jobs to claim
     * @return claimed jobs
     */
    public Uni<List<Job>> claimBatch(int size) {
        String sql = """
                WITH picked AS (
                SELECT id
                FROM work_queue
                WHERE next_retry_at <= now()
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT $1
                )
                DELETE FROM work_queue w
                USING picked p
                WHERE w.id = p.id
                RETURNING w.id, w.source_table, w.source_pk, w.payload, w.attempt_count;
                """;

        return pg.preparedQuery(sql).execute(Tuple.of(size))
                .onItem().transform(rows -> {
                    List<Job> list = new ArrayList<>(rows.rowCount());
                    for (Row r : rows) {
                        String table = r.getString("source_table");
                        long pk = r.getLong("source_pk");
                        String payload = r.getJson("payload").toString();
                        int attempt = r.getInteger("attempt_count");
                        switch (table) {
                            case "orders" -> list.add(new OrderJob(pk, payload, attempt));
                            case "invoices" -> list.add(new InvoiceJob(pk, payload, attempt));
                            default -> {
                                LOG.warnf("Unknown table: %s", table);
                            }
                        }
                    }
                    return list;
                });
    }


    public Uni<Void> markOk(Job job) {
        String sql = """
                INSERT INTO processed_log (source_table, source_pk, attempt_count, status, error)
                VALUES ($1, $2, $3, 'OK', NULL)
                ON CONFLICT (source_table, source_pk) DO UPDATE
                        SET status='OK', processed_at=now(), error=NULL
                """;
        return pg.preparedQuery(sql)
                .execute(Tuple.of(job.table(), job.pk(), job.attempt()))
                .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                .onFailure().invoke(e2 ->
                        LOG.errorf(e2, "markOK failed for %s:%d", job.table(), job.pk())
                )
                .replaceWithVoid();
    }


    /**
     * Requeues a job after a failure by recording error context and a computed next-retry timestamp.
     *
     * @param job the failed job
     * @param err the failure cause
     * @return completion signal
     */
    public Uni<Void> requeue(Job job, Throwable err) {
        int attempts = job.attempt() + 1;
        if (attempts > maxAttempts) {
            // mark permanently failed
            String fail = """
                    INSERT INTO processed_log (source_table, source_pk, attempt_count, status, error)
                    VALUES ($1, $2, $3, 'FAILED', LEFT($4, 500))
                    ON CONFLICT (source_table, source_pk) DO UPDATE
                            SET status='FAILED', processed_at=now(), error=EXCLUDED.error
                    """;
            return pg.preparedQuery(fail)
                    .execute(Tuple.of(job.table(), job.pk(), job.attempt(), errMessage(err)))
                    .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                    .onFailure().invoke(e2 ->
                            LOG.errorf(e2, "requeue failed for %s:%d", job.table(), job.pk())
                    )
                    .replaceWithVoid();
        }


        // Random backoff between 1–2 minutes
        @SuppressWarnings("java:S2245")
        int backoffMins = java.util.concurrent.ThreadLocalRandom.current().nextInt(1, 3);
        var nextRetryAt = java.time.OffsetDateTime.now(java.time.ZoneOffset.UTC)
                .plusMinutes(backoffMins);

        String sql = """
                INSERT INTO work_queue (source_table, source_pk, payload, attempt_count, next_retry_at, last_error)
                VALUES ($1, $2, $3, $4, $5, LEFT($6, 500))
                ON CONFLICT (source_table, source_pk) DO UPDATE
                  SET attempt_count = EXCLUDED.attempt_count,
                      next_retry_at = EXCLUDED.next_retry_at,
                      last_error    = EXCLUDED.last_error
                """;
        return pg.preparedQuery(sql)
                .execute(Tuple.of(
                        job.table(),
                        job.pk(),
                        parseToJsonObject(job.payload()),
                        attempts,
                        nextRetryAt,
                        errMessage(err)
                ))
                .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                .onFailure().invoke(e2 ->
                        LOG.errorf(e2, "requeue failed for %s:%d", job.table(), job.pk())
                )
                .replaceWithVoid();
    }

    // New: parameterized requeue (backoff computed in Java)
    public Uni<Void> requeueParams(String table, long pk, String payload,
                                   int attempts, java.time.OffsetDateTime nextRetryAt, String error) {
        final String sql = """
                  INSERT INTO work_queue (source_table, source_pk, payload, attempt_count, next_retry_at, last_error)
                  VALUES ($1, $2, $3, $4, $5, LEFT($6, 500))
                  ON CONFLICT (source_table, source_pk) DO UPDATE
                    SET attempt_count = EXCLUDED.attempt_count,
                        next_retry_at = EXCLUDED.next_retry_at,
                        last_error    = EXCLUDED.last_error
                """;
        return pg.preparedQuery(sql)
                .execute(Tuple.of(table, pk, parseToJsonObject(payload), attempts, nextRetryAt, error))
                .onFailure().retry().withBackOff(Duration.ofMillis(10), Duration.ofMillis(100)).atMost(3)
                .onFailure().invoke(e2 ->
                        LOG.errorf(e2, "requeue with params failed for %s:%d", table, pk)
                )
                .replaceWithVoid();
    }

    private JsonObject parseToJsonObject(String payload) {
        if (payload == null || payload.isBlank()) {
            return new JsonObject();
        }
        try {
            return new JsonObject(payload);
        } catch (Exception e) {
            LOG.errorf("JSON parse failed. Payload: %s, Error: %s", payload, e.getMessage());
            return new JsonObject();
        }
    }

    private String errMessage(Throwable t) {
        String m = t == null ? "error" : t.getMessage();
        if (m == null) return "error";
        return m.length() > 500 ? m.substring(0, 500) : m;
    }
}