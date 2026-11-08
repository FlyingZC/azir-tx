/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.maintenance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.util.Objects;

import java.util.function.IntFunction;

/**
 * One-way bounded maintenance service; failure can be observed and the next round continues, and closure must wait for a real exit.
 */
public final class MaintenanceService implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(MaintenanceService.class.getName());

    private final ScheduledExecutorService executor;

    private final ScheduledFuture<?> future;

    private final MaintenanceOptions options;

    private volatile Health health = new Health(0, 0, null, null, null);

    /**
     * Use a fixed delay to start; different examples can be moved in parallel and coordinated by the database protocol.
     *
     * @param name name
     * @param options options
     * @param work work
     */
    public MaintenanceService(final String name, final MaintenanceOptions options, final IntFunction<MaintenanceResult> work) {
        this.options = Objects.requireNonNull(options, "options");
        Objects.requireNonNull(work, "work");
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("azir-maintenance-" + name).factory());
        try {
            future = executor.scheduleWithFixedDelay(() -> run(work), options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS);
        } catch (final RuntimeException | Error failure) {
            try {
                executor.shutdownNow();
            } catch (final RuntimeException | Error cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /**
     * Obtains unblocked recent maintenance results and anomalies that do not expose database connection information.
     *
     * @return the operation result
     */
    public Health health() {
        Health current = health;
        if ((future.isDone() || executor.isShutdown()) && current.lastError() == null) {
            return new Health(current.successes(), current.failures(), current.lastFinished(), current.lastResult(), "MaintenanceStopped");
        }
        return current;
    }

    /**
     * A failed round cannot permanently cancel a follow-up dispatch, nor can it be swallowed up in silence.
     *
     * @param work work
     */
    private void run(final IntFunction<MaintenanceResult> work) {
        Health old = health;
        try {
            MaintenanceResult result = work.apply(options.batchSize());
            health = new Health(old.successes() + 1, old.failures(), Instant.now(), result, null);
        } catch (final RuntimeException failure) {
            health = new Health(old.successes(), old.failures() + 1, Instant.now(), old.lastResult(), failure.getClass().getSimpleName());
            // The original JDBC anomaly may contain repeat keys or SQL payload, and the General Maintenance Log only output the type of anomaly.
            LOG.log(System.Logger.Level.ERROR, "后台事务维护失败，后续周期将重试，异常类型：{0}", failure.getClass().getSimpleName());
        } catch (final Error fatal) {
            // The fatal error is still assigned to the dispatcher to stop, but the health interface must immediately display a stopped failure.
            health = new Health(old.successes(), old.failures() + 1, Instant.now(), old.lastResult(), fatal.getClass().getSimpleName());
            LOG.log(System.Logger.Level.ERROR, "后台事务维护因致命错误停止，异常类型：{0}", fatal.getClass().getSimpleName());
            throw fatal;
        }
    }

    /**
     * Abort background jobs and wait for exit; timeout visible error reporting does not allow callers to close connection pool.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        future.cancel(true);
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(options.closeTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("maintenance did not stop within close budget; keep pools open and retry close");
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while stopping maintenance", failure);
        }
    }

    /**
     * Maintenance of health snapshot; recent failures remain until the next success and cumulatively fail.
     *
     * @param successes successes
     * @param failures failures
     * @param lastFinished last finished
     * @param lastResult last result
     * @param lastError last error
     */
    public record Health(long successes, long failures, Instant lastFinished, MaintenanceResult lastResult, String lastError) {
    }
}
