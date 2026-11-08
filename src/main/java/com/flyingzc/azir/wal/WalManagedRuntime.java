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

package com.flyingzc.azir.wal;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Owns the WAL runtime and its bounded background maintenance lifecycle.
 */
public final class WalManagedRuntime implements AutoCloseable {

    private final WalMvccRuntime runtime;

    private final WalOptions options;

    private final WalRetentionOptions retention;

    private final WalMaintenanceOptions maintenance;

    private final Object wake = new Object();

    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();

    private final Thread worker;

    private volatile Sample sample;

    private volatile boolean running = true;

    private volatile boolean closing;

    private volatile boolean closed;

    /**
     * Verify, maintenance capabilities for transaction, timeout and bounded at all backends before creating logs.
     *
     * @param directory directory
     * @param stores stores
     * @param options options
     * @param retention retention
     * @param maintenance maintenance
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static WalManagedRuntime open(final Path directory, Map<String, WalMaterializedStore> stores, final WalOptions options,
                                         final WalRetentionOptions retention, final WalMaintenanceOptions maintenance) {
        stores = Map.copyOf(stores);
        java.util.Objects.requireNonNull(maintenance);
        for (var store : stores.values()) {
            if (!store.supportsBoundedMaintenance()) {
                throw new IllegalArgumentException("managed backend requires bounded maintenance");
            }
            store.validateProduction();
        }
        return attach(WalMvccRuntime.open(directory, stores, options, retention), options, retention, maintenance);
    }

    /**
     * The pure local mode is still subject to version memory budget and automatically maintains the only authority version as Cache.
     *
     * @param directory directory
     * @param shards shards
     * @param options options
     * @param retention retention
     * @param maintenance maintenance
     * @return the operation result
     */
    public static WalManagedRuntime open(final Path directory, final Set<String> shards, final WalOptions options,
                                         final WalRetentionOptions retention, final WalMaintenanceOptions maintenance) {
        java.util.Objects.requireNonNull(maintenance);
        return attach(WalMvccRuntime.open(directory, shards, options, retention), options, retention, maintenance);
    }

    /**
     * Releases a new run when the assembly fails, without leaving a directory lock or an background workspace.
     *
     * @param runtime runtime
     * @param options options
     * @param retention retention
     * @param maintenance maintenance
     * @return the operation result
     */
    private static WalManagedRuntime attach(final WalMvccRuntime runtime, final WalOptions options, final WalRetentionOptions retention, final WalMaintenanceOptions maintenance) {
        try {
            return new WalManagedRuntime(runtime, options, retention, maintenance);
        } catch (final Throwable failure) {
            runtime.close();
            throw failure;
        }
    }

    /**
     * The sole maintainer can not compete with multiple unbounded time jobs after initializing the observation snapshot.
     *
     * @param runtime runtime
     * @param options options
     * @param retention retention
     * @param maintenance maintenance
     */
    private WalManagedRuntime(final WalMvccRuntime runtime, final WalOptions options, final WalRetentionOptions retention, final WalMaintenanceOptions maintenance) {
        this.runtime = runtime;
        this.options = options;
        this.retention = retention;
        this.maintenance = maintenance;
        sample = new Sample(runtime.stats(), System.nanoTime(), 0, 0, 0, 0, 0, 0, null);
        worker = Thread.ofVirtual().name("azir-wal-maintenance").start(this::maintain);
    }

    /**
     * Returns the new mode transaction API; the data source and mode entry is still selected by application.
     *
     * @return the operation result
     */
    public WalMvccRuntime runtime() {
        return runtime;
    }

    /**
     * Sets the read-only maintenance status of this process, which is reset by application configuration when restarted, and is not segregated as a high-available writer.
     *
     * @param value value to write
     */
    public void setReadOnly(final boolean value) {
        runtime.setReadOnly(value);
    }

    /**
     * Single-cycle bounded collection, with an error in health status and a fixed rate of retry for the next cycle.
     */
    private void maintain() {
        long cycles = 0;
        long failures = 0;
        long keys = 0;
        long rows = 0;
        long overruns = 0;
        while (running) {
            try {
                synchronized (wake) {
                    if (running) {
                        wake.wait(maintenance.intervalMillis());
                    }
                }
                if (!running) {
                    break;
                }
                var lifecycle = runtime.lifecycle();
                if (lifecycle.closed() || lifecycle.fatal() != null) {
                    break;
                }
                if (lifecycle.backup()) {
                    continue;
                }
                long start = System.nanoTime();
                String error = null;
                try {
                    var progress = runtime.collectGarbageBounded(maintenance.keyLimit(), maintenance.rowLimit(), maintenance.workMillis());
                    keys += progress.inspectedKeys();
                    rows += progress.removedRows();
                } catch (final RuntimeException failure) {
                    failures++;
                    error = diagnostic(failure);
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                if (duration > maintenance.workMillis()) {
                    overruns++;
                }
                cycles++;
                if (running) {
                    sample = new Sample(runtime.stats(), System.nanoTime(), cycles, failures, keys, rows, duration, overruns, error);
                }
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
                return;
            } catch (final RuntimeException failure) {
                Sample previous = sample;
                sample = new Sample(previous.stats(), previous.capturedNanos(), cycles, ++failures, keys, rows, previous.durationMillis(), overruns, diagnostic(failure));
            }
        }
    }

    /**
     * Nonblocking Read the current state and recent sampling, and expired samples cannot continue to be declared ready.
     *
     * @return the operation result
     */
    public Health health() {
        Sample current = sample;
        var live = runtime.lifecycle();
        var stats = current.stats();
        var warnings = new ArrayList<String>();
        long age = Math.max(0, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - current.capturedNanos()));
        warn(warnings, "wal_budget", stats.walBytes(), options.maxWalBytes());
        warn(warnings, "receipt_archive_budget", stats.receiptArchiveBytes(), retention.maxReceiptArchiveBytes());
        warn(warnings, "pending_receipts", stats.pendingReceipts(), retention.maxPendingReceipts());
        warn(warnings, "version_memory", stats.retainedBytes() + stats.bufferedBytes(), options.maxMemoryBytes());
        warn(warnings, "materialization_lag", stats.materializationLag(), options.maxMaterializationLag());
        if (stats.oldestActiveMillis() >= options.transactionTimeoutMillis() * .8) {
            warnings.add("old_snapshot_or_queued_decision");
        }
        String failure = live.fatal() != null ? live.fatal()
                : current.error() != null ? current.error()
                        : live.materializationFailure() != null ? live.materializationFailure() : live.checkpointFailure();
        State state;
        if (closed || live.closed()) {
            state = State.CLOSED;
        } else if (live.fatal() != null) {
            state = State.FAILED;
        } else if (closing || !live.accepting()) {
            state = State.CLOSING;
        } else if (live.backup()) {
            state = State.BACKUP;
        } else if (age > maintenance.staleAfterMillis() || failure != null || live.proposalBlocked()) {
            state = State.DEGRADED;
        } else if (live.readOnly()) {
            state = State.READ_ONLY;
        } else if (stats.walBytes() >= options.maxWalBytes() || stats.receiptArchiveBytes() >= retention.maxReceiptArchiveBytes()
                || stats.pendingReceipts() >= retention.maxPendingReceipts() || stats.materializationLag() >= options.maxMaterializationLag()) {
            state = State.BACKPRESSURE;
        } else {
            state = State.RUNNING;
        }
        return new Health(state, state != State.FAILED && state != State.CLOSED, state == State.RUNNING, age, current.cycles(), current.failures(), warnings, failure);
    }

    /**
     * The exact refusal is still determined by the actual bytes budget of the commit path according to the configuration watermark.
     *
     * @param warnings warnings
     * @param name name
     * @param used used
     * @param limit maximum number of items
     */
    private void warn(final List<String> warnings, final String name, final long used, final long limit) {
        if ((double) used / limit * 100 >= maintenance.warningPercent()) {
            warnings.add(name);
        }
    }

    /**
     * Output of Prometheus text that does not need to be relied upon, all from the recently completed snapshot.
     *
     * @return the operation result
     */
    public String metrics() {
        Sample current = sample;
        var stats = current.stats();
        var health = health();
        var text = new StringBuilder();
        metric(text, "ready", health.ready() ? 1 : 0);
        metric(text, "alive", health.alive() ? 1 : 0);
        metric(text, "read_only", runtime.lifecycle().readOnly() ? 1 : 0);
        metric(text, "health_sample_age_milliseconds", health.sampleAgeMillis());
        metric(text, "published_sequence", stats.publishedSequence());
        metric(text, "safe_point", stats.safePoint());
        metric(text, "oldest_active_milliseconds", stats.oldestActiveMillis());
        metric(text, "wal_bytes", stats.walBytes());
        metric(text, "wal_budget_bytes", options.maxWalBytes());
        metric(text, "receipt_archive_bytes", stats.receiptArchiveBytes());
        metric(text, "receipt_archive_budget_bytes", retention.maxReceiptArchiveBytes());
        metric(text, "pending_receipts", stats.pendingReceipts());
        metric(text, "version_retained_bytes", stats.retainedBytes());
        metric(text, "buffered_bytes", stats.bufferedBytes());
        metric(text, "hot_cache_bytes", stats.hotCacheBytes());
        metric(text, "materialization_lag", stats.materializationLag());
        metric(text, "active_transactions", stats.activeTransactions());
        metric(text, "maintenance_cycles_total", current.cycles());
        metric(text, "maintenance_failures_total", current.failures());
        metric(text, "gc_inspected_keys_total", current.inspectedKeys());
        metric(text, "gc_removed_rows_total", current.removedRows());
        metric(text, "memory_retired_versions_total", stats.retiredMemoryVersions());
        metric(text, "maintenance_duration_milliseconds", current.durationMillis());
        metric(text, "maintenance_overruns_total", current.overruns());
        metric(text, "cache_hits_total", stats.cacheHits());
        metric(text, "cache_misses_total", stats.cacheMisses());
        return text.toString();
    }

    /**
     * The fixed name is only a number and does not write the user key, data source address or abnormal stack to an indicator label.
     *
     * @param output output
     * @param name name
     * @param value value to write
     */
    private static void metric(final StringBuilder output, final String name, final long value) {
        output.append("azir_wal_").append(name).append(' ').append(value).append('\n');
    }

    /**
     * The public phase error code and SQL status does not bring SQL text, key or connection receipt into the health response.
     *
     * @param failure failure
     * @return the operation result
     */
    private static String diagnostic(final RuntimeException failure) {
        String result = failure.getClass().getSimpleName();
        if (failure instanceof WalMvccException wal) {
            result += ":" + wal.errorCode().name();
        }
        Throwable cause = failure;
        for (int depth = 0; depth < 8 && cause != null; depth++, cause = cause.getCause()) {
            if (cause instanceof java.sql.SQLException sql) {
                String state = sql.getSQLState();
                return result + ":sqlstate=" + (state != null && state.matches("[A-Z0-9]{5}") ? state : "unknown") + ":vendor=" + sql.getErrorCode();
            }
        }
        return result;
    }

    /**
     * Stops new transaction and waits for the maintainer to exit, then empty commit and materialization; timeout does not release active directory locks.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        closeGuard.lock();
        try {
            if (closed) {
                return;
            }
            closing = true;
            running = false;
            runtime.stopAccepting();
            synchronized (wake) {
                wake.notifyAll();
            }
            try {
                worker.join(30_000);
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while closing maintenance", failure);
            }
            if (worker.isAlive()) {
                throw new IllegalStateException("maintenance did not stop; WAL directory remains locked");
            }
            runtime.close();
            closed = true;
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * The current state of service reflects operational capability and does not mean that all production acceptances have been completed.
     */
    public enum State {
        RUNNING, READ_ONLY, BACKUP, BACKPRESSURE, DEGRADED, CLOSING, CLOSED, FAILED
    }

    /**
     * The health query does not access database, does not wait for the run-time reading lock and is suitable for mapping to be ready and live probe.
     *
     * @param state state
     * @param alive alive
     * @param ready ready
     * @param sampleAgeMillis sample age millis
     * @param maintenanceCycles maintenance cycles
     * @param maintenanceFailures maintenance failures
     * @param warnings warnings
     * @param failure failure
     */
    public record Health(State state, boolean alive, boolean ready, long sampleAgeMillis,
            long maintenanceCycles, long maintenanceFailures, List<String> warnings, String failure) {

        /**
         * Fixed alarm list to avoid callers changing service observation status.
         *
         * @param state state
         * @param alive alive
         * @param ready ready
         * @param sampleAgeMillis sample age millis
         * @param maintenanceCycles maintenance cycles
         * @param maintenanceFailures maintenance failures
         * @param warnings warnings
         * @param failure failure
         */
        public Health {
            warnings = List.copyOf(warnings);
        }
    }

    /**
     * The whole snapshot at a time when the sample is finished, and the count is kept in line with the time when the cross-line is released.
     *
     * @param stats stats
     * @param capturedNanos captured nanos
     * @param cycles cycles
     * @param failures failures
     * @param inspectedKeys inspected keys
     * @param removedRows removed rows
     * @param durationMillis duration millis
     * @param overruns overruns
     * @param error error
     */
    private record Sample(WalMvccRuntime.Stats stats, long capturedNanos, long cycles, long failures,
            long inspectedKeys, long removedRows, long durationMillis, long overruns, String error) {
    }
}
