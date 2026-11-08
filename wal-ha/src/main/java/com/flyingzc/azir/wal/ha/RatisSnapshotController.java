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

package com.flyingzc.azir.wal.ha;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.io.IOException;

/**
 * Periodic missions only observe background mirrors and clean-up results and do not wait for full database export at the schedule line.
 */
final class RatisSnapshotController implements AutoCloseable {

    private final RatisWalNode node;

    private final RatisSnapshotOptions options;

    private final ScheduledExecutorService worker;

    private final ExecutorService verification;

    private volatile CompletableFuture<Long> image, purge;

    private volatile long rounds, snapshots, purges, bytes, firstIndex = -1, lastSnapshot = -1, lastCompleted = System.nanoTime();

    private long purgeStart, requestedBoundary = -1, completedBoundary = -1;

    private volatile String state = "STARTING", error;

    /**
     * A section with an observation line, document scanning and retirement workload is also fixed budget.
     *
     * @param node node
     * @param options options
     */
    RatisSnapshotController(final RatisWalNode node, final RatisSnapshotOptions options) {
        this.node = node;
        this.options = options;
        worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-snapshot-lifecycle").factory());
        // The maximum commit maintenance and mirror request for the same observation wheel is two; a full round must also be accommodated when the first item has not been removed from the work line.
        verification = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2),
                Thread.ofPlatform().daemon().name("wal-snapshot-verification").factory(), new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Enables for the first time the same limited schedule as restarts.
     */
    void start() {
        worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the current policy to fit idempotent checks.
     *
     * @return the operation result
     */
    RatisSnapshotOptions options() {
        return options;
    }

    /**
     * Reads only the cache status and does not allow the monitor to execute a directory scan or database query.
     *
     * @return the operation result
     */
    RatisWalNode.SnapshotStats stats() {
        return new RatisWalNode.SnapshotStats(true, state, bytes, firstIndex, lastSnapshot, rounds, snapshots, purges, image != null && !image.isDone(), node.snapshotLogPressure(), error);
    }

    /**
     * Super watermark back-press new business, mirror failure to keep log and next round continues with retry.
     */
    private void round() {
        try {
            if (!node.snapshotMaintenanceReady()) {
                state = "WAITING";
                return;
            }
            rounds++;
            try {
                bytes = node.measureRaftLogBytes();
            } catch (final IOException unavailable) {
                node.snapshotLogPressure(true);
                throw unavailable;
            }
            firstIndex = node.firstLogIndexForTest();
            node.snapshotLogPressure(bytes >= options.logHighWaterBytes());
            if (purge != null && purge.isDone()) {
                purge.get();
                purge = null;
                completedBoundary = Math.max(completedBoundary, requestedBoundary);
                firstIndex = node.firstLogIndexForTest();
                if (requestedBoundary >= 0 && firstIndex > purgeStart) {
                    purges++;
                }
            }
            if (image != null && image.isDone()) {
                long created = image.get();
                image = null;
                if (created >= 0) {
                    lastSnapshot = created;
                    snapshots++;
                    lastCompleted = System.nanoTime();
                }
            }
            if (image == null && purge == null) {
                long existing = node.snapshotIndex(), applied = node.appliedRaftIndex();
                lastSnapshot = Math.max(lastSnapshot, existing);
                long tail = node.snapshotLogPressure() ? 0 : options.preserveEntries();
                // Keeps all unovercovered records below high watermark, with the end covered only replay optimised and cannot be lifted by back pressure.
                boolean reclaim = existing >= 0 && existing - tail >= firstIndex && existing - tail > completedBoundary;
                purgeStart = firstIndex;
                requestedBoundary = reclaim ? existing - tail : -1;
                // Directory maintenance, completeness check and force do not constitute an watermark observation line; successful borders do not repeat syncs or requests collection.
                purge = CompletableFuture.supplyAsync(() -> {
                    try {
                        node.maintainSnapshotFiles();
                        return reclaim ? node.purgeCoveredSnapshot(existing, tail) : CompletableFuture.completedFuture(-1L);
                    } catch (final IOException failure) {
                        throw new CompletionException(failure);
                    }
                }, verification).thenCompose(java.util.function.Function.identity());
                boolean due = applied > existing
                        && (applied - existing >= options.entryGap() || System.nanoTime() - lastCompleted >= TimeUnit.MILLISECONDS.toNanos(options.maxIntervalMillis()) || node.snapshotLogPressure());
                if (due) {
                    // The requested mirror may also wait for the maintenance lock to be handed over to the same bounded background queue, and only the result handle will be kept on the observation
                    // line.
                    image = CompletableFuture.supplyAsync(() -> {
                        try {
                            return node.requestSnapshotImage();
                        } catch (final IOException failure) {
                            throw new CompletionException(failure);
                        }
                    }, verification).thenCompose(java.util.function.Function.identity());
                    state = "EXPORTING";
                } else {
                    state = reclaim ? "PURGING" : "MAINTAINING";
                }
            } else {
                state = image != null ? "EXPORTING" : requestedBoundary >= 0 ? "PURGING" : "MAINTAINING";
            }
            error = null;
        } catch (final Exception failure) {
            if (image != null && image.isDone()) {
                image = null;
            }
            if (purge != null && purge.isDone()) {
                purge = null;
            }
            error = failure.getClass().getSimpleName();
            state = "RETRYING";
        }
    }

    /**
     * Background mirrors are owned by the status machine, stop observing and do not cancel their shared I/O, before closing the status machine.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("snapshot lifecycle did not drain");
            }
            verification.shutdown();
            if (!verification.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("snapshot verification did not drain");
            }
            var pending = purge;
            if (pending != null) {
                try {
                    pending.get(30, TimeUnit.SECONDS);
                } catch (final ExecutionException finishedWithError) {
                    /*
                     * collection terminated, complete snapshot retained.
                     */ }
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("snapshot lifecycle close interrupted", interrupted);
        } catch (final TimeoutException running) {
            throw new IOException("snapshot purge did not drain", running);
        }
    }
}
