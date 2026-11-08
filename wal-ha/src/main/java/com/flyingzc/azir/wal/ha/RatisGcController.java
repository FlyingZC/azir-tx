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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

/**
 * Each replica version collection worker does not perform JDBC or version scans on the consensus application thread.
 */
final class RatisGcController implements AutoCloseable {

    private final RatisWalNode node;

    private final RatisGcOptions options;

    private final ScheduledExecutorService worker;

    private volatile RatisTimedSample<RatisWalNode.GcStats> sampled;

    /**
     * Fixed delayed one-way movement without cumulation of the uncompleted cycle.
     *
     * @param node node
     * @param options options
     */
    RatisGcController(final RatisWalNode node, final RatisGcOptions options) {
        this.node = node;
        this.options = options;
        worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-version-gc").factory());
        sampled = RatisTimedSample.initial(new RatisWalNode.GcStats(true, "STARTING", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, true, null, null));
    }

    /**
     * Durable Configure recovery with the same cycle as first enabled.
     */
    void start() {
        worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Idempotent assembly must check the complete configuration.
     *
     * @return the operation result
     */
    RatisGcOptions options() {
        return options;
    }

    /**
     * Probe reproduces cache only, does not capture engine locks or execute SQL; stagnation can also be observed.
     *
     * @return the operation result
     */
    RatisWalNode.GcStats stats() {
        var observation = sampled;
        var current = observation.value();
        long age = observation.ageMillis(System.nanoTime());
        return new RatisWalNode.GcStats(true, current.state(), current.rounds(), current.failures(), current.inspectedKeys(), current.removedRows(),
                current.safePoint(), current.publishedSequence(), current.retiredMemoryVersions(), current.durationMillis(), current.overruns(), age,
                age > options.staleAfterMillis(), current.error(), current.generation());
    }

    /**
     * Generation Skipped while busy or installed, abnormally kept in the desensitization indicator for next cycle retry.
     */
    private void round() {
        var observation = sampled;
        var previous = observation.value();
        long began = System.nanoTime();
        try {
            var result = node.collectVersionGarbage(options);
            if (result == null) {
                sampled =
                        observation.retaining(new RatisWalNode.GcStats(true, "WAITING", previous.rounds(), previous.failures(), previous.inspectedKeys(), previous.removedRows(), previous.safePoint(),
                                previous.publishedSequence(), previous.retiredMemoryVersions(), previous.durationMillis(), previous.overruns(), 0, false, previous.error(), previous.generation()));
                return;
            }
            long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);
            sampled = RatisTimedSample.captured(new RatisWalNode.GcStats(true, "RUNNING", previous.rounds() + 1, previous.failures(), previous.inspectedKeys() + result.progress().inspectedKeys(),
                    previous.removedRows() + result.progress().removedRows(), result.stats().safePoint(), result.stats().publishedSequence(), result.stats().retiredMemoryVersions(),
                    duration, previous.overruns() + (duration > options.workMillis() ? 1 : 0), 0, false, null, result.generation()), System.nanoTime());
        } catch (final Exception failure) {
            sampled = observation.retaining(new RatisWalNode.GcStats(true, "RETRYING", previous.rounds() + 1, previous.failures() + 1, previous.inspectedKeys(), previous.removedRows(),
                    previous.safePoint(), previous.publishedSequence(), previous.retiredMemoryVersions(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began), previous.overruns(), 0, false, failure.getClass().getSimpleName(), previous.generation()));
        }
    }

    /**
     * Stops the production of a new cycle and is emptied of collection, without breaking off shared WAL or JDBC.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("version GC did not drain");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("version GC close interrupted", interrupted);
        }
    }
}
