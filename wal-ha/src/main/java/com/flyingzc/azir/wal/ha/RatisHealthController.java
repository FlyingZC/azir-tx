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
 * One bounded health mission per node, failed to stave off and recovery re-qualified.
 */
final class RatisHealthController implements AutoCloseable {

    private final RatisWalNode node;

    private final RatisHealthOptions options;

    private final ScheduledExecutorService worker;

    private volatile long rounds, isolations, transfers, recoveries;

    private volatile int failed, succeeded;

    private volatile String state = "STARTING", error;

    /**
     * The construction does not create the unbounded queue, and the next round must wait for the last round to be completed.
     *
     * @param node node
     * @param options options
     */
    RatisHealthController(final RatisWalNode node, final RatisHealthOptions options) {
        this.node = node;
        this.options = options;
        worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-health-control").factory());
    }

    /**
     * Enables visible or durable policy recovery to start a cycle job later.
     */
    void start() {
        worker.scheduleWithFixedDelay(this::round, options.intervalMillis(), options.intervalMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Cache status is used for probe readiness, not to perform database queries in probe.
     *
     * @return the operation result
     */
    RatisWalNode.HealthStats stats() {
        return new RatisWalNode.HealthStats(true, node.healthQuarantined(), node.healthCampaignBlocked(), state, failed, succeeded, rounds, isolations, transfers, recoveries, error);
    }

    /**
     * Returns a fixed policy, and only the same configuration can be used for duplicate assembly.
     *
     * @return the operation result
     */
    RatisHealthOptions options() {
        return options;
    }

    /**
     * Single failure changes the limited status and does not write JDBC addresses or receipt to indicators in exception information.
     */
    private void round() {
        try {
            var verdict = node.healthProbe(options);
            if (verdict == RatisWalNode.HealthVerdict.BUSY) {
                state = "WAITING";
                return;
            }
            rounds++;
            if (verdict == RatisWalNode.HealthVerdict.GOOD) {
                failed = 0;
                succeeded = Math.min(options.recoveries(), succeeded + 1);
                if (node.healthQuarantined()) {
                    if (succeeded >= options.recoveries() && node.healthCanRecover()) {
                        node.healthResume(options);
                        recoveries++;
                        state = "HEALTHY";
                    } else {
                        state = "RECOVERING";
                        if (node.healthMaintainIsolation(options)) {
                            transfers++;
                        }
                    }
                } else {
                    state = "HEALTHY";
                }
            } else {
                succeeded = 0;
                failed = Math.min(options.failures(), failed + 1);
                state = "SUSPECT";
                if (failed >= options.failures() || node.healthQuarantined()) {
                    if (!node.healthQuarantined()) {
                        node.healthIsolate();
                        isolations++;
                    }
                    state = "QUARANTINED";
                    if (node.healthMaintainIsolation(options)) {
                        transfers++;
                    }
                }
            }
            error = null;
        } catch (final Exception failure) {
            error = failure.getClass().getSimpleName();
            state = node.healthQuarantined() ? "QUARANTINED" : "PROBE_ERROR";
        }
    }

    /**
     * Do not interrupt the JDBC or WAL, and the consensus is stopped before you leave the task.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("health management did not drain");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("health close interrupted", interrupted);
        }
    }
}
