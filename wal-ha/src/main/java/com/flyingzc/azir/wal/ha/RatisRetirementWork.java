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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The same maintenance calls for a common count and a single time, with no additional budget to cover the workload at any stage.
 */
final class RatisRetirementWork {

    final RatisRetirementOptions options;

    final long started = System.nanoTime();

    int scanned, removed, inspected, dropped, completed;

    boolean exhausted;

    /**
     * A round of maintaining the independence count.
     *
     * @param options options
     */
    RatisRetirementWork(final RatisRetirementOptions options) {
        this.options = options;
    }

    /**
     * A single I/O hard-duration is driven and this methodology provides a collaboration period.
     *
     * @throws Limit if the operation cannot complete
     */
    void check() throws Limit {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(options.budgetMillis())) {
            exhausted = true;
            throw new Limit();
        }
    }

    /**
     * Scan entry cannot grow indefinitely.
     *
     * @throws Limit if the operation cannot complete
     */
    void scan() throws Limit {
        check();
        if (scanned >= options.scanLimit()) {
            exhausted = true;
            throw new Limit();
        }
        scanned++;
    }

    /**
     * Save a delete budget before destructive I/ O.
     *
     * @throws Limit if the operation cannot complete
     */
    void deleting() throws Limit {
        check();
        if (removed >= options.fileLimit()) {
            exhausted = true;
            throw new Limit();
        }
    }

    /**
     * Full file check also shares the remaining period and cannot use another five minutes budget secretly.
     *
     * @return the operation result
     * @throws Limit if the operation cannot complete
     */
    long remainingMillis() throws Limit {
        check();
        return Math.max(1, options.budgetMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    /**
     * Budget Exhaustion is distinguished from data or damage to ownership and allows the next round to continue.
     */
    static final class Limit extends IOException {

        /**
         * Maintain budget due does not represent an operation rollback.
         */
        Limit() {
            super("retirement work budget exhausted");
        }
    }
}
