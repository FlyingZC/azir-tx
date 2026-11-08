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

package com.flyingzc.azir.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background Task running statistical information.
 */
public final class BackgroundTaskStats {

    private final AtomicLong lastRunAtMillis = new AtomicLong();

    private final AtomicLong lastSuccessAtMillis = new AtomicLong();

    private final AtomicInteger runCount = new AtomicInteger();

    private final AtomicInteger failureCount = new AtomicInteger();

    private final AtomicLong lastProcessedCount = new AtomicLong();

    /**
     * Record one successful background mission.
     *
     * @param processedCount processed count
     */
    public void recordSuccess(final long processedCount) {
        long now = System.currentTimeMillis();
        lastRunAtMillis.set(now);
        lastSuccessAtMillis.set(now);
        runCount.incrementAndGet();
        lastProcessedCount.set(processedCount);
    }

    /**
     * Failed to record an background job.
     */
    public void recordFailure() {
        lastRunAtMillis.set(System.currentTimeMillis());
        runCount.incrementAndGet();
        failureCount.incrementAndGet();
    }

    /**
     * Returns the last run time.
     *
     * @return the operation result
     */
    public long lastRunAtMillis() {
        return lastRunAtMillis.get();
    }

    /**
     * Returns the last successful time.
     *
     * @return the operation result
     */
    public long lastSuccessAtMillis() {
        return lastSuccessAtMillis.get();
    }

    /**
     * Returns the number of times you run.
     *
     * @return the operation result
     */
    public int runCount() {
        return runCount.get();
    }

    /**
     * Returns the number of failures.
     *
     * @return the operation result
     */
    public int failureCount() {
        return failureCount.get();
    }

    /**
     * Returns the amount of last processed data.
     *
     * @return the operation result
     */
    public long lastProcessedCount() {
        return lastProcessedCount.get();
    }
}
