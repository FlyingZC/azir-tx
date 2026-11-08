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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;

/**
 * Checks observations for freshness using a single tone of certainty without the need for linear sleep, database or Raft services.
 */
class RatisTimedSampleTest {

    /**
     * Reader When old snapshot is retained, the new sample cannot refresh the age of the old data.
     */
    @Test
    void replacingSampleCannotRefreshAnAlreadyReadObservation() {
        var old = RatisTimedSample.captured("old", 0);
        var fresh = RatisTimedSample.captured("fresh", TimeUnit.SECONDS.toNanos(20));
        long now = TimeUnit.SECONDS.toNanos(21);
        Assertions.assertEquals(21_000, old.ageMillis(now));
        Assertions.assertEquals(1000, fresh.ageMillis(now));
        Assertions.assertEquals("old", old.value());
    }

    /**
     * Failures and skippings change diagnosis only, not renewing the old generation indicator.
     */
    @Test
    void failureAndBusyUpdatesRetainTheLastSuccessfulCapture() {
        var original = RatisTimedSample.captured("generation-a", TimeUnit.SECONDS.toNanos(1));
        var failed = original.retaining("probe-error").retaining("storage-busy");
        Assertions.assertEquals(20_000, failed.ageMillis(TimeUnit.SECONDS.toNanos(21)));
        Assertions.assertEquals(Long.MAX_VALUE, RatisTimedSample.initial("starting").retaining("busy").ageMillis(0));
    }

    /**
     * Zero and negative numbers are legitimate single-tempo moments, with slow detection not recommenced from the time of release.
     */
    @Test
    void validClockValuesAndSlowProbeKeepTheirOriginalBoundary() {
        Assertions.assertEquals(1000, RatisTimedSample.captured("zero", 0).ageMillis(TimeUnit.SECONDS.toNanos(1)));
        Assertions.assertEquals(2000, RatisTimedSample.captured("negative", -TimeUnit.SECONDS.toNanos(1)).ageMillis(TimeUnit.SECONDS.toNanos(1)));
        Assertions.assertEquals(30_000, RatisTimedSample.captured("slow-proof", 0).ageMillis(TimeUnit.SECONDS.toNanos(30)));
    }
}
