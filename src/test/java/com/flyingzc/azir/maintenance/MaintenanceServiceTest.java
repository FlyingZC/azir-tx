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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verify background failed recovery, resources closed to wait and budget borders.
 */
class MaintenanceServiceTest {

    /**
     * Unusually, the movement cannot be permanently cancelled and, when successful, recent errors should be cleared and cumulative failures retained.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failureIsObservableAndNextRoundRetries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var service = new MaintenanceService("test-retry", new MaintenanceOptions(10, 3, 1000), limit -> {
            Assertions.assertEquals(3, limit);
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("injected maintenance failure");
            }
            return new MaintenanceResult(2, 3, 10);
        })) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (service.health().successes() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertTrue(service.health().successes() > 0);
            Assertions.assertEquals(1, service.health().failures());
            assertNull(service.health().lastError());
            Assertions.assertEquals(10, service.health().lastResult().safePoint());
        }
    }

    /**
     * When blocking JDBC does not respond to interruptions, closing must be reported timeout and cannot pretend that it is over.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void closeTimeoutCanBeRetriedAfterTaskExits() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var service = new MaintenanceService("test-close", new MaintenanceOptions(10, 1, 20), limit -> {
            entered.countDown();
            boolean interrupted = false;
            while (release.getCount() != 0) {
                try {
                    release.await();
                } catch (final InterruptedException failure) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            return new MaintenanceResult(0, 0, 0);
        });
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, service::close);
        } finally {
            release.countDown();
        }
        service.close();
        service.close();
    }

    /**
     * Configure error should fail before background thread is created.
     */
    @Test
    void invalidBudgetsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceOptions(0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceOptions(10, 4097, 1));
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceResult(-1, 0, 0));
    }
}
