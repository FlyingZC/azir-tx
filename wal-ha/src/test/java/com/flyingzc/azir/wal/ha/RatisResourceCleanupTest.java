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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks the retry that failed to close, relies on protection and original anomalies, and avoid infer release by only closing marks.
 */
class RatisResourceCleanupTest {

    /**
     * A second real release remains to be implemented after the initial release has failed and, when successful, re-opening will not be carried out.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void leaseReleaseFailureRemainsRetryable() throws Exception {
        var calls = new AtomicInteger();
        var lease = new WalReplicaBackendFactory.Lease(Map.of(), () -> {
            if (calls.incrementAndGet() == 1) {
                throw new IOException("first close failed");
            }
        });
        assertThrows(IOException.class, lease::close);
        lease.close();
        lease.close();
        Assertions.assertEquals(2, calls.get());
    }

    /**
     * Keep namespace protection when the pre-release failed and allow retirement only after the pool release has been successful.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void failedPoolCloseCannotReleaseNamespaceProtection() throws Exception {
        var keys = List.of("cleanup-test-" + UUID.randomUUID());
        var guard = MysqlNamespaceLeases.open(keys);
        var allowed = new AtomicBoolean();
        var sequence = new RatisCloseSequence(() -> {
            if (!allowed.get()) {
                throw new IOException("pool still running");
            }
        }, guard::close);
        try {
            assertThrows(IOException.class, sequence::close);
            assertThrows(IOException.class, () -> MysqlNamespaceLeases.retire(keys));
        } finally {
            allowed.set(true);
            sequence.close();
        }
        try (var retired = MysqlNamespaceLeases.retire(keys)) {
            assertNotNull(retired);
        }
    }

    /**
     * The sequential clean-up of the failure phase of the reservation does not skip the relying stage and does not repeat the stage where implementation has been completed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void cleanupRetriesOnlyTheIncompleteSuffix() throws Exception {
        var events = new ArrayList<String>();
        var calls = new AtomicInteger();
        var sequence = new RatisCloseSequence(() -> events.add("producer-stopped"), () -> {
            if (calls.incrementAndGet() == 1) {
                events.add("consumer-busy");
                throw new IOException("busy");
            }
            events.add("consumer-drained");
        }, () -> events.add("storage-closed"));
        assertThrows(IOException.class, sequence::close);
        Assertions.assertEquals(List.of("producer-stopped", "consumer-busy"), events);
        sequence.close();
        sequence.close();
        Assertions.assertEquals(List.of("producer-stopped", "consumer-busy", "consumer-drained", "storage-closed"), events);
    }

    /**
     * When the inner layer is not released, the outer resources cannot be closed early and the original start-up reasons are retained after retry has been completed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void nestedStartupCleanupPreservesCauseAndDependencyOrder() throws Exception {
        var primary = new IOException("startup failed");
        var allowed = new AtomicBoolean();
        var outerClosed = new AtomicBoolean();
        var nested = new RatisStartupException(primary, () -> {
            if (!allowed.get()) {
                throw new IOException("inner resource busy");
            }
        });
        var failure = RatisStartupException.afterFailure(nested, () -> outerClosed.set(true));
        var pending = Assertions.assertInstanceOf(RatisStartupException.class, failure);
        Assertions.assertSame(primary, pending.getCause().getCause());
        assertTrue(pending.cleanupPending());
        assertFalse(outerClosed.get());
        allowed.set(true);
        pending.retryCleanup();
        pending.retryCleanup();
        assertFalse(pending.cleanupPending());
        assertFalse(nested.cleanupPending());
        assertTrue(outerClosed.get());
    }
}
