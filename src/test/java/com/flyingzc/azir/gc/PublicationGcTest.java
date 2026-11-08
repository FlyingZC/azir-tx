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

package com.flyingzc.azir.gc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Verify lazy lease, materialization transaction and begin/GC published barrier competitions.
 */
class PublicationGcTest {

    /**
     * The durable storage unknown slot left after GET_LOCK has failed must prevent GC timestamp allocation and safe point propulsion.
     */
    @Test
    void pendingLocalPublicationBlocksSafePointAdvancement() {
        var metadata = new InMemoryTransactionMetadataStore();
        var safePoint = new InMemorySafePointStore(0);
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), safePoint, Map.of());
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        assertThrows(com.flyingzc.azir.common.MvccException.class, gc::advanceSafePoint);
        Assertions.assertEquals(0, safePoint.currentSafePoint());
    }

    /**
     * Pending local commit and Unfinished Center commit protect the recovery lower bounds without blocking old metadata after completion.
     */
    @Test
    void unfinalizedDecisionPinsGcUntilMaterialization() {
        var metadata = new InMemoryTransactionMetadataStore();
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), new InMemorySafePointStore(0), Map.of());
        metadata.createCommitted(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), 20);
        Assertions.assertEquals(9, gc.advanceSafePoint());
        metadata.markFinalized(10);
        assertTrue(gc.advanceSafePoint() >= 100);
        assertTrue(metadata.get(10).isPresent());
    }

    /**
     * The remaining expired records after the bounded lease clean-up remain conservatively blocked and will not exceed a single wheel batch.
     */
    @Test
    void expiredLazySnapshotsAreRemovedInBoundedBatches() {
        var metadata = new InMemoryTransactionMetadataStore();
        metadata.registerSnapshot(1, 1, 1);
        metadata.registerSnapshot(2, 2, 1);
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), new InMemorySafePointStore(0), Map.of(), 1, 0, 1000);
        Assertions.assertEquals(1, gc.advanceSafePoint());
        Assertions.assertEquals(2L, metadata.oldestActiveStartTs().orElseThrow());
        assertTrue(gc.advanceSafePoint() >= 100);
    }

    /**
     * Even if registration is temporarily suspended after assigning startTs, GC can only see the bottom line of protection once registration has been completed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void gcCannotPassSnapshotAllocatedInsidePublicationLock() throws Exception {
        var metadata = new InMemoryTransactionMetadataStore();
        var tso = new AtomicTimestampOracle(100);
        var gc = new GcManager(metadata, tso, new InMemorySafePointStore(0), Map.of());
        var allocated = new CountDownLatch(1);
        var gcStarted = new CountDownLatch(1);
        var register = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var begin = executor.submit(() -> metadata.withPublicationLock(() -> {
                long startTs = tso.nextTimestamp();
                allocated.countDown();
                await(register);
                metadata.registerSnapshot(startTs, startTs, Long.MAX_VALUE);
                return startTs;
            }));
            try {
                assertTrue(allocated.await(5, TimeUnit.SECONDS));
                var advance = executor.submit(() -> {
                    gcStarted.countDown();
                    return gc.advanceSafePoint();
                });
                assertTrue(gcStarted.await(5, TimeUnit.SECONDS));
                assertFalse(advance.isDone());
                register.countDown();
                Assertions.assertEquals(begin.get(5, TimeUnit.SECONDS) - 1, advance.get(5, TimeUnit.SECONDS));
            } finally {
                register.countDown();
            }
        }
    }

    /**
     * Constructs a definitive competition with bounded and retains an interrupted semantic.
     *
     * @param latch latch
     * @throws AssertionError if the operation cannot complete
     */
    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }
}
