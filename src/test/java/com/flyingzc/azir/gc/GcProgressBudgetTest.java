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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimum progress using controlled monotonic clock verify small budget shard and metadata collection.
 */
class GcProgressBudgetTest {

    /**
     * The control phase and each work unit exceed one millisecond and the rotation still covers all shard and metadata.
     */
    @Test
    void oneMillisecondBudgetStillRotatesThroughAllWork() {
        var fixture = new Fixture();
        fixture.records.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), 2);
        fixture.records.markFinalized(1);
        var gc = fixture.manager(Map.of("a", fixture.adapter("a", false), "b", fixture.adapter("b", false)));
        Assertions.assertEquals(1, gc.gcOnce(1));
        Assertions.assertEquals(1, gc.gcOnce(1));
        Assertions.assertEquals(0, gc.gcOnce(1));
        Assertions.assertEquals(List.of("a", "b", "metadata"), fixture.work);
        assertTrue(fixture.records.get(1).isEmpty());
        Assertions.assertEquals(1, gc.gcOnce(1));
        Assertions.assertEquals(List.of("a", "b", "metadata", "a"), fixture.work);
    }

    /**
     * Cursor must also move forward when an shard continues to fail and cannot starve to another shard or metadata cleanup.
     */
    @Test
    void failingShardDoesNotKeepTheNextRoundOnTheSameWork() {
        var fixture = new Fixture();
        var gc = fixture.manager(Map.of("a", fixture.adapter("a", true), "b", fixture.adapter("b", false)));
        assertThrows(IllegalStateException.class, () -> gc.gcOnce(1));
        Assertions.assertEquals(1, gc.gcOnce(1));
        Assertions.assertEquals(0, gc.gcOnce(1));
        Assertions.assertEquals(List.of("a", "b", "metadata"), fixture.work);
    }

    /**
     * If shard is not involved, a metadata cleanup batch must be performed after the long control phase.
     */
    @Test
    void metadataOnlyGcAlwaysAttemptsItsWorkUnit() {
        var fixture = new Fixture();
        var gc = fixture.manager(Map.of());
        Assertions.assertEquals(0, gc.gcOnce(1));
        Assertions.assertEquals(List.of("metadata"), fixture.work);
    }

    /**
     * Advancing virtual time by calling behaviour, does not depend on hibernation, linear movement or real database.
     */
    private static final class Fixture {

        private final AtomicLong nanos = new AtomicLong();

        private final InMemoryTransactionMetadataStore records = new InMemoryTransactionMetadataStore();

        private final List<String> work = new ArrayList<>();

        /**
         * Construct the GC of one millisecond budget, with a fixed consumption of 10 milliseconds during the control phase.
         *
         * @param adapters adapters
         * @return the operation result
         */
        private GcManager manager(final Map<String, StorageAdapter> adapters) {
            var watermark = new InMemorySafePointStore(0);
            SafePointStore slowControl = new SafePointStore() {

                /**
                 * Returns real test watermark.
                 *
                 * @return the operation result
                 */
                @Override
                public long currentSafePoint() {
                    return watermark.currentSafePoint();
                }

                /**
                 * Simulates slow control surface, but does not consume real time.
                 *
                 * @param candidate candidate
                 * @return the operation result
                 */
                @Override
                public long advanceTo(final long candidate) {
                    nanos.addAndGet(10_000_000);
                    return watermark.advanceTo(candidate);
                }
            };
            return new GcManager(metadata(), new AtomicTimestampOracle(100), slowControl, adapters, 1, 0, 1, nanos::get);
        }

        /**
         * Maintenance of real metadata behaviour, only recording clean-up and providing fixed future wall clocks.
         *
         * @return the operation result
         */
        private TransactionMetadataStore metadata() {
            return (TransactionMetadataStore) Proxy.newProxyInstance(TransactionMetadataStore.class.getClassLoader(),
                    new Class<?>[]{TransactionMetadataStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("currentTimeMillis")) {
                            return Long.MAX_VALUE;
                        }
                        if (method.getName().equals("purgeFinalized")) {
                            work.add("metadata");
                            nanos.addAndGet(2_000_000);
                        }
                        try {
                            return method.invoke(records, args);
                        } catch (final InvocationTargetException ex) {
                            throw ex.getCause();
                        }
                    });
        }

        /**
         * Simulates an shard work unit that is bound to exhaust the remaining budget in this round.
         *
         * @param shardId logical shard identifier
         * @param fail fail
         * @return the operation result
         */
        private StorageAdapter adapter(final String shardId, final boolean fail) {
            return (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(), new Class<?>[]{StorageAdapter.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("shardId")) {
                            return shardId;
                        }
                        if (method.getName().equals("gc")) {
                            work.add(shardId);
                            nanos.addAndGet(2_000_000);
                            if (fail) {
                                throw new IllegalStateException("shard unavailable");
                            }
                            return 1;
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
