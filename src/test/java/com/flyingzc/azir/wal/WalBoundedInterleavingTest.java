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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * All bounded API staggered by two writers and one double reader, materialization interpolated with GC every step.
 */
class WalBoundedInterleavingTest {

    @TempDir
    Path directory;

    /**
     * The two authors, two steps each and three steps reader, involved 210 staggered cases with a total of 1680 cases of cold thermal caches and isolation/conflict combinations.
     *
     * @return the operation result
     */
    static Stream<Schedule> schedules() {
        var orders = new ArrayList<String>();
        enumerate(new int[]{2, 2, 3}, "", orders);
        return Stream.of(IsolationLevel.READ_COMMITTED, IsolationLevel.REPEATABLE_READ)
                .flatMap(level -> Stream.of(true, false).flatMap(shared -> Stream.of(0L, 8192L)
                        .flatMap(cache -> orders.stream().map(order -> new Schedule(level, shared, order, cache)))));
    }

    /**
     * The recalcitrant list lists all the staggers that remain unaltered within each role.
     *
     * @param remaining remaining
     * @param prefix prefix
     * @param result result
     */
    private static void enumerate(final int[] remaining, final String prefix, final List<String> result) {
        if (remaining[0] + remaining[1] + remaining[2] == 0) {
            result.add(prefix);
            return;
        }
        for (int actor = 0; actor < remaining.length; actor++) {
            if (remaining[actor] > 0) {
                remaining[actor]--;
                enumerate(remaining, prefix + actor, result);
                remaining[actor]++;
            }
        }
    }

    /**
     * Independent models decide to conflict with the same key, checking old RRs, refresh RCs, tombstone and background collection.
     *
     * @param schedule schedule
     */
    @ParameterizedTest
    @MethodSource("schedules")
    void allBoundedApiSchedulesPreserveSnapshots(final Schedule schedule) {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        var chains = new TreeMap<String, TreeMap<Long, String>>();
        for (String key : List.of("k1", "k2")) {
            var chain = new TreeMap<Long, String>();
            chain.put(1L, "seed");
            chains.put(key, chain);
        }
        var handles = new WalTransaction[3];
        var positions = new int[3];
        var active = new boolean[3];
        long published = 1;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults().withHotCacheBytes(schedule.cacheBytes))) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) {
                for (String key : chains.keySet()) {
                    runtime.put(seed, shard, key, bytes("seed"));
                }
            }
            Assertions.assertEquals(1, runtime.commit(seed).commitSequence());
            for (char digit : schedule.actors.toCharArray()) {
                int actor = digit - '0';
                int step = positions[actor]++;
                if (step == 0) {
                    var isolation = actor == 2 ? schedule.isolation : IsolationLevel.REPEATABLE_READ;
                    handles[actor] = runtime.begin(isolation);
                    active[actor] = true;
                    Assertions.assertEquals(published, handles[actor].startSequence());
                    if (actor < 2) {
                        String key = actor == 0 || schedule.sharedKey ? "k1" : "k2";
                        for (String shard : List.of("a", "b")) {
                            if (actor == 0) {
                                runtime.delete(handles[actor], shard, key);
                            } else {
                                runtime.put(handles[actor], shard, key, bytes("writer1"));
                            }
                        }
                    }
                } else if (actor < 2) {
                    String key = actor == 0 || schedule.sharedKey ? "k1" : "k2";
                    var tx = handles[actor];
                    if (chains.get(key).lastKey() > tx.startSequence()) {
                        Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(tx)).errorCode());
                        assertTrue(runtime.findCommitted(tx.id()).isEmpty());
                    } else {
                        var receipt = runtime.commit(tx);
                        Assertions.assertEquals(++published, receipt.commitSequence());
                        chains.get(key).put(published, actor == 0 ? null : "writer1");
                        Assertions.assertEquals(receipt, runtime.commit(tx), "duplicate commit must keep one decision");
                    }
                    active[actor] = false;
                } else {
                    var tx = handles[2];
                    var snapshot = runtime.beginReadStatement(tx);
                    long expectedSnapshot = schedule.isolation == IsolationLevel.REPEATABLE_READ ? tx.startSequence() : published;
                    Assertions.assertEquals(expectedSnapshot, snapshot.sequence());
                    var expected = new TreeMap<String, String>();
                    chains.forEach((key, chain) -> {
                        String value = chain.floorEntry(expectedSnapshot).getValue();
                        if (value != null) {
                            expected.put(key, value);
                        }
                    });
                    for (String shard : List.of("a", "b")) {
                        for (String key : chains.keySet()) {
                            Assertions.assertEquals(chains.get(key).floorEntry(expectedSnapshot).getValue(),
                                    runtime.get(tx, shard, key, snapshot).map(WalMvccRuntimeTest::text).orElse(null));
                        }
                        var scanned = new TreeMap<String, String>();
                        runtime.scan(tx, shard, null, null, snapshot).forEach((key, value) -> scanned.put(key, text(value)));
                        Assertions.assertEquals(expected, scanned);
                    }
                    if (step == 2) {
                        runtime.rollback(tx);
                        active[2] = false;
                    }
                }
                runtime.flush();
                runtime.collectGarbage(4096);
                long oldest = published;
                for (int i = 0; i < 3; i++) {
                    if (active[i]) {
                        oldest = Math.min(oldest, handles[i].startSequence());
                    }
                }
                Assertions.assertEquals(oldest, runtime.stats().safePoint(), "safe point must match live snapshot protection");
                Assertions.assertEquals(published, runtime.stats().publishedSequence());
            }
            Assertions.assertEquals(published, runtime.stats().safePoint());
            for (String key : chains.keySet()) {
                var last = chains.get(key).lastEntry();
                for (var store : List.of(a, b)) {
                    var found = store.get(key, Long.MAX_VALUE);
                    if (last.getValue() == null) {
                        assertTrue(found.isEmpty(), "safe tombstone and older versions must be retired");
                        continue;
                    }
                    var physical = found.orElseThrow();
                    Assertions.assertEquals(last.getKey(), physical.commitSequence());
                    Assertions.assertEquals(last.getValue(), physical.deleted() ? null : text(physical.value()));
                }
            }
        }
    }

    /**
     * Route, isolation and movement in a single bounded status space.
     *
     * @param isolation global transaction isolation level
     * @param sharedKey shared key
     * @param actors actors
     * @param cacheBytes cache bytes
     */
    private record Schedule(IsolationLevel isolation, boolean sharedKey, String actors, long cacheBytes) {

        /**
         * Output is staggered accurately in the report, and no guessing of a linear schedule is required when failure occurs.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return isolation + "-" + (sharedKey ? "shared" : "disjoint") + "-" + actors + "-cache=" + cacheBytes;
        }
    }
}
