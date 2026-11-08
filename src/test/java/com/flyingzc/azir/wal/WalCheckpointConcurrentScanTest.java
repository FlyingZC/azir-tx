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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Three-fold reading limit staggered pages, collection and check points. Independent models cross-click to check the scan results instead of comparing only two pieces equal.
 */
class WalCheckpointConcurrentScanTest {

    @TempDir
    Path directory;

    /**
     * Each statement maintains the same snapshot completes all pages, and checkpoints and GCs should not lead to slippages, rows or errors version.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void pagedSnapshotsRemainExactDuringRepeatedCheckpoints() throws Exception {
        String saved = System.getProperty("azir.wal.rangeEvidence");
        Path evidence = saved == null ? directory.resolve("history.tsv.gz") : Path.of(saved);
        if (saved != null) {
            Files.createDirectories(evidence.getParent());
        }
        var running = new AtomicBoolean(true);
        var keys = new ArrayList<String>();
        for (int worker = 0; worker < 3; worker++) {
            for (int k = 0; k < 8; k++) {
                keys.add("w" + worker + ":k" + k);
            }
        }
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        var policy = new WalRetentionOptions(200, 3_600_000, 4_000_000, 1_000_000, 1000);
        try (
                var model = new WalSteadyModel(evidence);
                var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults().withHotCacheBytes(0), policy);
                var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", keys.getFirst(), bytes("seed"));
            runtime.put(seed, "b", keys.getFirst(), bytes("seed"));
            model.committed(runtime.commit(seed), seed.startSequence(), keys.getFirst(), "seed");
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int worker = 0; worker < 3; worker++) {
                    int id = worker;
                    futures.add(tasks.submit(() -> {
                        int ordinal = 0;
                        try {
                            while (running.get()) {
                                java.util.concurrent.locks.LockSupport.parkNanos(30_000_000);
                                String key = "w" + id + ":k" + ordinal % 8;
                                String value = ordinal % 7 == 0 ? null : id + ":" + ordinal;
                                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                                for (String shard : new String[]{"a", "b"}) {
                                    if (value == null) {
                                        runtime.delete(tx, shard, key);
                                    } else {
                                        runtime.put(tx, shard, key, bytes(value));
                                    }
                                }
                                model.committed(runtime.commit(tx), tx.startSequence(), key, value);
                                ordinal++;
                            }
                        } catch (final Exception failure) {
                            throw new IllegalStateException(failure);
                        }
                    }));
                }
                for (int worker = 0; worker < 3; worker++) {
                    int id = worker;
                    futures.add(tasks.submit(() -> {
                        long ordinal = 0;
                        try {
                            while (running.get()) {
                                String token = "scan:" + id + ":" + ordinal;
                                long lower = model.pin(token);
                                var level = ordinal++ % 2 == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED;
                                WalTransaction tx = null;
                                try {
                                    tx = runtime.begin(level);
                                    assertTrue(tx.startSequence() >= lower);
                                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                    while (running.get() && System.nanoTime() < end) {
                                        java.util.concurrent.locks.LockSupport.parkNanos(100_000_000);
                                        long minimum = model.known();
                                        var snapshot = runtime.beginReadStatement(tx);
                                        var left = pages(runtime, tx, "a", snapshot);
                                        var right = pages(runtime, tx, "b", snapshot);
                                        assertTrue(keys.containsAll(left.keySet()));
                                        assertTrue(keys.containsAll(right.keySet()));
                                        for (String key : keys) {
                                            model.observed(token, snapshot.sequence(), key, left.get(key), right.get(key), level, tx.startSequence(), minimum, true);
                                        }
                                    }
                                } finally {
                                    if (tx != null) {
                                        runtime.rollback(tx);
                                    }
                                    model.unpin(token);
                                }
                            }
                        } catch (final Exception failure) {
                            throw new IllegalStateException(failure);
                        }
                    }));
                }
                for (int cycle = 0; cycle < 100; cycle++) {
                    Thread.sleep(50);
                    runtime.collectGarbage(64);
                    assertTrue(runtime.checkpoint().created());
                }
            } finally {
                running.set(false);
            }
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(model.known(), runtime.stats().publishedSequence());
            assertTrue(model.observations() >= 240);
        }
    }

    /**
     * The same sentence uses the same token for all pages, with visible detection of duplicate keys and unterminated cursor.
     *
     * @param runtime runtime
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param snapshot snapshot
     * @return the operation result
     */
    private static Map<String, String> pages(final WalMvccRuntime runtime, final WalTransaction tx, final String shard, final WalReadSnapshot snapshot) {
        var result = new LinkedHashMap<String, String>();
        String cursor = null;
        int count = 0;
        do {
            var page = runtime.scanPage(tx, shard, null, null, cursor, 3, snapshot);
            page.rows().forEach((key, value) -> assertNull(result.put(key, new String(value, StandardCharsets.UTF_8))));
            cursor = page.nextAfterKey();
            assertTrue(++count < 64);
        } while (cursor != null);
        var ordered = result.keySet().stream().sorted(com.flyingzc.azir.storage.Utf8KeyOrder::compare).toList();
        Assertions.assertEquals(ordered, new ArrayList<>(result.keySet()));
        return result;
    }
}
