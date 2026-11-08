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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Check concurrent history with an independent reference version model, including conflicts, tombstone, old snapshot and materialization collection.
 */
class WalHistoryAcceptanceTest {

    @TempDir
    Path directory;

    /**
     * Fixed seed maintains the operational options that can be replicated, with actual staggering and observations recorded separately.
     *
     * @param seed seed
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @ValueSource(longs = {7, 20261010, 99123})
    void concurrentHistoriesMatchIndependentSnapshotModel(final long seed) throws Exception {
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        var commits = new ConcurrentLinkedQueue<Commit>();
        var reads = new ConcurrentLinkedQueue<Read>();
        var scans = new ConcurrentLinkedQueue<Scan>();
        var conflicts = new AtomicInteger();
        var writersRemaining = new AtomicInteger(3);
        var start = new CountDownLatch(1);
        var background = new AtomicBoolean(true);
        try (
                var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults());
                var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seedTx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (int k = 0; k < 8; k++) {
                runtime.put(seedTx, "a", "k" + k, bytes("seed"));
                runtime.put(seedTx, "b", "k" + k, bytes("seed"));
            }
            long initial = runtime.commit(seedTx).commitSequence();
            for (int k = 0; k < 8; k++) {
                commits.add(new Commit(initial, 0, "k" + k, "seed"));
            }
            var held = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int worker = 0; worker < 3; worker++) {
                int id = worker;
                futures.add(tasks.submit(() -> {
                    var random = new Random(seed + id * 31);
                    await(start);
                    try {
                        for (int ordinal = 0; ordinal < 100; ordinal++) {
                            String key = "k" + random.nextInt(8);
                            String value = random.nextInt(5) == 0 ? null : id + ":" + ordinal;
                            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                            if (value == null) {
                                runtime.delete(tx, "a", key);
                                runtime.delete(tx, "b", key);
                            } else {
                                runtime.put(tx, "a", key, bytes(value));
                                runtime.put(tx, "b", key, bytes(value));
                            }
                            Assertions.assertEquals(value, runtime.get(tx, "a", key).map(WalMvccRuntimeTest::text).orElse(null));
                            Assertions.assertEquals(value, runtime.get(tx, "b", key).map(WalMvccRuntimeTest::text).orElse(null));
                            try {
                                commits.add(new Commit(runtime.commit(tx).commitSequence(), tx.startSequence(), key, value));
                            } catch (final WalMvccException failure) {
                                Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, failure.errorCode());
                                conflicts.incrementAndGet();
                            }
                        }
                    } finally {
                        writersRemaining.decrementAndGet();
                    }
                }));
            }
            for (int worker = 0; worker < 3; worker++) {
                int id = worker;
                futures.add(tasks.submit(() -> {
                    var random = new Random(seed ^ (id + 19));
                    await(start);
                    int iterations = 0;
                    do {
                        var tx = runtime.begin(id == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED);
                        var snapshot = runtime.beginReadStatement(tx);
                        String key = "k" + random.nextInt(8);
                        String left = runtime.get(tx, "a", key, snapshot).map(WalMvccRuntimeTest::text).orElse(null);
                        Thread.yield();
                        String right = runtime.get(tx, "b", key, snapshot).map(WalMvccRuntimeTest::text).orElse(null);
                        reads.add(new Read(snapshot.sequence(), key, left, right));
                        Assertions.assertEquals(left, right, "fractured cross-shard read");
                        if (iterations % 10 == 0) {
                            var paged = new TreeMap<String, String>();
                            String cursor = null;
                            do {
                                var page = runtime.scanPage(tx, "a", null, null, cursor, 3, snapshot);
                                page.rows().forEach((k, v) -> assertNull(paged.put(k, text(v)), "duplicate paged key"));
                                cursor = page.nextAfterKey();
                            } while (cursor != null);
                            var full = new TreeMap<String, String>();
                            runtime.scan(tx, "b", null, null, snapshot).forEach((k, v) -> full.put(k, text(v)));
                            scans.add(new Scan(snapshot.sequence(), paged, full));
                        }
                        runtime.rollback(tx);
                        if (++iterations % 5 == 0) {
                            java.util.concurrent.locks.LockSupport.parkNanos(100_000);
                        }
                    } while (writersRemaining.get() > 0 || iterations < 200);
                }));
            }
            var maintenance = tasks.submit(() -> {
                await(start);
                int round = 0;
                while (background.get()) {
                    b.failApply = ++round % 7 == 0;
                    if (round % 11 == 0) {
                        a.loseApplyResponse = true;
                    }
                    try {
                        runtime.flush();
                    } catch (final WalMvccException expectedTransientFailure) {
                    }
                    runtime.collectGarbage(32);
                    java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                }
            });
            start.countDown();
            try {
                for (var future : futures) {
                    future.get(30, TimeUnit.SECONDS);
                }
            } finally {
                background.set(false);
            }
            maintenance.get(30, TimeUnit.SECONDS);
            // Turning off all failure tags after the injection has been withdrawn, a closing check can no longer trigger the loss of the previous unconsumpted simulator response.
            b.failApply = false;
            a.loseApplyResponse = false;
            runtime.flush();
            for (int k = 0; k < 8; k++) {
                Assertions.assertEquals("seed", text(runtime.get(held, "a", "k" + k).orElseThrow()));
                Assertions.assertEquals("seed", text(runtime.get(held, "b", "k" + k).orElseThrow()));
            }
            runtime.rollback(held);
            runtime.collectGarbage(32);
            var reference = new TreeMap<String, TreeMap<Long, String>>();
            commits.stream().sorted(Comparator.comparingLong(Commit::sequence)).forEach(commit -> {
                var chain = reference.computeIfAbsent(commit.key, ignored -> new TreeMap<>());
                if (!chain.isEmpty()) {
                    assertTrue(chain.lastKey() <= commit.startSequence, "stale same-key write was accepted");
                }
                chain.put(commit.sequence, commit.value);
            });
            for (Read read : reads) {
                var expected = reference.get(read.key).floorEntry(read.sequence);
                assertNotNull(expected);
                Assertions.assertEquals(expected.getValue(), read.left, "wrong version at snapshot " + read.sequence);
                Assertions.assertEquals(expected.getValue(), read.right, "wrong right-shard version");
            }
            for (Scan scan : scans) {
                var expected = new TreeMap<String, String>();
                reference.forEach((key, chain) -> {
                    var version = chain.floorEntry(scan.sequence);
                    if (version != null && version.getValue() != null) {
                        expected.put(key, version.getValue());
                    }
                });
                Assertions.assertEquals(expected, scan.left, "wrong paged snapshot");
                Assertions.assertEquals(expected, scan.right, "wrong scanned snapshot");
            }
            long accepted = commits.stream().map(Commit::sequence).distinct().count();
            Assertions.assertEquals(301, accepted + conflicts.get());
            Assertions.assertEquals(accepted, runtime.stats().publishedSequence());
            assertTrue(reads.size() >= 600);
            assertTrue(scans.size() >= 60);
            assertTrue(reads.stream().anyMatch(read -> read.sequence > initial));
            var finalReader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String key : reference.keySet()) {
                String expected = reference.get(key).lastEntry().getValue();
                Assertions.assertEquals(expected, runtime.get(finalReader, "a", key).map(WalMvccRuntimeTest::text).orElse(null));
                Assertions.assertEquals(expected, runtime.get(finalReader, "b", key).map(WalMvccRuntimeTest::text).orElse(null));
            }
            runtime.rollback(finalReader);
        } finally {
            writeEvidence(seed, commits, reads, scans);
        }
    }

    /**
     * Retain the history of each case as needed to facilitate a staggered review of failures rather than merely preserve the final operational value.
     *
     * @param seed seed
     * @param commits commits
     * @param reads reads
     * @param scans scans
     * @throws java.io.IOException if the operation cannot complete
     */
    private static void writeEvidence(final long seed, final Iterable<Commit> commits, final Iterable<Read> reads, final Iterable<Scan> scans) throws java.io.IOException {
        String target = System.getProperty("azir.wal.historyEvidence");
        if (target == null) {
            return;
        }
        Path root = Path.of(target);
        java.nio.file.Files.createDirectories(root);
        var commitLines = new StringBuilder("sequence\tstart_sequence\tkey\tvalue\n");
        for (Commit commit : commits) {
            commitLines.append(commit.sequence).append('\t').append(commit.startSequence).append('\t')
                    .append(commit.key).append('\t').append(commit.value == null ? "<NULL>" : commit.value).append('\n');
        }
        var readLines = new StringBuilder("sequence\tkey\tleft\tright\n");
        for (Read read : reads) {
            readLines.append(read.sequence).append('\t').append(read.key).append('\t')
                    .append(read.left == null ? "<NULL>" : read.left).append('\t')
                    .append(read.right == null ? "<NULL>" : read.right).append('\n');
        }
        var scanLines = new StringBuilder("sequence\tleft\tright\n");
        for (Scan scan : scans) {
            scanLines.append(scan.sequence).append('\t').append(scan.left).append('\t').append(scan.right).append('\n');
        }
        java.nio.file.Files.writeString(root.resolve(seed + "-commits.tsv"), commitLines);
        java.nio.file.Files.writeString(root.resolve(seed + "-reads.tsv"), readLines);
        java.nio.file.Files.writeString(root.resolve(seed + "-scans.tsv"), scanLines);
    }

    /**
     * Full write set logic for a successful commit.
     *
     * @param sequence sequence
     * @param startSequence start sequence
     * @param key business key
     * @param value value to write
     */
    private record Commit(long sequence, long startSequence, String key, String value) {
    }

    /**
     * A statement is actually observed on two shard s.
     *
     * @param sequence sequence
     * @param key business key
     * @param left left
     * @param right right
     */
    private record Read(long sequence, String key, String left, String right) {
    }

    /**
     * The result of the cross-shard break and the entire section of the same sentence.
     *
     * @param sequence sequence
     * @param left left
     * @param right right
     */
    private record Scan(long sequence, Map<String, String> left, Map<String, String> right) {
    }
}
