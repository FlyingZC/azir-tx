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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.storage.Utf8KeyOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import java.util.zip.GZIPOutputStream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Expands the history of a single film mixed with an cross-shard, independently auditing each version, transaction terminal state and statement snapshot.
 */
class WalExtendedHistoryTest {

    @TempDir
    Path directory;

    private static final List<String> KEYS = List.of("", "k00", "k01", "k02", "k03", "k04", "k05", "k06",
            "k07", "k08", "k09", "k10", "k~", "k中", "k\uE000", "k😀");

    /**
     * Ordinary returns are maintained on a small scale, with formal acceptances extending the number of seeds significantly through parameters.
     *
     * @return the operation result
     */
    static LongStream seeds() {
        return LongStream.range(0, Integer.getInteger("azir.wal.extendedSeeds", 2)).map(i -> 2026101000L + i);
    }

    /**
     * Mixed multiple keys commit, active rollback, read-your-own-writes, propulsive safe point and instantaneous materialization failure.
     *
     * @param seed seed
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("seeds")
    void historiesRemainValidUnderMixedTransactions(final long seed) throws Exception {
        int iterations = Integer.getInteger("azir.wal.extendedWrites", 100);
        int readerIterations = Integer.getInteger("azir.wal.extendedReads", 80);
        var a = new TestMaterializedStore();
        var b = new TestMaterializedStore();
        var accepted = new ConcurrentLinkedQueue<Accepted>();
        var observations = new ConcurrentLinkedQueue<Observation>();
        var outcomes = new ConcurrentLinkedQueue<Outcome>();
        var start = new CountDownLatch(1);
        var maintaining = new AtomicBoolean(true);
        var writers = new AtomicInteger(3);
        var advancingGc = new AtomicInteger();
        var options = WalOptions.defaults().withTimeout(300_000);
        try (
                var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), options);
                var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var initial = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var initialWrites = new LinkedHashMap<Key, String>();
            for (String shard : List.of("a", "b")) {
                for (String key : KEYS) {
                    initialWrites.put(new Key(shard, key), "seed");
                    runtime.put(initial, shard, key, bytes("seed"));
                }
            }
            accepted.add(new Accepted(initial.id(), initial.startSequence(), runtime.commit(initial).commitSequence(), initialWrites));
            outcomes.add(new Outcome(initial.id(), "COMMITTED", 1));
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int worker = 0; worker < 3; worker++) {
                int id = worker;
                futures.add(tasks.submit(() -> {
                    var random = new Random(seed + id * 79);
                    await(start);
                    try {
                        for (int n = 0; n < iterations; n++) {
                            var isolation = n % 2 == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED;
                            var tx = runtime.begin(isolation);
                            var writes = new LinkedHashMap<Key, String>();
                            int keyCount = 1 + random.nextInt(3);
                            int route = random.nextInt(3);
                            for (int k = 0; k < keyCount; k++) {
                                String key = KEYS.get(random.nextInt(seed % 4 == 0 ? 4 : KEYS.size()));
                                String value = random.nextInt(5) == 0 ? null : id + ":" + n + ":" + k;
                                for (String shard : route == 0 ? List.of("a") : route == 1 ? List.of("b") : List.of("a", "b")) {
                                    writes.put(new Key(shard, key), value);
                                    if (value == null) {
                                        runtime.delete(tx, shard, key);
                                    } else {
                                        runtime.put(tx, shard, key, bytes(value));
                                    }
                                }
                            }
                            for (var mutation : writes.entrySet()) {
                                Assertions.assertEquals(mutation.getValue(), runtime.get(tx, mutation.getKey().shard, mutation.getKey().key)
                                        .map(WalMvccRuntimeTest::text).orElse(null), "read own write");
                            }
                            if (n % 31 == 0) {
                                observe(runtime, tx, isolation, n, writes, observations, true);
                            }
                            if (random.nextInt(20) == 0) {
                                runtime.rollback(tx);
                                runtime.rollback(tx);
                                assertTrue(runtime.findCommitted(tx.id()).isEmpty());
                                outcomes.add(new Outcome(tx.id(), "ABORTED", 0));
                            } else {
                                try {
                                    long sequence = runtime.commit(tx).commitSequence();
                                    accepted.add(new Accepted(tx.id(), tx.startSequence(), sequence, writes));
                                    outcomes.add(new Outcome(tx.id(), "COMMITTED", sequence));
                                    Assertions.assertEquals(sequence, runtime.findCommitted(tx.id()).orElseThrow().commitSequence());
                                } catch (final WalMvccException failure) {
                                    Assertions.assertEquals(ErrorCode.WRITE_CONFLICT, failure.errorCode(), "unexpected write failure");
                                    outcomes.add(new Outcome(tx.id(), "CONFLICT", 0));
                                    assertTrue(runtime.findCommitted(tx.id()).isEmpty());
                                }
                            }
                        }
                    } finally {
                        writers.decrementAndGet();
                    }
                }));
            }
            for (int worker = 0; worker < 3; worker++) {
                int id = worker;
                futures.add(tasks.submit(() -> {
                    await(start);
                    for (int n = 0; n < readerIterations; n++) {
                        var isolation = (n + id) % 2 == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED;
                        var tx = runtime.begin(isolation);
                        int statements = n % 17 == 0 ? 12 : 2;
                        for (int statement = 0; statement < statements; statement++) {
                            observe(runtime, tx, isolation, statement, Map.of(), observations, n % 11 == 0);
                            if (statement % 4 == 0 && writers.get() > 0) {
                                java.util.concurrent.locks.LockSupport.parkNanos(200_000);
                            }
                        }
                        runtime.commit(tx);
                        outcomes.add(new Outcome(tx.id(), "READ_ONLY", 0));
                    }
                }));
            }
            var maintenance = tasks.submit(() -> {
                await(start);
                int round = 0;
                while (maintaining.get()) {
                    b.failApply = ++round % 13 == 0;
                    if (round % 17 == 0) {
                        a.loseApplyResponse = true;
                    }
                    try {
                        runtime.flush();
                    } catch (final WalMvccException failure) {
                        Assertions.assertEquals(ErrorCode.STORAGE_UNAVAILABLE, failure.errorCode());
                    }
                    runtime.collectGarbage(4096);
                    if (runtime.stats().safePoint() > 1) {
                        advancingGc.incrementAndGet();
                    }
                    java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                }
            });
            start.countDown();
            try {
                for (var future : futures) {
                    future.get(5, TimeUnit.MINUTES);
                }
            } finally {
                maintaining.set(false);
            }
            maintenance.get(30, TimeUnit.SECONDS);
            b.failApply = false;
            a.loseApplyResponse = false;
            while (runtime.stats().materializedSequences().values().stream().anyMatch(s -> s < runtime.stats().publishedSequence())) {
                runtime.flush();
            }
            runtime.collectGarbage(4096);
            assertTrue(advancingGc.get() > 0, "GC must advance during concurrent work");
            verify(accepted, observations, outcomes);
            Assertions.assertEquals(1L + 3L * iterations + 3L * readerIterations, outcomes.size());
            Assertions.assertEquals(accepted.size(), runtime.stats().publishedSequence());
            Assertions.assertEquals(runtime.stats().publishedSequence(), runtime.stats().safePoint());
            // Final direct check for backend version to avoid covering up materialization problems by reading only from memory caches.
            for (String shard : List.of("a", "b")) {
                var store = shard.equals("a") ? a : b;
                for (String key : KEYS) {
                    var last = accepted.stream().filter(c -> c.writes.containsKey(new Key(shard, key)))
                            .max(Comparator.comparingLong(Accepted::sequence)).orElseThrow();
                    var found = store.get(key, Long.MAX_VALUE);
                    if (last.writes.get(new Key(shard, key)) == null) {
                        assertTrue(found.isEmpty(), "safe deleted key must not retain physical history");
                        continue;
                    }
                    var physical = found.orElseThrow();
                    Assertions.assertEquals(last.sequence, physical.commitSequence());
                    Assertions.assertEquals(last.writes.get(new Key(shard, key)), physical.deleted() ? null : text(physical.value()));
                }
            }
        } finally {
            evidence(seed, accepted, observations, outcomes);
        }
    }

    /**
     * The same sentence cross-shard is read, the scan uses a semi-open space and completes all pages under the same snapshot.
     *
     * @param runtime runtime
     * @param tx transaction context
     * @param isolation global transaction isolation level
     * @param ordinal ordinal
     * @param writes writes
     * @param observations observations
     * @param scan scan
     */
    private static void observe(final WalMvccRuntime runtime, final WalTransaction tx, final IsolationLevel isolation, final int ordinal,
                                final Map<Key, String> writes, final ConcurrentLinkedQueue<Observation> observations, final boolean scan) {
        var snapshot = runtime.beginReadStatement(tx);
        String key = KEYS.get(Math.floorMod(ordinal, KEYS.size()));
        String from = ordinal % 2 == 0 ? null : "k02";
        String to = ordinal % 3 == 0 ? "k中" : null;
        for (String shard : List.of("a", "b")) {
            var own = new LinkedHashMap<String, String>();
            writes.forEach((identity, value) -> {
                if (identity.shard.equals(shard)) {
                    own.put(identity.key, value);
                }
            });
            var rows = new LinkedHashMap<String, String>();
            rows.put(key, runtime.get(tx, shard, key, snapshot).map(WalMvccRuntimeTest::text).orElse(null));
            observations.add(new Observation(tx.id(), isolation, tx.startSequence(), ordinal, snapshot.sequence(), shard,
                    "POINT", key, null, own, rows));
            if (scan) {
                var paged = new LinkedHashMap<String, String>();
                String cursor = null;
                int pages = 0;
                do {
                    var page = runtime.scanPage(tx, shard, from, to, cursor, 3, snapshot);
                    page.rows().forEach((k, v) -> assertNull(paged.put(k, text(v)), "duplicate scan key"));
                    cursor = page.nextAfterKey();
                    assertTrue(++pages < 20, "pagination must terminate");
                } while (cursor != null);
                var full = new LinkedHashMap<String, String>();
                runtime.scan(tx, shard, from, to, snapshot).forEach((k, v) -> full.put(k, text(v)));
                Assertions.assertEquals(full, paged);
                Assertions.assertEquals(new ArrayList<>(full.keySet()), new ArrayList<>(paged.keySet()), "page order");
                observations.add(new Observation(tx.id(), isolation, tx.startSequence(), ordinal, snapshot.sequence(), shard,
                        "SCAN", from, to, own, paged));
            }
            Thread.yield();
        }
    }

    /**
     * Builds an independent history of version chain, checking conflict boundaries, reading and scanning sequences.
     *
     * @param accepted accepted
     * @param observations observations
     * @param outcomes outcomes
     */
    private static void verify(final Iterable<Accepted> accepted, final Iterable<Observation> observations, final Iterable<Outcome> outcomes) {
        var sorted = new ArrayList<Accepted>();
        accepted.forEach(sorted::add);
        sorted.sort(Comparator.comparingLong(Accepted::sequence));
        var chains = new java.util.HashMap<Key, TreeMap<Long, String>>();
        var decisions = new java.util.HashMap<UUID, Long>();
        long expectedSequence = 0;
        for (Accepted commit : sorted) {
            Assertions.assertEquals(++expectedSequence, commit.sequence);
            assertTrue(commit.start < commit.sequence);
            assertNull(decisions.put(commit.id, commit.sequence), "duplicate durable transaction");
            commit.writes.forEach((key, value) -> {
                var chain = chains.computeIfAbsent(key, ignored -> new TreeMap<>());
                if (!chain.isEmpty()) {
                    assertTrue(chain.lastKey() <= commit.start, "stale write accepted");
                }
                chain.put(commit.sequence, value);
            });
        }
        var seen = new java.util.HashSet<UUID>();
        for (Outcome outcome : outcomes) {
            assertTrue(seen.add(outcome.id), "duplicate terminal outcome");
            if (outcome.state.equals("COMMITTED")) {
                Assertions.assertEquals(outcome.sequence, decisions.get(outcome.id));
            } else {
                assertFalse(decisions.containsKey(outcome.id), "aborted or read-only transaction persisted");
            }
        }
        for (Observation read : observations) {
            assertTrue(read.start <= read.snapshot && read.snapshot <= expectedSequence);
            if (read.isolation == IsolationLevel.REPEATABLE_READ) {
                Assertions.assertEquals(read.start, read.snapshot);
            }
            var visible = new TreeMap<String, String>(Utf8KeyOrder::compare);
            chains.forEach((key, chain) -> {
                if (key.shard.equals(read.shard)) {
                    var version = chain.floorEntry(read.snapshot);
                    if (version != null) {
                        visible.put(key.key, version.getValue());
                    }
                }
            });
            visible.putAll(read.own);
            var expected = new LinkedHashMap<String, String>();
            if (read.kind.equals("POINT")) {
                expected.put(read.from, visible.get(read.from));
            } else {
                visible.forEach((key, value) -> {
                    if (value != null && (read.from == null || Utf8KeyOrder.compare(key, read.from) >= 0)
                            && (read.to == null || Utf8KeyOrder.compare(key, read.to) < 0)) {
                        expected.put(key, value);
                    }
                });
            }
            Assertions.assertEquals(expected, read.rows, "invalid snapshot " + read);
            Assertions.assertEquals(new ArrayList<>(expected.keySet()), new ArrayList<>(read.rows.keySet()), "invalid ordering");
        }
    }

    /**
     * Preserve compressed original evidence for independent review by Python and try to preserve history when testing fails.
     *
     * @param seed seed
     * @param accepted accepted
     * @param observations observations
     * @param outcomes outcomes
     * @throws IOException if the operation cannot complete
     */
    private static void evidence(final long seed, final Iterable<Accepted> accepted, final Iterable<Observation> observations,
                                 final Iterable<Outcome> outcomes) throws IOException {
        String destination = System.getProperty("azir.wal.extendedEvidence");
        if (destination == null) {
            return;
        }
        Path root = Path.of(destination);
        Files.createDirectories(root);
        try (var out = gzip(root.resolve(seed + "-commits.tsv.gz"))) {
            out.write("sequence\tstart\tid\tshard\tkey\tvalue\n");
            for (Accepted c : accepted) {
                for (var m : c.writes.entrySet()) {
                    out.write(c.sequence + "\t" + c.start + "\t" + c.id + "\t" + m.getKey().shard + "\t" + m.getKey().key + "\t" + scalar(m.getValue()) + "\n");
                }
            }
        }
        try (var out = gzip(root.resolve(seed + "-observations.tsv.gz"))) {
            out.write("id\tisolation\tstart\tstatement\tsnapshot\tshard\tkind\tfrom\tto\town\trows\n");
            for (Observation r : observations) {
                out.write(r.id + "\t" + r.isolation + "\t" + r.start + "\t" + r.statement + "\t" + r.snapshot + "\t" + r.shard + "\t" + r.kind
                        + "\t" + scalar(r.from) + "\t" + scalar(r.to) + "\t" + encode(r.own) + "\t" + encode(r.rows) + "\n");
            }
        }
        try (var out = gzip(root.resolve(seed + "-outcomes.tsv.gz"))) {
            out.write("id\tstate\tsequence\n");
            for (Outcome o : outcomes) {
                out.write(o.id + "\t" + o.state + "\t" + o.sequence + "\n");
            }
        }
    }

    /**
     * Writes a TSV with UTF-8 and gzip that only contains the characters of this test.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static BufferedWriter gzip(final Path path) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path)), StandardCharsets.UTF_8));
    }

    /**
     * Special empty tags and synthetic business values are not renamed.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String scalar(final String value) {
        return value == null ? "<NULL>" : value;
    }

    /**
     * Encoding an orderly map, composite key and value does not contain semicolons, equal marks or tabulators.
     *
     * @param values values
     * @return the operation result
     */
    private static String encode(final Map<String, String> values) {
        var result = new StringBuilder();
        values.forEach((key, value) -> result.append(key).append('=').append(scalar(value)).append(';'));
        return result.toString();
    }

    /**
     * Shard and user keys for reference model identity.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    private record Key(String shard, String key) {
    }

    /**
     * A full write set of a real commit. The reference model does not read inside the engine version.
     *
     * @param id id
     * @param start start
     * @param sequence sequence
     * @param writes writes
     */
    private record Accepted(UUID id, long start, long sequence, Map<Key, String> writes) {
    }

    /**
     * An actual reading and local overwhelm, which contains scanned borders and statements identity.
     *
     * @param id id
     * @param isolation global transaction isolation level
     * @param start start
     * @param statement statement
     * @param snapshot snapshot
     * @param shard logical shard identifier
     * @param kind kind
     * @param from from
     * @param to to
     * @param own own
     * @param rows rows
     */
    private record Observation(UUID id, IsolationLevel isolation, long start, int statement, long snapshot,
            String shard, String kind, String from, String to, Map<String, String> own,
            Map<String, String> rows) {
    }

    /**
     * Each transaction is ultimately observed in the state of foreground and used to check that no failure or repetition of decision occurred.
     *
     * @param id id
     * @param state state
     * @param sequence sequence
     */
    private record Outcome(UUID id, String state, long sequence) {
    }
}
