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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Multiple real killings and re-opening of the same data directory to check for successful prefixes and unrecognized decision.
 */
class WalCrashMatrixTest {

    @TempDir
    Path directory;

    /**
     * The normal return is carried out once at each boundary, with the official frequency controlled by a visible acceptance parameter.
     *
     * @return the operation result
     */
    static Stream<CrashCase> cases() {
        int repeats = Integer.getInteger("azir.wal.crashRepeats", 1);
        int random = Integer.getInteger("azir.wal.randomCrashes", 2);
        return Stream.concat(Stream.of("BEFORE_APPEND", "AFTER_FORCE", "BEFORE_PUBLISH", "DURING_INSTALL", "AFTER_PUBLISH", "ACK", "RECOVERY")
                .flatMap(stage -> IntStream.range(0, repeats).mapToObj(n -> new CrashCase(stage, n))),
                IntStream.range(0, random).mapToObj(n -> new CrashCase("RANDOM", n)));
    }

    /**
     * All processes have been created by this test and must wait for exit to try recovery after killing.
     *
     * @param scenario scenario
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("cases")
    void successfulTransactionsSurviveRealProcessDeath(final CrashCase scenario) throws Exception {
        boolean random = scenario.stage.equals("RANDOM");
        var child = launch(random ? WalCrashWorkloadProcess.class : WalCrashProcess.class,
                random ? List.of(directory.toString()) : List.of(directory.toString(), scenario.stage.equals("RECOVERY") ? "ACK" : scenario.stage));
        try {
            waitReady(child, "ready");
            if (random) {
                Thread.sleep(new Random(99123L + scenario.repetition).nextInt(21));
            }
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(30, TimeUnit.SECONDS), "owned child must terminate");
        }
        if (scenario.stage.equals("RECOVERY")) {
            var recovering = launch(WalReopenProcess.class, List.of(directory.toString()));
            try {
                waitReady(recovering, "recovery-ready");
            } finally {
                recovering.destroyForcibly();
                assertTrue(recovering.waitFor(30, TimeUnit.SECONDS));
            }
        }
        if (random) {
            verifyRandom(scenario);
        } else {
            verifyBoundary(scenario);
        }
    }

    /**
     * The child JVM uses only the current catalogue of compiled products and quarantines, without touching any existing services.
     *
     * @param main main
     * @param arguments arguments
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private Process launch(final Class<?> main, final List<String> arguments) throws IOException {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx128m");
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(main.getName());
        command.addAll(arguments);
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
    }

    /**
     * Use a loose but bounded schedule budget, without miscalculating the short-time load of other inmate tasks as protocol failure.
     *
     * @param child child
     * @param barrier barrier
     * @throws Exception if the operation cannot complete
     */
    private void waitReady(final Process child, final String barrier) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!Files.exists(directory.resolve(barrier))) {
            if (!child.isAlive() || System.nanoTime() >= deadline) {
                fail("child did not reach barrier: " + Files.readString(directory.resolve("child.log")));
            }
            Thread.sleep(2);
        }
    }

    /**
     * The fixed border must be recovery block cross-shard transaction and no durable commit before adding it.
     *
     * @param scenario scenario
     * @throws IOException if the operation cannot complete
     */
    private void verifyBoundary(final CrashCase scenario) throws IOException {
        UUID id = UUID.fromString(Files.readString(directory.resolve("transaction-id")));
        boolean committed = !scenario.stage.equals("BEFORE_APPEND");
        for (int reopen = 0; reopen < 2; reopen++) {
            try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
                Assertions.assertEquals(committed, runtime.findCommitted(id).isPresent());
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                var snapshot = runtime.beginReadStatement(tx);
                Assertions.assertEquals(committed, runtime.get(tx, "a", "balance", snapshot).isPresent());
                Assertions.assertEquals(committed, runtime.get(tx, "b", "balance", snapshot).isPresent());
                if (committed) {
                    assertArrayEquals(new byte[]{90}, runtime.get(tx, "a", "balance", snapshot).orElseThrow());
                    assertArrayEquals(new byte[]{110}, runtime.get(tx, "b", "balance", snapshot).orElseThrow());
                    Assertions.assertEquals(1, runtime.findCommitted(id).orElseThrow().commitSequence());
                }
                runtime.rollback(tx);
            }
        }
        save(scenario, "id\tcommitted\n" + id + "\t" + committed + "\n", null);
    }

    /**
     * It has been confirmed that the record must exist, that the unconfirmed record is checked by an recovery log and that the final two are identical.
     *
     * @param scenario scenario
     * @throws IOException if the operation cannot complete
     */
    private void verifyRandom(final CrashCase scenario) throws IOException {
        String content = Files.readString(directory.resolve("events.tsv"));
        var lines = content.substring(0, content.lastIndexOf('\n') + 1).lines().toList();
        var events = new ArrayList<Event>();
        for (String line : lines) {
            String[] fields = line.split("\t", -1);
            // The full end of the line is only observed and not recognized as a half-line.
            Assertions.assertEquals(5, fields.length, "corrupt event evidence");
            events.add(new Event(fields[0], UUID.fromString(fields[1]), Long.parseLong(fields[2]), fields[3], fields[4]));
        }
        var attempts = new HashMap<UUID, Event>();
        var acknowledgements = new HashMap<UUID, Long>();
        for (Event event : events) {
            if (event.kind.equals("ATTEMPT")) {
                assertNull(attempts.put(event.id, event));
            } else {
                Assertions.assertEquals("ACK", event.kind);
                assertTrue(attempts.containsKey(event.id));
                assertNull(acknowledgements.put(event.id, event.sequence));
            }
        }
        assertTrue(acknowledgements.size() >= 3, "must kill after a real acknowledged prefix");
        var first = recoverRandom(attempts, acknowledgements);
        var second = recoverRandom(attempts, acknowledgements);
        Assertions.assertEquals(first, second, "recovery must be idempotent");
        var output = new StringBuilder("id\tsequence\tkey\tvalue\tacknowledged\n");
        for (var entry : first.entrySet()) {
            var event = attempts.get(entry.getKey());
            output.append(event.id).append('\t').append(entry.getValue()).append('\t').append(event.key).append('\t')
                    .append(event.value).append('\t').append(acknowledgements.containsKey(event.id)).append('\n');
        }
        save(scenario, output.toString(), directory.resolve("events.tsv"));
    }

    /**
     * The reference model uses only external attempts and recovery receipt and does not read internal version chain.
     *
     * @param attempts attempts
     * @param acknowledgements acknowledgements
     * @return the operation result
     */
    private Map<UUID, Long> recoverRandom(final Map<UUID, Event> attempts, final Map<UUID, Long> acknowledgements) {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            var recovered = new HashMap<UUID, Long>();
            var latest = new HashMap<String, Event>();
            var ordered = new java.util.TreeMap<Long, Event>();
            attempts.forEach((id, attempt) -> runtime.findCommitted(id).ifPresent(receipt -> {
                assertNull(recovered.put(id, receipt.commitSequence()));
                assertNull(ordered.put(receipt.commitSequence(), attempt));
            }));
            acknowledgements.forEach((id, sequence) -> Assertions.assertEquals(sequence, recovered.get(id), "acknowledged transaction lost"));
            Assertions.assertEquals(runtime.stats().publishedSequence(), recovered.size(), "unexplained durable transaction");
            long expectedSequence = 0;
            for (var entry : ordered.entrySet()) {
                Assertions.assertEquals(++expectedSequence, entry.getKey());
                latest.put(entry.getValue().key, entry.getValue());
            }
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            var snapshot = runtime.beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) {
                var scanned = runtime.scan(tx, shard, null, null, snapshot);
                var expectedKeys = new java.util.TreeSet<String>();
                for (var entry : latest.entrySet()) {
                    String expected = entry.getValue().value.equals("<NULL>") ? null : entry.getValue().value;
                    Assertions.assertEquals(expected, runtime.get(tx, shard, entry.getKey(), snapshot).map(WalMvccRuntimeTest::text).orElse(null));
                    if (expected != null) {
                        expectedKeys.add(entry.getKey());
                        Assertions.assertEquals(expected, text(scanned.get(entry.getKey())));
                    }
                }
                Assertions.assertEquals(expectedKeys, scanned.keySet(), "scan diverges after recovery");
            }
            runtime.rollback(tx);
            return recovered;
        }
    }

    /**
     * Successfully, evidence of acceptance was preserved, containing original attempts and confirmation records prior to random and forcible killing.
     *
     * @param scenario scenario
     * @param result result
     * @param source database connection source
     * @throws IOException if the operation cannot complete
     */
    private static void save(final CrashCase scenario, final String result, final Path source) throws IOException {
        String target = System.getProperty("azir.wal.crashEvidence");
        if (target == null) {
            return;
        }
        Path root = Path.of(target);
        Files.createDirectories(root);
        Files.writeString(root.resolve(scenario + "-recovered.tsv"), result, StandardCharsets.UTF_8);
        if (source != null) {
            Files.copy(source, root.resolve(scenario + "-events.tsv"));
        }
    }

    /**
     * An independent number of the fixed boundary and random moment scene.
     *
     * @param stage stage
     * @param repetition repetition
     */
    private record CrashCase(String stage, int repetition) {

        /**
         * Renewable scene labels are maintained in the test report.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return stage + "-" + repetition;
        }
    }

    /**
     * The caller actually obtained an attempt or confirmation before the killing.
     *
     * @param kind kind
     * @param id id
     * @param sequence sequence
     * @param key business key
     * @param value value to write
     */
    private record Event(String kind, UUID id, long sequence, String key, String value) {
    }
}
