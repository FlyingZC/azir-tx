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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Strongly kills new files, atomic pointers and old generations to execute real processes. verify can be recovery for every success transaction.
 */
class WalCheckpointFaultTest {

    @TempDir
    Path directory;

    /**
     * The normal regression takes place once at each stage, with a formal test that expands the frequency of repetition and separately controls the frequency of initiation of sub-processes.
     *
     * @return the operation result
     */
    static Stream<Case> cases() {
        return Stream.of("BEFORE_CHECKPOINT_FILES", "AFTER_RECEIPTS_FORCE", "AFTER_SNAPSHOT_FORCE", "BEFORE_CURRENT_MOVE",
                "AFTER_CURRENT_MOVE", "AFTER_CURRENT_FORCE", "BEFORE_OLD_GENERATION_DELETE", "AFTER_OLD_GENERATION_DELETE",
                "PARTIAL_IMAGE", "PARTIAL_RECEIPT", "PARTIAL_POINTER").flatMap(stage -> IntStream.range(0, Integer.getInteger("azir.wal.checkpointFaultRepeats", 1)).mapToObj(n -> new Case(stage, n)));
    }

    /**
     * After any border collapse, recovery must be the same full prefix and repeated re-opening and subsequent additions cannot change decision.
     *
     * @param scenario scenario
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("cases")
    void killedCheckpointNeverLosesAcknowledgedTransactions(final Case scenario) throws Exception {
        long began = System.nanoTime();
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx128m");
        command.add("-XX:ActiveProcessorCount=1");
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(WalCheckpointCrashProcess.class.getName());
        command.add(directory.toString());
        command.add(scenario.stage);
        var child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!Files.exists(directory.resolve("ready"))) {
                if (!child.isAlive() || System.nanoTime() > deadline) {
                    fail("owned child missed checkpoint barrier: " + Files.readString(directory.resolve("child.log")));
                }
                Thread.sleep(2);
            }
        } finally {
            child.destroyForcibly();
            assertTrue(child.waitFor(30, TimeUnit.SECONDS));
        }
        UUID old = UUID.fromString(Files.readString(directory.resolve("old-id")));
        UUID current = UUID.fromString(Files.readString(directory.resolve("new-id")));
        for (int attempt = 0; attempt < 2; attempt++) {
            try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
                Assertions.assertEquals(2, runtime.stats().publishedSequence());
                Assertions.assertEquals(1, runtime.findCommitted(old).orElseThrow().commitSequence());
                Assertions.assertEquals(2, runtime.findCommitted(current).orElseThrow().commitSequence());
                var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
                var snapshot = runtime.beginReadStatement(reader);
                Assertions.assertEquals("new", text(runtime.get(reader, "a", "k", snapshot).orElseThrow()));
                Assertions.assertEquals("new", text(runtime.get(reader, "b", "k", snapshot).orElseThrow()));
                runtime.rollback(reader);
            }
        }
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(tx, "a", "after", bytes("continued"));
            Assertions.assertEquals(3, runtime.commit(tx).commitSequence());
            assertTrue(runtime.checkpoint().created());
        }
        String evidence = System.getProperty("azir.wal.checkpointEvidence");
        if (evidence != null) {
            Path root = Path.of(evidence);
            Files.createDirectories(root);
            Files.writeString(root.resolve(scenario + ".tsv"), "old_id\tnew_id\tconfirmed_sequence\n" + old + "\t" + current + "\t2\n");
        }
        // The formal acceptance will limit the start-up rate of this test subprocess at intervals and will not take advantage of the schedule settings for other tasks of the user.
        long minimum = TimeUnit.MILLISECONDS.toNanos(Long.getLong("azir.wal.checkpointFaultIntervalMillis", 0));
        long remaining = minimum - (System.nanoTime() - began);
        if (remaining > 0) {
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    /**
     * Revertible failure identity under a standalone directory.
     *
     * @param stage stage
     * @param repetition repetition
     */
    private record Case(String stage, int repetition) {

        /**
         * Retain the phase and rotation in the report.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return stage + "-" + repetition;
        }
    }
}
