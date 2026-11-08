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
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * The successful prefix must be retained if the device error is inserted before or after the parts of a genuine new generation document are written and brushed.
 */
class WalCheckpointIoFailureTest {

    @TempDir
    Path directory;

    /**
     * Mirrors, receipt, end logs and pointers cover partial writing, pre-painting and post-painting anomalies, respectively.
     *
     * @return the operation result
     */
    static Stream<Failure> cases() {
        return Stream.of("IMAGE", "RECEIPTS", "TAIL", "POINTER")
                .flatMap(kind -> Stream.of("PARTIAL", "BEFORE_FORCE", "AFTER_FORCE").flatMap(boundary -> java.util.stream.IntStream.range(0, Integer.getInteger("azir.wal.checkpointIoRepeats", 1))
                        .mapToObj(n -> new Failure(kind, boundary, n))));
    }

    /**
     * After the failure of the second check point, both recovery confirmed transaction must be made regardless of whether the first generation has been cut off.
     *
     * @param scenario scenario
     */
    @ParameterizedTest
    @MethodSource("cases")
    void fileErrorsNeverDeleteTheOnlyRecoverableState(final Failure scenario) {
        long began = System.nanoTime();
        var armed = new AtomicBoolean();
        var io = new WalLog.IoHook() {

            /**
             * Stopping after 32 byte of the true document cannot replace the part with an anomaly.
             *
             * @param name name
             * @param position position
             * @param remaining remaining
             * @return the operation result
             */
            @Override
            public int checkpointWriteLimit(final String name, final long position, final int remaining) {
                return armed.get() && matches(scenario.kind, name) && scenario.boundary.equals("PARTIAL") ? Math.min(32, remaining) : remaining;
            }

            /**
             * Cannot initialise Evolution's mail component.
             *
             * @param name name
             * @param position position
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void afterCheckpointWrite(final String name, final long position) throws IOException {
                fail(name, "PARTIAL");
            }

            /**
             * The complete document has been written, but failed before brushing.
             *
             * @param name name
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void beforeCheckpointForce(final String name) throws IOException {
                fail(name, "BEFORE_FORCE");
            }

            /**
             * If the brush is successful but the caller receives an anomaly, recovery must retain the previously confirmed prefix.
             *
             * @param name name
             * @throws IOException if the operation cannot complete
             */
            @Override
            public void afterCheckpointForce(final String name) throws IOException {
                fail(name, "AFTER_FORCE");
            }

            /**
             * Injection is carried out only at the target document and target stage.
             *
             * @param name name
             * @param boundary boundary
             * @throws IOException if the operation cannot complete
             */
            private void fail(final String name, final String boundary) throws IOException {
                if (armed.get() && matches(scenario.kind, name) && scenario.boundary.equals(boundary)) {
                    throw new IOException("injected checkpoint device failure");
                }
            }
        };
        UUID first;
        UUID second;
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a"), Map.of(), WalOptions.defaults(), stage -> {
        }, io, System::nanoTime)) {
            first = writeValue(runtime, "one");
            runtime.collectGarbage(10);
            runtime.checkpoint();
            second = writeValue(runtime, "two");
            armed.set(true);
            assertThrows(WalMvccException.class, runtime::checkpoint);
            assertThrows(WalMvccException.class, () -> runtime.begin(IsolationLevel.REPEATABLE_READ));
        }
        try (var reopened = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, reopened.findCommitted(first).orElseThrow().commitSequence());
            Assertions.assertEquals(2, reopened.findCommitted(second).orElseThrow().commitSequence());
            var reader = reopened.begin(IsolationLevel.REPEATABLE_READ);
            Assertions.assertEquals("two", text(reopened.get(reader, "a", "k").orElseThrow()));
            reopened.rollback(reader);
            assertTrue(reopened.checkpoint().created());
        }
        // Only the frequency of implementation of wrong equipment usage in this round is reduced and no adjustments are made to other processes of users.
        long minimum = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Long.getLong("azir.wal.checkpointIoIntervalMillis", 0));
        long remaining = minimum - (System.nanoTime() - began);
        if (remaining > 0) {
            java.util.concurrent.locks.LockSupport.parkNanos(remaining);
        }
    }

    /**
     * Accurately identifies the files generated by this book and does not normally add WAL to the failure scene at the checkpoint.
     *
     * @param kind kind
     * @param name name
     * @return the operation result
     */
    private static boolean matches(final String kind, final String name) {
        return switch (kind) {
            case "IMAGE" -> name.equals("snapshot.bin");
            case "TAIL" -> name.equals("tail.wal");
            case "RECEIPTS" -> name.startsWith("receipts-");
            case "POINTER" -> name.startsWith("CURRENT-");
            default -> false;
        };
    }

    /**
     * Record that transaction identity has returned successfully.
     *
     * @param runtime runtime
     * @param value value to write
     * @return the operation result
     */
    private static UUID writeValue(final WalMvccRuntime runtime, final String value) {
        var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
        runtime.put(tx, "a", "k", bytes(value));
        runtime.commit(tx);
        return tx.id();
    }

    /**
     * An independent failure scene of document type and brushing borders.
     *
     * @param kind kind
     * @param boundary boundary
     * @param repetition repetition
     */
    private record Failure(String kind, String boundary, int repetition) {

        /**
         * Keep precision failed identity in the report.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return kind + "-" + boundary + "-" + repetition;
        }
    }
}
