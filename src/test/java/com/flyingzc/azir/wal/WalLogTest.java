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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * An independent acceptance of the log layer durable, identity, validate and the cut-off tail.
 */
class WalLogTest {

    @TempDir
    Path directory;

    /**
     * Multiple transaction shares a brush and closes and restarts to keep identity, write set and summary.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void batchRoundTripRetainsIdentityAndReceipts() throws Exception {
        var first = record(1);
        var second = record(2);
        UUID engine;
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            engine = log.engineId();
            log.append(List.of(first, second));
            Assertions.assertEquals(1, log.forces());
        }
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            Assertions.assertEquals(engine, log.engineId());
            Assertions.assertEquals(2, log.lastSequence());
            Assertions.assertEquals(first.digest(), log.receipt(first.transactionId()).orElseThrow().digest());
            Assertions.assertEquals(second.transactionId(), log.read(2, 1).getFirst().transactionId());
            assertArrayEquals(second.mutations().getFirst().value(), log.read(2, 1).getFirst().mutations().getFirst().value());
        }
    }

    /**
     * Only the uncompleted tails can be repaired and new transaction may be added on a continuous basis.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void incompleteTailIsDiscardedWithoutLosingCompletePrefix() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            log.append(List.of(record(1)));
        }
        long completeSize = Files.size(file());
        byte[] data = WalCodec.encode(record(2));
        ByteBuffer frame = ByteBuffer.allocate(16 + data.length);
        frame.putInt(0x415A574C).putInt(data.length).putInt(crc(data));
        frame.putInt(crc(Arrays.copyOf(frame.array(), 12))).put(data);
        Files.write(file(), Arrays.copyOf(frame.array(), 20), StandardOpenOption.APPEND);
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            Assertions.assertEquals(1, log.lastSequence());
            Assertions.assertEquals(completeSize, Files.size(file()));
            log.append(List.of(record(2)));
            Assertions.assertEquals(2, log.lastSequence());
        }
    }

    /**
     * Full recording of damage to content must fail to close and cannot be considered as discardable.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptPayloadIsRejectedWithoutTruncation() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            log.append(List.of(record(1)));
        }
        byte[] data = Files.readAllBytes(file());
        data[data.length - 1] ^= 1;
        Files.write(file(), data);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        Assertions.assertEquals(data.length, Files.size(file()));
    }

    /**
     * Length field damage cannot be disguised as unfinished tail and leads to the cut-off of commit data.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptLengthHeaderIsRejectedWithoutTruncation() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            log.append(List.of(record(1)));
        }
        byte[] data = Files.readAllBytes(file());
        ByteBuffer.wrap(data).putInt(64, 1_000_000);
        Files.write(file(), data);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        Assertions.assertEquals(data.length, Files.size(file()));
    }

    /**
     * The file lock prevents double-writing with the directory, and route changes must also be refused.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void exclusiveWriterAndRouteBindingAreEnforced() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertThrows(Exception.class, () -> new WalLog(directory, Set.of("a"), 1_000_000));
        }
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("b"), 1_000_000));
        try (var ignored = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertTrue(Files.exists(file()));
        }
    }

    /**
     * Transaction ID must be rejected before changing files without consecutive additions and repetitions.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidSequenceCannotChangeTheLog() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            assertThrows(IOException.class, () -> log.append(List.of(record(2))));
            Assertions.assertEquals(60, log.bytes());
        }
    }

    /**
     * The same batch repeats the transaction ID must be refused before brushing, and cannot write a log that does not work on recovery.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void duplicateTransactionIdsAreRejectedBeforeAppend() throws Exception {
        try (var log = new WalLog(directory, Set.of("a"), 1_000_000)) {
            var first = record(1);
            var second = new WalCommit(first.transactionId(), 2, 0, first.mutations());
            assertThrows(IOException.class, () -> log.append(List.of(first, second)));
            Assertions.assertEquals(60, log.bytes());
        }
    }

    /**
     * Returns the current test log path.
     *
     * @return the operation result
     */
    private Path file() {
        return directory.resolve("transactions.wal");
    }

    /**
     * Creates an independent transaction log entry.
     *
     * @param sequence sequence
     * @return the operation result
     */
    private static WalCommit record(final long sequence) {
        return new WalCommit(UUID.randomUUID(), sequence, 0,
                List.of(new WalMutation("a", "k", new byte[]{(byte) sequence})));
    }

    /**
     * Construct the correct frame validate for simulation writing to interrupt.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var crc = new CRC32C();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
