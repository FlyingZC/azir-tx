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
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Injection of detectable damage to the head, frame and payload of the complete log shall be refused without interruption of evidence.
 */
class WalCorruptionMatrixTest {

    @TempDir
    Path directory;

    /**
     * The official scenes were 100 times per region and the general return was maintained twice per region.
     *
     * @return the operation result
     */
    static Stream<Damage> cases() {
        return Stream.of("FILE_HEADER", "FRAME_HEADER", "PAYLOAD").flatMap(region -> IntStream.range(0, Integer.getInteger("azir.wal.corruptionRepeats", 2)).mapToObj(n -> new Damage(region, n)));
    }

    /**
     * The single bit damage cannot be mistaken for disposal of tails, but can still be repaired after failure to open and the original log recovery.
     *
     * @param damage damage
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("cases")
    void completeRecordDamageFailsClosedWithoutDestroyingEvidence(final Damage damage) throws Exception {
        var records = IntStream.rangeClosed(1, 3).mapToObj(sequence -> new WalCommit(UUID.randomUUID(), sequence, sequence - 1,
                List.of(new WalMutation("a", "key:" + sequence, new byte[256]),
                        new WalMutation("b", "key:" + sequence, new byte[256]))))
                .toList();
        try (var log = new WalLog(directory, Set.of("a", "b"), 1_000_000)) {
            log.append(records);
        }
        Path file = directory.resolve("transactions.wal");
        byte[] intact = Files.readAllBytes(file);
        byte[] damaged = intact.clone();
        var random = new Random(20261010L + damage.repetition);
        int recordIndex = damage.repetition % records.size();
        int offset = 60;
        for (int i = 0; i < recordIndex; i++) {
            offset += 16 + WalCodec.encode(records.get(i)).length;
        }
        int position = switch (damage.region) {
            case "FILE_HEADER" -> random.nextInt(60);
            case "FRAME_HEADER" -> offset + random.nextInt(16);
            case "PAYLOAD" -> offset + 16 + random.nextInt(WalCodec.encode(records.get(recordIndex)).length);
            default -> throw new IllegalArgumentException(damage.region);
        };
        damaged[position] ^= (byte) (1 << random.nextInt(8));
        Files.write(file, damaged);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a", "b"), 1_000_000));
        assertArrayEquals(damaged, Files.readAllBytes(file), "failed recovery must preserve corrupted evidence");
        Files.write(file, intact);
        try (var repaired = new WalLog(directory, Set.of("a", "b"), 1_000_000)) {
            Assertions.assertEquals(3, repaired.lastSequence());
            for (WalCommit record : records) {
                Assertions.assertEquals(record.digest(), repaired.receipt(record.transactionId()).orElseThrow().digest());
            }
        }
    }

    /**
     * Indicates the area of damage and the random location where it can be repeated.
     *
     * @param region region
     * @param repetition repetition
     */
    private record Damage(String region, int repetition) {

        /**
         * Keep specific failure identity in the junit.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return region + "-" + repetition;
        }
    }
}
