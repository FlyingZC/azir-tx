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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The actual reading and writing, cut-off and identity verify in the backup format cannot be replaced by a full file summary for content semantic examination.
 */
class WalBackupDataTest {

    /**
     * Unicode, empty keys and delete version complete round trip with only one line held by the consumer.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void streamsValuesAndTombstonesInBinaryKeyOrder() throws Exception {
        var header = header();
        var bytes = new ByteArrayOutputStream();
        var writer = new WalBackupData.Writer(bytes, header);
        List<String> keys = List.of("", "a\u0000b", "中", "x\uE000", "x😀").stream().sorted(com.flyingzc.azir.storage.Utf8KeyOrder::compare).toList();
        for (int i = 0; i < keys.size(); i++) {
            writer.accept(new WalBackupRow(keys.get(i), new WalValue(i + 1, i == 2 ? null : new byte[]{(byte) i})));
        }
        Assertions.assertEquals(keys.size(), writer.finish());
        var rows = new ArrayList<WalBackupRow>();
        Assertions.assertEquals(keys.size(), WalBackupData.read(new ByteArrayInputStream(bytes.toByteArray()), header, rows::add));
        Assertions.assertEquals(keys, rows.stream().map(WalBackupRow::key).toList());
        assertTrue(rows.get(2).version().deleted());
        for (int i = 0; i < rows.size(); i++) {
            if (i != 2) {
                assertArrayEquals(new byte[]{(byte) i}, rows.get(i).version().value());
            }
        }
    }

    /**
     * Every incomplete prefix must be rejected, even if it happens to contain a number of complete lines.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void everyTruncatedPrefixIsRejected() throws Exception {
        byte[] valid = encoded();
        for (int size = 0; size < valid.length; size++) {
            byte[] prefix = java.util.Arrays.copyOf(valid, size);
            assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(prefix), header(), row -> {
            }));
        }
    }

    /**
     * Subsequent bytes, error lines and error engine identity cannot be used as the same complete backup.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void rejectsTrailingDataWrongFooterAndIdentity() throws Exception {
        byte[] bytes = encoded();
        byte[] trailing = java.util.Arrays.copyOf(bytes, bytes.length + 1);
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(trailing), header(), row -> {
        }));
        bytes[bytes.length - 1] ^= 1;
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(bytes), header(), row -> {
        }));
        assertThrows(IOException.class, () -> WalBackupData.read(new ByteArrayInputStream(encoded()),
                new WalBackupData.Header(UUID.randomUUID(), "a", header().boundary()), row -> {
                }));
    }

    /**
     * Repeats or reverses keys, crosses version and repeats end must be rejected at the encoding.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void writerRejectsInvalidOrderAndBoundary() throws Exception {
        var writer = new WalBackupData.Writer(new ByteArrayOutputStream(), header());
        writer.accept(new WalBackupRow("b", new WalValue(1, null)));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("b", new WalValue(2, null))));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("a", new WalValue(2, null))));
        assertThrows(IOException.class, () -> writer.accept(new WalBackupRow("c", new WalValue(11, null))));
        writer.finish();
        assertThrows(IOException.class, writer::finish);
        assertThrows(IllegalArgumentException.class, () -> new WalBackupRow("k", new WalValue(0, null)));
    }

    /**
     * Provides a stable re-emergence and consistent call for different tests on the head.
     *
     * @return the operation result
     */
    private static WalBackupData.Header header() {
        return new WalBackupData.Header(new UUID(1, 2), "a", new WalCheckpoint(10, "0".repeat(64)));
    }

    /**
     * Generates a genuine, complete-format sample for use in the byte cut and damage test.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static byte[] encoded() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var writer = new WalBackupData.Writer(buffer, header());
        writer.accept(new WalBackupRow("key", new WalValue(1, new byte[]{1, 2, 3})));
        writer.finish();
        return buffer.toByteArray();
    }
}
