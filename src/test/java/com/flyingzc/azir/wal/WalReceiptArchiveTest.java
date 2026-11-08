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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * An independent check of disk receipt index, negative filtering and damage detection cannot be based on a probability hit as commit evidence.
 */
class WalReceiptArchiveTest {

    @TempDir
    Path directory;

    /**
     * Checks the UUID, serial number and summary over multiple pages, as well as the non-existent UUID.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void sortedPagesReturnOnlyExactReceipts() throws Exception {
        UUID engine = UUID.randomUUID();
        var receipts = records(250);
        var descriptor = WalReceiptArchive.create(directory, engine, receipts, Long.MAX_VALUE);
        var archive = WalReceiptArchive.open(directory, engine, descriptor);
        for (var receipt : receipts) {
            Assertions.assertEquals(receipt, archive.find(receipt.transactionId()).orElseThrow());
        }
        assertTrue(archive.find(UUID.randomUUID()).isEmpty());
        var scanned = new ArrayList<WalReceipt>();
        archive.forEach(scanned::add);
        Assertions.assertEquals(Set.copyOf(receipts), Set.copyOf(scanned));
    }

    /**
     * The filter damage must be refused to open and cannot produce a false negative of the query.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void damagedBloomFailsBeforeLookup() throws Exception {
        UUID engine = UUID.randomUUID();
        var descriptor = WalReceiptArchive.create(directory, engine, records(20), Long.MAX_VALUE);
        Path file = directory.resolve(descriptor.fileName());
        byte[] data = Files.readAllBytes(file);
        data[70] ^= 1;
        Files.write(file, data);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, engine, descriptor));
    }

    /**
     * The key or value damage in the index page is rejected by the entire page validate, and cannot be returned due to a split error.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void damagedIndexPageFailsBeforeUsingKeys() throws Exception {
        UUID engine = UUID.randomUUID();
        var descriptor = WalReceiptArchive.create(directory, engine, records(20), Long.MAX_VALUE);
        Path file = directory.resolve(descriptor.fileName());
        byte[] data = Files.readAllBytes(file);
        data[data.length - 100] ^= 1;
        Files.write(file, data);
        var archive = WalReceiptArchive.open(directory, engine, descriptor);
        assertThrows(IOException.class, () -> archive.forEach(receipt -> {
        }));
    }

    /**
     * None of the different engines, cut-off or tail multiple records can be considered a normal archive.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void identityAndExactSizeAreRequired() throws Exception {
        UUID engine = UUID.randomUUID();
        var descriptor = WalReceiptArchive.create(directory, engine, records(2), Long.MAX_VALUE);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, UUID.randomUUID(), descriptor));
        Files.write(directory.resolve(descriptor.fileName()), new byte[]{1}, java.nio.file.StandardOpenOption.APPEND);
        assertThrows(IOException.class, () -> WalReceiptArchive.open(directory, engine, descriptor));
    }

    /**
     * Neither filters with any assigned size can omit the marked UUID.
     */
    @Test
    void bloomNeverRejectsInsertedIds() {
        for (int count : new int[]{1, 50, 1000, 10_000}) {
            var filter = WalBloom.allocate(count, 16 * 1024 * 1024);
            var receipts = records(count);
            receipts.forEach(receipt -> filter.add(receipt.transactionId()));
            var restored = new WalBloom(filter.bytes());
            receipts.forEach(receipt -> assertTrue(restored.mightContain(receipt.transactionId())));
        }
    }

    /**
     * Creates a stable format without external dependence receipt data.
     *
     * @param count count
     * @return the operation result
     */
    private static List<WalReceipt> records(final int count) {
        var result = new ArrayList<WalReceipt>();
        for (int i = 1; i <= count; i++) {
            result.add(new WalReceipt(UUID.randomUUID(), i, WalCodec.digest(new byte[]{(byte) i})));
        }
        return result;
    }
}
