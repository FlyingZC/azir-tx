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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Immutable commit receipt Archive: Accurate UUID sorting, Blond negative filtering and page-by-page validate.
 */
final class WalReceiptArchive {

    private static final int MAGIC = 0x415A5243;

    private static final int HEADER = 64;

    private static final int PAGE = 4096;

    private static final int RECORD = 56;

    private static final int PER_PAGE = (PAGE - 4) / RECORD;

    private final Path path;

    private final Descriptor descriptor;

    private final WalBloom bloom;

    /**
     * Loads bounded filters and verify engines identity and metadata summaries.
     *
     * @param root root
     * @param engine engine
     * @param expected expected
     * @throws IOException if the operation cannot complete
     */
    private WalReceiptArchive(final Path root, final UUID engine, final Descriptor expected) throws IOException {
        path = root.resolve(expected.fileName);
        descriptor = expected;
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            byte[] header = WalFiles.read(channel, 0, HEADER);
            var in = ByteBuffer.wrap(header);
            if (in.getInt() != MAGIC || in.getInt() != 1 || !engine.equals(new UUID(in.getLong(), in.getLong()))
                    || in.getLong() != expected.count || in.getLong() != expected.minSequence
                    || in.getLong() != expected.maxSequence || in.getLong() != expected.expiresAtMillis) {
                throw new IOException("receipt archive identity mismatch");
            }
            int length = in.getInt();
            int checksum = in.getInt();
            if (length < 128 || length > 2 * 1024 * 1024 || Integer.bitCount(length) != 1) {
                throw new IOException("invalid archive bloom size");
            }
            byte[] bits = WalFiles.read(channel, HEADER, length);
            if (metadataCrc(header, bits) != checksum || !metadataHash(header, bits).equals(expected.metadataHash)) {
                throw new IOException("receipt archive metadata checksum mismatch");
            }
            bloom = new WalBloom(bits);
            if (channel.size() != diskBytes(expected.count, length)) {
                throw new IOException("receipt archive size mismatch");
            }
        }
    }

    /**
     * Opens an archive that has been cited at the current checkpoint.
     *
     * @param root root
     * @param engine engine
     * @param descriptor descriptor
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalReceiptArchive open(final Path root, final UUID engine, final Descriptor descriptor) throws IOException {
        return new WalReceiptArchive(root, engine, descriptor);
    }

    /**
     * Creates an orderly archive and synchronises the text; the catalogue items are synchronized by a single intergenerational release protocol.
     *
     * @param root root
     * @param engine engine
     * @param receipts receipts
     * @param expiresAtMillis expires at millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Descriptor create(final Path root, final UUID engine, final List<WalReceipt> receipts, final long expiresAtMillis) throws IOException {
        return create(root, engine, receipts, expiresAtMillis, WalLog.NORMAL_IO);
    }

    /**
     * Injecting failure in a real receipt file, part of the archive before it is released will never be recovery authority.
     *
     * @param root root
     * @param engine engine
     * @param receipts receipts
     * @param expiresAtMillis expires at millis
     * @param io io
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static Descriptor create(final Path root, final UUID engine, final List<WalReceipt> receipts, final long expiresAtMillis, final WalLog.IoHook io) throws IOException {
        if (receipts.isEmpty()) {
            throw new IllegalArgumentException("empty archive");
        }
        var sorted = new ArrayList<>(receipts);
        sorted.sort((left, right) -> compare(left.transactionId(), right.transactionId()));
        var filter = WalBloom.allocate(sorted.size(), 2 * 1024 * 1024);
        long min = Long.MAX_VALUE;
        long max = 0;
        UUID previous = null;
        for (var receipt : sorted) {
            if (receipt.transactionId().equals(previous)) {
                throw new IOException("duplicate archived receipt");
            }
            filter.add(receipt.transactionId());
            previous = receipt.transactionId();
            min = Math.min(min, receipt.commitSequence());
            max = Math.max(max, receipt.commitSequence());
        }
        byte[] bits = filter.bytes();
        byte[] header = ByteBuffer.allocate(HEADER).putInt(MAGIC).putInt(1)
                .putLong(engine.getMostSignificantBits()).putLong(engine.getLeastSignificantBits()).putLong(sorted.size())
                .putLong(min).putLong(max).putLong(expiresAtMillis).putInt(bits.length).array();
        ByteBuffer.wrap(header).putInt(60, metadataCrc(header, bits));
        String name = "receipts-" + UUID.randomUUID() + ".sst";
        try (var channel = FileChannel.open(root.resolve(name), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            WalFiles.write(channel, ByteBuffer.wrap(header), name, io);
            WalFiles.write(channel, ByteBuffer.wrap(bits), name, io);
            for (int first = 0; first < sorted.size(); first += PER_PAGE) {
                var page = ByteBuffer.allocate(PAGE);
                for (int i = first; i < Math.min(first + PER_PAGE, sorted.size()); i++) {
                    var receipt = sorted.get(i);
                    page.putLong(receipt.transactionId().getMostSignificantBits()).putLong(receipt.transactionId().getLeastSignificantBits())
                            .putLong(receipt.commitSequence()).put(HexFormat.of().parseHex(receipt.digest()));
                }
                page.putInt(PAGE - 4, WalFiles.crc(page.array(), 0, PAGE - 4));
                page.position(0);
                WalFiles.write(channel, page, name, io);
            }
            io.beforeCheckpointForce(name);
            channel.force(true);
            io.afterCheckpointForce(name);
        }
        return new Descriptor(name, sorted.size(), min, max, expiresAtMillis, metadataHash(header, bits));
    }

    /**
     * After Blom's hit, the UUID was given a precise split; any damage on the index page refused to return the result.
     *
     * @param id id
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    Optional<WalReceipt> find(final UUID id) throws IOException {
        if (!bloom.mightContain(id)) {
            return Optional.empty();
        }
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long low = 0;
            long high = descriptor.count - 1;
            long cachedPage = -1;
            byte[] data = null;
            while (low <= high) {
                long middle = (low + high) >>> 1;
                long number = middle / PER_PAGE;
                if (number != cachedPage) {
                    data = readPage(channel, number);
                    cachedPage = number;
                }
                var receipt = decode(data, (int) (middle % PER_PAGE));
                int order = compare(receipt.transactionId(), id);
                if (order == 0) {
                    return Optional.of(receipt);
                }
                if (order < 0) {
                    low = middle + 1;
                } else {
                    high = middle - 1;
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Filters reassign or have an archive expired, page by page verify and rebuild and assemble to not load all receipt objects.
     *
     * @param consumer consumer
     * @throws IOException if the operation cannot complete
     */
    void forEach(final Consumer<WalReceipt> consumer) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            for (long first = 0; first < descriptor.count; first += PER_PAGE) {
                byte[] page = readPage(channel, first / PER_PAGE);
                for (int i = 0; i < Math.min(PER_PAGE, descriptor.count - first); i++) {
                    consumer.accept(decode(page, i));
                }
            }
        }
    }

    /**
     * Returns the filter size for a cache capacity account only.
     *
     * @return the operation result
     */
    int memoryBytes() {
        return bloom.byteSize();
    }

    /**
     * Returns the fixed physical size of the archive, which is used to check total budget before writing.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    long diskBytes() throws IOException {
        return Files.size(path);
    }

    /**
     * Estimate the physical size of the archive before writing.
     *
     * @param count count
     * @return the operation result
     */
    static long estimatedBytes(final int count) {
        return diskBytes(count, WalBloom.allocate(count, 2 * 1024 * 1024).bytes().length);
    }

    /**
     * The number of pages and the length of metadata are calculated to prevent the acceptance of missing or additional records at the end.
     *
     * @param count count
     * @param bloomBytes bloom bytes
     * @return the operation result
     */
    private static long diskBytes(final long count, final int bloomBytes) {
        return HEADER + bloomBytes + ((count + PER_PAGE - 1) / PER_PAGE) * PAGE;
    }

    /**
     * The index keys are used only after a check of the entire page, so that the bad key does not lead to the wrong dichotomy.
     *
     * @param channel channel
     * @param page page
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private byte[] readPage(final FileChannel channel, final long page) throws IOException {
        byte[] bytes = WalFiles.read(channel, HEADER + bloom.byteSize() + page * PAGE, PAGE);
        if (ByteBuffer.wrap(bytes).getInt(PAGE - 4) != WalFiles.crc(bytes, 0, PAGE - 4)) {
            throw new IOException("receipt index page checksum mismatch");
        }
        return bytes;
    }

    /**
     * Decoding an receipt, the commit serial number from the validate page must fall within this archive.
     *
     * @param page page
     * @param position position
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private WalReceipt decode(final byte[] page, final int position) throws IOException {
        var in = ByteBuffer.wrap(page);
        in.position(position * RECORD);
        UUID id = new UUID(in.getLong(), in.getLong());
        long sequence = in.getLong();
        byte[] digest = new byte[32];
        in.get(digest);
        if (sequence < descriptor.minSequence || sequence > descriptor.maxSequence) {
            throw new IOException("receipt sequence outside archive");
        }
        return new WalReceipt(id, sequence, HexFormat.of().formatHex(digest));
    }

    /**
     * UUID is sorted in no sign bytes, creating and searching are consistent.
     *
     * @param left left
     * @param right right
     * @return the operation result
     */
    private static int compare(final UUID left, final UUID right) {
        int high = Long.compareUnsigned(left.getMostSignificantBits(), right.getMostSignificantBits());
        return high != 0 ? high : Long.compareUnsigned(left.getLeastSignificantBits(), right.getLeastSignificantBits());
    }

    /**
     * The head validate also overwhelms the filter, and damage cannot make real receipt a false negative.
     *
     * @param header header
     * @param bits bits
     * @return the operation result
     */
    private static int metadataCrc(final byte[] header, final byte[] bits) {
        var crc = new CRC32C();
        crc.update(header, 0, HEADER - 4);
        crc.update(bits);
        return (int) crc.getValue();
    }

    /**
     * Ties a filter to the metadata identity to the overall summary of the checkpoint.
     *
     * @param header header
     * @param bits bits
     * @return the operation result
     */
    private static String metadataHash(final byte[] header, final byte[] bits) {
        byte[] data = new byte[header.length + bits.length];
        System.arraycopy(header, 0, data, 0, header.length);
        System.arraycopy(bits, 0, data, header.length, bits.length);
        return WalCodec.digest(data);
    }

    /**
     * The archive directory saves only a trusted metadata reference, the text is read on demand and not reload all transaction receipt.
     *
     * @param fileName file name
     * @param count count
     * @param minSequence min sequence
     * @param maxSequence max sequence
     * @param expiresAtMillis expires at millis
     * @param metadataHash metadata hash
     */
    record Descriptor(String fileName, long count, long minSequence, long maxSequence, long expiresAtMillis,
            String metadataHash) {

        /**
         * Rejects path crossing and unreasonable indexing boundaries.
         *
         * @param fileName file name
         * @param count count
         * @param minSequence min sequence
         * @param maxSequence max sequence
         * @param expiresAtMillis expires at millis
         * @param metadataHash metadata hash
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Descriptor {
            if (fileName == null || !fileName.matches("receipts-[a-f0-9-]{36}\\.sst") || count < 1 || count > 1_000_000
                    || minSequence < 1 || minSequence > maxSequence || expiresAtMillis < 0
                    || metadataHash == null || !metadataHash.matches("[a-f0-9]{64}")) {
                throw new IllegalArgumentException("invalid receipt archive descriptor");
            }
        }
    }
}
