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

package com.flyingzc.azir.wal.ha;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.UUID;

/**
 * Installs the real atomic replacement of a pointer and an retirement log and can retry be deleted without synchronizing that the file does not exist as a directory.
 */
final class RatisRecordFile {

    /**
     * All normal access points are covered by a real file system.
     */
    static final Io REAL = new Io() {
    };

    /**
     * Tools cannot be exemplified.
     */
    private RatisRecordFile() {
    }

    /**
     * The caller holds an generation lock and the coding and name range is determined by a fixed protocol record.
     *
     * @param target target
     * @param bytes bytes
     * @param prefix prefix
     * @param maximum maximum
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void publish(final Path target, final byte[] bytes, final String prefix, final int maximum, final Io io) throws IOException {
        if (bytes.length < 1 || bytes.length > maximum) {
            throw new IOException("mutable record exceeds encoding budget");
        }
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("mutable record target is not regular");
        }
        Path temporary = target.resolveSibling(prefix + UUID.randomUUID());
        try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.write(channel, bytes);
            io.forceFile(channel);
        }
        io.replace(temporary, target);
        io.forceDirectory(target.getParent());
    }

    /**
     * Last time the deletion occurred but synchronisation failed, retry was still synchronizing the directory and should not be successful simply because the records disappeared.
     *
     * @param target target
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void remove(final Path target, final Io io) throws IOException {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("mutable record deletion target is not regular");
        }
        io.delete(target);
        io.forceDirectory(target.getParent());
    }

    /**
     * There is also a correct record to reconfirm content and directory for creating durable basis before destructive collection.
     *
     * @param target target
     * @param expected expected
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void confirm(final Path target, final byte[] expected, final Io io) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != expected.length || !java.util.Arrays.equals(Files.readAllBytes(target), expected)) {
            throw new IOException("mutable record changed before durability confirmation");
        }
        try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.forceFile(channel);
        }
        io.forceDirectory(target.getParent());
    }

    /**
     * The I/O injection in the package allows failure before and after the actual operation, with no global variable failure switch on the official path.
     */
    interface Io {

        /**
         * It is written in part that only private temporary storage can be contaminated.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        default void write(final FileChannel channel, final byte[] bytes) throws IOException {
            RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes));
        }

        /**
         * Make the complete contents durable before publishing the final filename.
         *
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        default void forceFile(final FileChannel channel) throws IOException {
            channel.force(true);
        }

        /**
         * Always atomic replaces the full old record and is not allowed to be downgraded to cover in situ.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        default void replace(final Path source, final Path target) throws IOException {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }

        /**
         * Deleting records also allow retry, which must still be synchronized with parent directories.
         *
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        default void delete(final Path target) throws IOException {
            Files.deleteIfExists(target);
        }

        /**
         * Confirms the list of entries that are replaced or deleted.
         *
         * @param parent parent
         * @throws IOException if the operation cannot complete
         */
        default void forceDirectory(final Path parent) throws IOException {
            RatisGenerationRecord.forceDirectory(parent);
        }
    }
}
