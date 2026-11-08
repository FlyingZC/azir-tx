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

import java.util.Arrays;
import java.util.UUID;

/**
 * The unified durable control policy is issued with both the correct document and re-synchronisation, which cannot be used to impersonate the last sync success.
 */
final class RatisPolicyFile {

    /**
     * The formal assembly does not use any injection or global variable failure switches.
     */
    static final Io REAL = new Io() {
    };

    /**
     * Prevent instantiation of this utility class.
     */
    private RatisPolicyFile() {
    }

    /**
     * Node has a unique directory lock to call, and any operation failed is spread, followed by retry to keep and reconfirm the full result.
     *
     * @param root root
     * @param kind kind
     * @param bytes bytes
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void publish(final Path root, final Kind kind, final byte[] bytes, final Io io) throws IOException {
        if (bytes.length < 1 || bytes.length > 512) {
            throw new IOException("policy encoding exceeds budget");
        }
        Path target = root.resolve(kind.file);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            verify(target, bytes);
            try (var file = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                io.forceFile(file);
            }
            io.forceDirectory(root);
            return;
        }
        int scanned = 0, removed = 0;
        try (var entries = Files.list(root)) {
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                if (++scanned > 4096) {
                    throw new IOException("policy staging scan budget exhausted");
                }
                Path path = iterator.next();
                if (!path.getFileName().toString().matches(java.util.regex.Pattern.quote(kind.scratch) + "[a-f0-9-]{36}")) {
                    continue;
                }
                if (++removed > 128 || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("policy staging deletion budget exhausted or invalid");
                }
                Files.delete(path);
                io.forceDirectory(root);
            }
        }
        Path temporary = root.resolve(kind.scratch + UUID.randomUUID());
        try (var file = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            io.write(file, bytes);
            io.forceFile(file);
        }
        io.move(temporary, target);
        io.forceDirectory(root);
    }

    /**
     * Documents already in existence must be fully byte consistent and no damage, external configuration, links or special documents can be covered.
     *
     * @param target target
     * @param bytes bytes
     * @throws IOException if the operation cannot complete
     */
    private static void verify(final Path target, final byte[] bytes) throws IOException {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != bytes.length || !Arrays.equals(Files.readAllBytes(target), bytes)) {
            throw new IOException("existing policy differs from requested immutable record");
        }
    }

    /**
     * Only fixed protocol names and their private suspense prefixes are allowed, and no business path is accepted.
     */
    enum Kind {

        HEALTH("HEALTH_POLICY", ".health-"), SNAPSHOT("SNAPSHOT_POLICY", ".snapshot-policy-"), GC("GC_POLICY", ".gc-policy-"), AUTHENTICATED("AUTHENTICATED", ".authenticated-");

        final String file, scratch;

        /**
         * File name is the same controlled policy as half-written temporary storage.
         *
         * @param file file
         * @param scratch scratch
         */
        Kind(final String file, final String scratch) {
            this.file = file;
            this.scratch = scratch;
        }
    }

    /**
     * The I/O injection in the package allows real operations to report errors, and the official entry is done using a real file system only.
     */
    interface Io {

        /**
         * A partial write must never produce a usable final record.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        default void write(final FileChannel channel, final byte[] bytes) throws IOException {
            RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes));
        }

        /**
         * The new document uses the same durable confirmation as the existing correct document.
         *
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        default void forceFile(final FileChannel channel) throws IOException {
            channel.force(true);
        }

        /**
         * Directly fails when atomic moves are not supported and cannot be downgraded to partially overwrite.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        default void move(final Path source, final Path target) throws IOException {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        }

        /**
         * Recording names and temporary clean-up require directory durable confirmation.
         *
         * @param root root
         * @throws IOException if the operation cannot complete
         */
        default void forceDirectory(final Path root) throws IOException {
            RatisGenerationRecord.forceDirectory(root);
        }
    }
}
