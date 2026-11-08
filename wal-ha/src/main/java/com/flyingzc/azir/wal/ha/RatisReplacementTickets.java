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
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.io.IOException;
import java.nio.channels.FileChannel;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The operator 's document is uncovered to save, the unknown release result retry is reconfirmed and the file does not contain the backend receipt.
 */
public final class RatisReplacementTickets {

    private static final Set<Path> OWNED = ConcurrentHashMap.newKeySet();

    private static final int MAXIMUM = 16_384;

    private static final Io REAL = new Io() {
    };

    /**
     * Tools cannot be exemplified.
     */
    private RatisReplacementTickets() {
    }

    /**
     * Changes in membership can only start after successful storage; double saving of the same instrument is resynchronized.
     *
     * @param file file
     * @param ticket ticket
     * @throws IOException if the operation cannot complete
     */
    public static void save(final Path file, final RatisMemberReplacement ticket) throws IOException {
        save(file, ticket, REAL);
    }

    /**
     * Failure in the package injects real file system actions that are shared with production preservation.
     *
     * @param file file
     * @param ticket ticket
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void save(final Path file, final RatisMemberReplacement ticket, final Io io) throws IOException {
        Objects.requireNonNull(ticket);
        byte[] bytes = ticket.encode();
        Path target = target(file);
        locked(target, () -> {
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                read(target, ticket.groupId());
                RatisRecordFile.confirm(target, bytes, io);
                clean(target, io);
                return null;
            }
            clean(target, io);
            Path temporary = target.resolveSibling(prefix(target) + UUID.randomUUID());
            try (var channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                io.write(channel, bytes);
                io.forceFile(channel);
            }
            io.link(temporary, target);
            io.forceDirectory(target.getParent());
            io.delete(temporary);
            io.forceDirectory(target.getParent());
            return null;
        });
    }

    /**
     * Previously validate group identity and confirmed files and directories cannot be used to execute management changes on the basis of visible but not yet confirmed names.
     *
     * @param file file
     * @param expectedGroup expected group
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisMemberReplacement load(final Path file, final UUID expectedGroup) throws IOException {
        Path target = target(file);
        return locked(target, () -> {
            var ticket = read(target, expectedGroup);
            RatisRecordFile.confirm(target, ticket.encode(), REAL);
            return ticket;
        });
    }

    /**
     * Only bounded normal files are accepted and links, damage and external groups do not trigger new operations.
     *
     * @param file file
     * @param group group
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static RatisMemberReplacement read(final Path file, final UUID group) throws IOException {
        Objects.requireNonNull(group);
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > MAXIMUM) {
            throw new IOException("replacement ticket missing or exceeds budget");
        }
        var ticket = RatisMemberReplacement.decode(Files.readAllBytes(file));
        if (!ticket.groupId().equals(group)) {
            throw new IOException("replacement ticket belongs to another group");
        }
        return ticket;
    }

    /**
     * Clear parent directories must exist and do not automatically create or take over any business path.
     *
     * @param file file
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Path target(final Path file) throws IOException {
        Path absolute = Objects.requireNonNull(file).toAbsolutePath().normalize();
        if (absolute.getParent() == null) {
            throw new IOException("explicit ticket file required");
        }
        Path parent = absolute.getParent().toRealPath();
        return parent.resolve(absolute.getFileName());
    }

    /**
     * There is only one co-author with the same directory, locking the file and not deleting it to avoid replacing the inode that creates a breaklock.
     *
     * @param <T> value type
     * @param target target
     * @param body body
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static <T> T locked(final Path target, final Operation<T> body) throws IOException {
        Path lockFile = target.resolveSibling(".azir-ticket-lock-" + key(target));
        if (!OWNED.add(target)) {
            throw new IOException("replacement ticket already in use");
        }
        try (var channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            try (var lock = channel.tryLock()) {
                if (lock == null) {
                    throw new IOException("replacement ticket owned by another process");
                }
                return body.run();
            } catch (final java.nio.channels.OverlappingFileLockException busy) {
                throw new IOException("replacement ticket lock busy", busy);
            }
        } finally {
            OWNED.remove(target);
        }
    }

    /**
     * Clears only private semi-written files of current instrument names, with fixed scans and deleted budget.
     *
     * @param target target
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    private static void clean(final Path target, final Io io) throws IOException {
        int scanned = 0, removed = 0;
        String prefix = prefix(target);
        try (var entries = Files.list(target.getParent())) {
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                if (++scanned > 4096) {
                    throw new IOException("ticket staging scan budget exhausted");
                }
                Path file = iterator.next();
                if (!file.getFileName().toString().matches(java.util.regex.Pattern.quote(prefix) + "[a-f0-9-]{36}")) {
                    continue;
                }
                if (++removed > 128 || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("ticket staging invalid or exceeds budget");
                }
                io.delete(file);
                io.forceDirectory(target.getParent());
            }
        }
    }

    /**
     * User filenames are not used directly as private temporary storage mode.
     *
     * @param target target
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static String prefix(final Path target) throws IOException {
        return ".azir-ticket-stage-" + key(target) + "-";
    }

    /**
     * Regulates that the name summary is used only for locking and temporary saving ownership, not as authentication receipt.
     *
     * @param target target
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static String key(final Path target) throws IOException {
        try {
            return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(target.getFileName().toString().getBytes(StandardCharsets.UTF_8)));
        } catch (final java.security.NoSuchAlgorithmException unavailable) {
            throw new IOException(unavailable);
        }
    }

    /**
     * A hard-link atomic creates only an official name, and no single file can be replaced.
     */
    interface Io extends RatisRecordFile.Io {

        /**
         * The temporary saving is in the same directory as the official document, and the atomic link fails when it does not support it.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        default void link(final Path source, final Path target) throws IOException {
            Files.createLink(target, source);
        }
    }

    /**
     * Limited operations protected by the same document lock.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    private interface Operation<T> {

        /**
         * Unusual transmission cannot replace partial preservation with success.
         *
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        T run() throws IOException;
    }
}
