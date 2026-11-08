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
import java.util.zip.CRC32C;

/**
 * Checkpoint file bounded read-write is synchronized with directory, and does not override the release protocol by non- atomic.
 */
final class WalFiles {

    /**
     * Prohibits case-based document protocol tools.
     */
    private WalFiles() {
    }

    /**
     * The new document must be fully written and synchronized, which is denied when the same name exists.
     *
     * @param path path
     * @param data data
     * @throws IOException if the operation cannot complete
     */
    static void writeNew(final Path path, final byte[] data) throws IOException {
        writeNew(path, data, WalLog.NORMAL_IO);
    }

    /**
     * The default entry does not add any additional file behavior to the actual writing and brushing position of failure.
     *
     * @param path path
     * @param data data
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void writeNew(final Path path, final byte[] data, final WalLog.IoHook io) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            String name = path.getFileName().toString();
            write(channel, ByteBuffer.wrap(data), name, io);
            io.beforeCheckpointForce(name);
            channel.force(true);
            io.afterCheckpointForce(name);
        }
    }

    /**
     * Write a complete buffer section, and the lack of progress on disk is considered an error.
     *
     * @param channel channel
     * @param data data
     * @throws IOException if the operation cannot complete
     */
    static void write(final FileChannel channel, final ByteBuffer data) throws IOException {
        while (data.hasRemaining()) {
            if (channel.write(data) <= 0) {
                throw new IOException("file write made no progress");
            }
        }
    }

    /**
     * Retain real offsets and segmenting of single checkpoint files, which cannot be replaced by after-action files.
     *
     * @param channel channel
     * @param data data
     * @param name name
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void write(final FileChannel channel, final ByteBuffer data, final String name, final WalLog.IoHook io) throws IOException {
        while (data.hasRemaining()) {
            int permitted = io.checkpointWriteLimit(name, channel.position(), data.remaining());
            if (permitted < 1 || permitted > data.remaining()) {
                throw new IOException("invalid checkpoint write limit");
            }
            int limit = data.limit();
            data.limit(data.position() + permitted);
            try {
                if (channel.write(data) <= 0) {
                    throw new IOException("checkpoint write made no progress");
                }
            } finally {
                data.limit(limit);
            }
            io.afterCheckpointWrite(name, channel.position());
        }
    }

    /**
     * Reads the complete block from the specified offset, and the cut-off cannot be disguised as a valid empty data.
     *
     * @param channel channel
     * @param offset offset
     * @param length length
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static byte[] read(final FileChannel channel, final long offset, final int length) throws IOException {
        var data = ByteBuffer.allocate(length);
        while (data.hasRemaining()) {
            if (channel.read(data, offset + data.position()) <= 0) {
                throw new IOException("truncated checkpoint file");
            }
        }
        return data.array();
    }

    /**
     * Checks the file size before distribution to prevent an abnormal file from causing unbounded memory application.
     *
     * @param path path
     * @param maximum maximum
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static byte[] readBounded(final Path path, final long maximum) throws IOException {
        long size = Files.size(path);
        if (size < 1 || size > maximum) {
            throw new IOException("checkpoint file size exceeds budget");
        }
        return Files.readAllBytes(path);
    }

    /**
     * Synchronizes the directory entries, and only after atomic has changed its name to synchronise with the directory is the deletion of the old generation allowed.
     *
     * @param directory directory
     * @throws IOException if the operation cannot complete
     */
    static void forceDirectory(final Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    /**
     * Calculates a separate data block validate, protects index keys and payload, not only the return value.
     *
     * @param data data
     * @return the operation result
     */
    static int crc(final byte[] data) {
        return crc(data, 0, data.length);
    }

    /**
     * Calculates validate for the specified range, frame validate field does not participate in the calculation itself.
     *
     * @param data data
     * @param offset offset
     * @param length length
     * @return the operation result
     */
    static int crc(final byte[] data, final int offset, final int length) {
        var crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }
}
