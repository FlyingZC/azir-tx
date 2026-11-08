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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import com.flyingzc.azir.wal.WalReplicaIdentity;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Installation of bounded durable records shared with the active pointer, fixed to receive identity, generation and snapshot abstracts.
 *
 * @param identity identity
 * @param generation generation
 * @param term term
 * @param index index
 * @param sequence sequence
 * @param hash hash
 */
record RatisGenerationRecord(WalReplicaIdentity identity, UUID generation, long term, long index, long sequence, String hash) {

    /**
     * The original generation does not have an snapshot position, and the installation of generation must carry a complete file summary.
     *
     * @param identity identity
     * @param generation generation
     * @param term term
     * @param index index
     * @param sequence sequence
     * @param hash hash
     * @throws IllegalArgumentException if the operation cannot complete
     */
    RatisGenerationRecord {
        java.util.Objects.requireNonNull(identity);
        java.util.Objects.requireNonNull(generation);
        java.util.Objects.requireNonNull(hash);
        if (sequence < 0 || (term == -1 ? index != -1 || sequence != 0 || !hash.isEmpty() : term < 1 || index < 0 || !hash.matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("invalid generation record");
        }
    }

    /**
     * Sequencization includes receiving identity, which does not allow other nodes to reach the current vote identity.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        out.writeInt(0x415A474E);
        out.writeInt(1);
        uuid(out, identity.engineId());
        uuid(out, identity.groupId());
        out.writeUTF(identity.nodeId());
        uuid(out, generation);
        out.writeLong(term);
        out.writeLong(index);
        out.writeLong(sequence);
        out.writeUTF(hash);
        out.flush();
        out.writeInt(crc(buffer.toByteArray()));
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * The length and CRC check first, then resolve immutable identity; the damage cannot be returned to old generation for continued service.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisGenerationRecord read(final Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) < 80 || Files.size(path) > 512) {
            throw new IOException("generation record missing or exceeds budget");
        }
        return decode(Files.readAllBytes(path));
    }

    /**
     * Embedded title records are in the same format and are not parsed through interim documents.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisGenerationRecord decode(final byte[] bytes) throws IOException {
        if (bytes.length < 80 || bytes.length > 512) {
            throw new IOException("generation record length invalid");
        }
        if (ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(java.util.Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("generation checksum mismatch");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A474E || in.readInt() != 1) {
                throw new IOException("unsupported generation record");
            }
            var identity = new WalReplicaIdentity(uuid(in), uuid(in), in.readUTF());
            var record = new RatisGenerationRecord(identity, uuid(in), in.readLong(), in.readLong(), in.readLong(), in.readUTF());
            if (in.available() != 0) {
                throw new IOException("trailing generation data");
            }
            return record;
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid generation record", failure);
        }
    }

    /**
     * New records write private temporary and synchronized, atomic replaced and then sync directories.
     *
     * @param target target
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path target) throws IOException {
        publish(target, RatisRecordFile.REAL);
    }

    /**
     * Formally injects the same code as failure into a new record with which retry does not rollback is fully replaced.
     *
     * @param target target
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path target, final RatisRecordFile.Io io) throws IOException {
        RatisRecordFile.publish(target, encode(), ".record-", 512, io);
    }

    /**
     * Installs a record to delete as retry and syncs the parent directory again if the result of the record deletion is uncertain.
     *
     * @param target target
     * @throws IOException if the operation cannot complete
     */
    static void remove(final Path target) throws IOException {
        remove(target, RatisRecordFile.REAL);
    }

    /**
     * There are no longer any durable properties that must also be confirmed for deletion, and the synchronisation is not omitted through abnormal short circuits.
     *
     * @param target target
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void remove(final Path target, final RatisRecordFile.Io io) throws IOException {
        RatisRecordFile.remove(target, io);
    }

    /**
     * It is important to write that real progress must be made.
     *
     * @param channel channel
     * @param bytes bytes
     * @throws IOException if the operation cannot complete
     */
    static void write(final FileChannel channel, final ByteBuffer bytes) throws IOException {
        while (bytes.hasRemaining()) {
            if (channel.write(bytes) < 1) {
                throw new IOException("generation file write made no progress");
            }
        }
    }

    /**
     * An ordinary flush does not replace durable directory synchronization.
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
     * Fixed length UUID encoding.
     *
     * @param out out
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    /**
     * Fixed length UUID decoding.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * All pointer fields participate in standalone block validate.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
