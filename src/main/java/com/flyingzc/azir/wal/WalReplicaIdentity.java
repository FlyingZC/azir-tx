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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Durable identity, shared engine and Raft group for copying directories, each replica uses different nodes identity.
 *
 * @param engineId engine id
 * @param groupId group id
 * @param nodeId node id
 */
public record WalReplicaIdentity(UUID engineId, UUID groupId, String nodeId) {

    /**
     * Node markings must be safe for deployment configuration and should not allow path or control characters.
     *
     * @param engineId engine id
     * @param groupId group id
     * @param nodeId node id
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalReplicaIdentity {
        java.util.Objects.requireNonNull(engineId);
        java.util.Objects.requireNonNull(groupId);
        if (nodeId == null || !nodeId.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("invalid replica node identity");
        }
    }

    /**
     * Fixed format binds three identity, CRC protects the recovery entrance.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x415A5250);
        out.writeInt(1);
        out.writeLong(engineId.getMostSignificantBits());
        out.writeLong(engineId.getLeastSignificantBits());
        out.writeLong(groupId.getMostSignificantBits());
        out.writeLong(groupId.getLeastSignificantBits());
        out.writeUTF(nodeId);
        out.flush();
        out.writeInt(WalFiles.crc(bytes.toByteArray()));
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * Identity cannot automatically guess another node without following a symbol link.
     *
     * @param root root
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalReplicaIdentity read(final Path root) throws IOException {
        Path path = root.resolve("REPLICA");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("replica identity missing or not regular");
        }
        return decode(WalFiles.readBounded(path, 256));
    }

    /**
     * The source replica identity in the backup list uses the same strict code and does not treat directory names as identity.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalReplicaIdentity decode(final byte[] bytes) throws IOException {
        if (bytes.length > 256) {
            throw new IOException("replica identity exceeds budget");
        }
        if (bytes.length < 46 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != WalFiles.crc(bytes, 0, bytes.length - 4)) {
            throw new IOException("replica identity checksum mismatch");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A5250 || in.readInt() != 1) {
                throw new IOException("unsupported replica identity");
            }
            var result = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            if (in.available() != 0) {
                throw new IOException("trailing replica identity");
            }
            return result;
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid replica identity", failure);
        }
    }

    /**
     * Release identity in a new directory without overwriteing the existing node identity.
     *
     * @param root root
     * @throws IOException if the operation cannot complete
     */
    void persist(final Path root) throws IOException {
        WalFiles.writeNew(root.resolve("REPLICA"), encode());
        WalFiles.forceDirectory(root);
    }

    /**
     * The database ownership fingerprint is independent of the user table key and does not leak deployment receipt.
     *
     * @return the operation result
     */
    String fingerprint() {
        return WalCodec.digest((engineId + "\u0000" + groupId + "\u0000" + nodeId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
