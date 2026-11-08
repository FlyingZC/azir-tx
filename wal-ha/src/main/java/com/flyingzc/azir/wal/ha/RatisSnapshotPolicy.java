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
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import com.flyingzc.azir.wal.WalReplicaIdentity;

import java.nio.ByteBuffer;

import java.util.zip.CRC32C;

/**
 * Automatic mirror policy binder identity, restart auto recovery, reject incomplete assembly when a health policy is missing.
 *
 * @param identity identity
 * @param options options
 */
record RatisSnapshotPolicy(WalReplicaIdentity identity, RatisSnapshotOptions options) {

    /**
     * Fixed configuration does not contain database receipt or a path.
     *
     * @param identity identity
     * @param options options
     */
    RatisSnapshotPolicy {
        Objects.requireNonNull(identity);
        Objects.requireNonNull(options);
    }

    /**
     * Read-only takes the bounded, is validate and belongs to this node.
     *
     * @param root root
     * @param owner owner
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisSnapshotPolicy load(final Path root, final WalReplicaIdentity owner) throws IOException {
        Path file = root.resolve("SNAPSHOT_POLICY");
        if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > 512) {
            throw new IOException("snapshot policy missing or exceeds budget");
        }
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < 87 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("snapshot policy checksum mismatch");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A5350 || in.readInt() != 1) {
                throw new IOException("unsupported snapshot policy");
            }
            var identity = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var options = new RatisSnapshotOptions(in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong());
            if (!identity.equals(owner) || in.available() != 0) {
                throw new IOException("snapshot policy owner mismatch");
            }
            return new RatisSnapshotPolicy(identity, options);
        } catch (final IllegalArgumentException malformed) {
            throw new IOException("invalid snapshot policy", malformed);
        }
    }

    /**
     * Durable is published within the directory lock only at node, with both the right policy and the re-confirmation of files and directories.
     *
     * @param root root
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path root) throws IOException {
        publish(root, RatisPolicyFile.REAL);
    }

    /**
     * Failure in the package shares the same coding and real release protocol with the official path.
     *
     * @param root root
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path root, final RatisPolicyFile.Io io) throws IOException {
        var previous = load(root, identity);
        if (previous != null && !previous.equals(this)) {
            throw new IOException("policy already configured differently");
        }
        RatisPolicyFile.publish(root, RatisPolicyFile.Kind.SNAPSHOT, encode(), io);
    }

    /**
     * Keeps the durable format in place and does not avoid the correct retry of the old policy by changing version.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x415A5350);
        out.writeInt(1);
        out.writeLong(identity.engineId().getMostSignificantBits());
        out.writeLong(identity.engineId().getLeastSignificantBits());
        out.writeLong(identity.groupId().getMostSignificantBits());
        out.writeLong(identity.groupId().getLeastSignificantBits());
        out.writeUTF(identity.nodeId());
        out.writeLong(options.intervalMillis());
        out.writeLong(options.entryGap());
        out.writeLong(options.maxIntervalMillis());
        out.writeLong(options.logHighWaterBytes());
        out.writeLong(options.preserveEntries());
        out.flush();
        out.writeInt(crc(bytes.toByteArray()));
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * All identity and configuration participation validate.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var checksum = new CRC32C();
        checksum.update(bytes);
        return (int) checksum.getValue();
    }
}
