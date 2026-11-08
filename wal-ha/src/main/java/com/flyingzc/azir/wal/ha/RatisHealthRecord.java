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
 * Once the health strategy is activated, durable will be saved, operations and campaign eligibility will be restarted and physical detection will be completed.
 *
 * @param identity identity
 * @param options options
 */
record RatisHealthRecord(WalReplicaIdentity identity, RatisHealthOptions options) {

    /**
     * Records identity binding machine, with no database address, password or certificate.
     *
     * @param identity identity
     * @param options options
     */
    RatisHealthRecord {
        Objects.requireNonNull(identity);
        Objects.requireNonNull(options);
    }

    /**
     * Only the current node is allowed to have the same clear strategy, and isolation cannot be bypassed by the omission of start-up parameters.
     *
     * @param directory directory
     * @param identity identity
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisHealthRecord load(final Path directory, final WalReplicaIdentity identity) throws IOException {
        Path path = directory.resolve("HEALTH_POLICY");
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 512) {
            throw new IOException("health policy missing or exceeds budget");
        }
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length < 79 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("health policy checksum mismatch");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A4850 || in.readInt() != 1) {
                throw new IOException("unsupported health policy");
            }
            var owner = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var options = new RatisHealthOptions(in.readLong(), in.readInt(), in.readInt(), in.readLong(), in.readLong());
            if (!owner.equals(identity) || in.available() != 0) {
                throw new IOException("health policy owner mismatch");
            }
            return new RatisHealthRecord(owner, options);
        } catch (final IllegalArgumentException malformed) {
            throw new IOException("invalid health policy", malformed);
        }
    }

    /**
     * Durable is published within the directory lock only at node, with both the right policy and the re-confirmation of files and directories.
     *
     * @param directory directory
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path directory) throws IOException {
        publish(directory, RatisPolicyFile.REAL);
    }

    /**
     * Failure in the package shares the same coding and real release protocol with the official path.
     *
     * @param directory directory
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path directory, final RatisPolicyFile.Io io) throws IOException {
        var previous = load(directory, identity);
        if (previous != null && !previous.equals(this)) {
            throw new IOException("policy already configured differently");
        }
        RatisPolicyFile.publish(directory, RatisPolicyFile.Kind.HEALTH, encode(), io);
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
        out.writeInt(0x415A4850);
        out.writeInt(1);
        out.writeLong(identity.engineId().getMostSignificantBits());
        out.writeLong(identity.engineId().getLeastSignificantBits());
        out.writeLong(identity.groupId().getMostSignificantBits());
        out.writeLong(identity.groupId().getLeastSignificantBits());
        out.writeUTF(identity.nodeId());
        out.writeLong(options.intervalMillis());
        out.writeInt(options.failures());
        out.writeInt(options.recoveries());
        out.writeLong(options.probeMillis());
        out.writeLong(options.transferMillis());
        out.flush();
        out.writeInt(crc(bytes.toByteArray()));
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * All identity and policy participation validate.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var value = new CRC32C();
        value.update(bytes);
        return (int) value.getValue();
    }
}
