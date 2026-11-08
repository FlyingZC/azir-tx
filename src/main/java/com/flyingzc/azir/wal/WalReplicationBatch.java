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
import java.util.List;
import java.util.UUID;

/**
 * Bounded complete batch for the Consensus Layer, with terms and sources independent of business commit digests without changing history UUID decision.
 *
 * @param epoch epoch
 * @param origin origin
 * @param commits commits
 */
public record WalReplicationBatch(long epoch, WalReplicaIdentity origin, List<WalCommit> commits) {
    public static final int MAX_BYTES = 16 * 1024 * 1024;

    /**
     * Rejects empty batch and illegal tenure, keeping immutable business records.
     *
     * @param epoch epoch
     * @param origin origin
     * @param commits commits
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalReplicationBatch {
        java.util.Objects.requireNonNull(origin);
        commits = List.copyOf(commits);
        if (epoch < 1 || commits.isEmpty() || commits.size() > 1024) {
            throw new IllegalArgumentException("invalid replication batch");
        }
    }

    /**
     * First check the records and total length, then distribute consensus messages buffer.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public byte[] encode() throws IOException {
        var encoded = commits.stream().map(WalCodec::encode).toList();
        long length = 64 + origin.nodeId().length() + encoded.stream().mapToLong(bytes -> 4L + bytes.length).sum();
        if (length > MAX_BYTES) {
            throw new IOException("replication batch exceeds byte limit");
        }
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x415A5242);
        out.writeInt(1);
        out.writeLong(epoch);
        out.writeLong(origin.engineId().getMostSignificantBits());
        out.writeLong(origin.engineId().getLeastSignificantBits());
        out.writeLong(origin.groupId().getMostSignificantBits());
        out.writeLong(origin.groupId().getLeastSignificantBits());
        out.writeUTF(origin.nodeId());
        out.writeInt(encoded.size());
        for (byte[] data : encoded) {
            out.writeInt(data.length);
            out.write(data);
        }
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * Checks the total size and length of each item before creating a value object. The log count provided by the network cannot be trusted.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static WalReplicationBatch decode(final byte[] bytes) throws IOException {
        if (bytes.length < 60 || bytes.length > MAX_BYTES) {
            throw new IOException("replication message size invalid");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 0x415A5242 || in.readInt() != 1) {
                throw new IOException("unsupported replication batch");
            }
            long epoch = in.readLong();
            UUID engine = new UUID(in.readLong(), in.readLong());
            UUID group = new UUID(in.readLong(), in.readLong());
            String node = in.readUTF();
            int count = in.readInt();
            if (count < 1 || count > 1024 || count > in.available() / 40) {
                throw new IOException("replication record count invalid");
            }
            var commits = new java.util.ArrayList<WalCommit>();
            for (int i = 0; i < count; i++) {
                int length = in.readInt();
                if (length < 36 || length > in.available()) {
                    throw new IOException("replication record truncated");
                }
                commits.add(WalCodec.decode(in.readNBytes(length)));
            }
            if (in.available() != 0) {
                throw new IOException("trailing replication data");
            }
            return new WalReplicationBatch(epoch, new WalReplicaIdentity(engine, group, node), commits);
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid replication batch", failure);
        }
    }

    /**
     * The proposal matches a full encoded summary and cannot be replaced by a business UUID or probability filter.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public String digest() throws IOException {
        return WalCodec.digest(encode());
    }
}
