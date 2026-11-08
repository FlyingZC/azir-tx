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
import com.flyingzc.azir.wal.WalReplicaIdentity;

import java.util.UUID;

/**
 * The new majority reading fence without a change in business status, each request has an independent nonce and cannot be rehearsed with old heartbeat evidence.
 *
 * @param epoch epoch
 * @param origin origin
 * @param nonce nonce
 */
record RatisReadFence(long epoch, WalReplicaIdentity origin, long nonce) {
    private static final int MAGIC = 0x415A5246;

    /**
     * Refusal of unlawful tenure and request number.
     *
     * @param epoch epoch
     * @param origin origin
     * @param nonce nonce
     * @throws IllegalArgumentException if the operation cannot complete
     */
    RatisReadFence {
        if (epoch < 1 || nonce < 1 || origin == null) {
            throw new IllegalArgumentException("invalid read fence");
        }
    }

    /**
     * Fixed message type different from business batch.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(MAGIC);
        out.writeInt(1);
        out.writeLong(epoch);
        out.writeLong(origin.engineId().getMostSignificantBits());
        out.writeLong(origin.engineId().getLeastSignificantBits());
        out.writeLong(origin.groupId().getMostSignificantBits());
        out.writeLong(origin.groupId().getLeastSignificantBits());
        out.writeUTF(origin.nodeId());
        out.writeLong(nonce);
        out.flush();
        return bytes.toByteArray();
    }

    /**
     * Type detection read-only takes a fixed header and does not decode operations.
     *
     * @param bytes bytes
     * @return the operation result
     */
    static boolean matches(final byte[] bytes) {
        return bytes.length >= 4 && java.nio.ByteBuffer.wrap(bytes).getInt() == MAGIC;
    }

    /**
     * Strict validate length, identity and tail.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisReadFence decode(final byte[] bytes) throws IOException {
        if (bytes.length > 256) {
            throw new IOException("read fence too large");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != MAGIC || in.readInt() != 1) {
                throw new IOException("invalid read fence format");
            }
            long term = in.readLong();
            var identity = new WalReplicaIdentity(new UUID(in.readLong(), in.readLong()), new UUID(in.readLong(), in.readLong()), in.readUTF());
            var result = new RatisReadFence(term, identity, in.readLong());
            if (in.available() != 0) {
                throw new IOException("trailing read fence");
            }
            return result;
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid read fence", failure);
        }
    }
}
