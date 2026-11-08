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

import java.util.UUID;

/**
 * Specifies that replica will not return any operational data for an internal health response after the applied location.
 *
 * @param group group
 * @param node node
 * @param appliedIndex applied index
 * @param publishedSequence published sequence
 * @param healthy healthy
 */
record RatisReplicaStatus(UUID group, String node, long appliedIndex, long publishedSequence, boolean healthy) {
    private static final int MAGIC = 0x415A5254;

    /**
     * Requests for identification are separated from business messages.
     *
     * @return the operation result
     */
    static byte[] request() {
        return java.nio.ByteBuffer.allocate(8).putInt(MAGIC).putInt(1).array();
    }

    /**
     * Only an accurate internal request can enter a status query.
     *
     * @param bytes bytes
     * @return the operation result
     */
    static boolean matches(final byte[] bytes) {
        return java.util.Arrays.equals(bytes, request());
    }

    /**
     * Respond to fixed identity, watermark and health markers, field length bounded.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        out.writeInt(MAGIC);
        out.writeInt(1);
        out.writeLong(group.getMostSignificantBits());
        out.writeLong(group.getLeastSignificantBits());
        out.writeUTF(node);
        out.writeLong(appliedIndex);
        out.writeLong(publishedSequence);
        out.writeBoolean(healthy);
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * Error node, damaged field or unknown version cannot be interpreted as health.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisReplicaStatus decode(final byte[] bytes) throws IOException {
        if (bytes.length < 43 || bytes.length > 256) {
            throw new IOException("replica status exceeds budget");
        }
        var in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != MAGIC || in.readInt() != 1) {
            throw new IOException("unsupported replica status");
        }
        var result = new RatisReplicaStatus(new UUID(in.readLong(), in.readLong()), in.readUTF(), in.readLong(), in.readLong(), in.readBoolean());
        if (!result.node().matches("[A-Za-z0-9._-]{1,64}") || result.appliedIndex() < -1 || result.publishedSequence() < 0 || in.available() != 0) {
            throw new IOException("invalid replica status");
        }
        return result;
    }
}
