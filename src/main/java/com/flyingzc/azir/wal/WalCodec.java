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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Fixed binary log code for version, all lengths before the assigned memory validate.
 */
final class WalCodec {

    static final int MAX_FRAME = 32 * 1024 * 1024;

    /**
     * Prohibits the case log coding tool.
     */
    private WalCodec() {
    }

    /**
     * Encoding complete transaction, and the field order also defines the content summary.
     *
     * @param commit commit
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static byte[] encode(final WalCommit commit) {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.writeLong(commit.sequence());
            out.writeLong(commit.transactionId().getMostSignificantBits());
            out.writeLong(commit.transactionId().getLeastSignificantBits());
            out.writeLong(commit.startSequence());
            out.writeInt(commit.mutations().size());
            for (WalMutation mutation : commit.mutations()) {
                writeBytes(out, mutation.shard().getBytes(StandardCharsets.UTF_8));
                writeBytes(out, mutation.key().getBytes(StandardCharsets.UTF_8));
                byte[] value = mutation.value();
                out.writeInt(value == null ? -1 : value.length);
                if (value != null) {
                    out.write(value);
                }
            }
            out.flush();
            return bytes.toByteArray();
        } catch (final IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * Decoding and rejecting malformations fields, repetition keys and tailing data.
     *
     * @param data data
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalCommit decode(final byte[] data) throws IOException {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(data));
            long sequence = in.readLong();
            UUID id = new UUID(in.readLong(), in.readLong());
            long start = in.readLong();
            int count = in.readInt();
            if (count < 1 || count > 100_000 || count > data.length / 12) {
                throw new IOException("invalid mutation count");
            }
            var writes = new ArrayList<WalMutation>();
            var keys = new java.util.HashSet<WalMvccRuntime.Key>();
            for (int i = 0; i < count; i++) {
                String shard = text(readBytes(in, 128));
                String key = text(readBytes(in, 512));
                int size = in.readInt();
                if (size < -1 || size > MAX_FRAME || size > in.available()) {
                    throw new IOException("invalid value size");
                }
                byte[] value = size == -1 ? null : in.readNBytes(size);
                if (!keys.add(new WalMvccRuntime.Key(shard, key))) {
                    throw new IOException("duplicate key");
                }
                writes.add(new WalMutation(shard, key, value));
            }
            if (in.available() != 0) {
                throw new IOException("trailing transaction data");
            }
            return new WalCommit(id, sequence, start, writes);
        } catch (final IllegalArgumentException ex) {
            throw new IOException("invalid transaction", ex);
        }
    }

    /**
     * Writes fields with long prefixes.
     *
     * @param out out
     * @param bytes bytes
     * @throws IOException if the operation cannot complete
     */
    private static void writeBytes(final DataOutputStream out, final byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * Reads a limited field and rejects the cut-off and abnormal length.
     *
     * @param in in
     * @param max max
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static byte[] readBytes(final DataInputStream in, final int max) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > max || size > in.available()) {
            throw new IOException("invalid field size");
        }
        return in.readNBytes(size);
    }

    /**
     * Strictly validate UTF-8, refusal to replace the character results in a change of key identity.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static String text(final byte[] bytes) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (final java.nio.charset.CharacterCodingException ex) {
            throw new IOException("invalid UTF-8", ex);
        }
    }

    /**
     * Calculates the content summary for reconciliation of durable log with the prefix to database.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static String digest(final byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (final NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
