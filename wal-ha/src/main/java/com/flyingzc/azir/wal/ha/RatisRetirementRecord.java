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

import java.nio.ByteBuffer;

import java.util.zip.CRC32C;

/**
 * Retirement Progress logs keep ownership records and backend cursor and cannot lose the deletion that has already started before completion.
 *
 * @param target target
 * @param cursor cursor
 * @param backendDone backend done
 */
record RatisRetirementRecord(RatisGenerationRecord target, String cursor, boolean backendDone) {

    /**
     * Malformed cursor is not allowed to cause unbounded metadata distribution.
     *
     * @param target target
     * @param cursor cursor
     * @param backendDone backend done
     * @throws IllegalArgumentException if the operation cannot complete
     */
    RatisRetirementRecord {
        if (cursor == null || cursor.length() > 512) {
            throw new IllegalArgumentException("invalid retirement cursor");
        }
    }

    /**
     * The atomic release is synchronized with the directory and does not directly overwrite a log that is working on recovery.
     *
     * @param destination destination
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path destination) throws IOException {
        publish(destination, RatisRecordFile.REAL);
    }

    /**
     * Retirement progress and activity pointer share the real replacement protocol, each retaining its original format and size budget.
     *
     * @param destination destination
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    void publish(final Path destination, final RatisRecordFile.Io io) throws IOException {
        RatisRecordFile.publish(destination, encode(), ".retirement-", 2048, io);
    }

    /**
     * Encoding all progress and ownership fields without changing the old log format.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        byte[] owner = target.encode();
        out.writeInt(0x415A5252);
        out.writeInt(1);
        out.writeInt(owner.length);
        out.write(owner);
        out.writeUTF(cursor);
        out.writeBoolean(backendDone);
        out.flush();
        out.writeInt(crc(buffer.toByteArray()));
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * The failure of the CRC and its length check must be rejected and another generation cannot be re-selected to cover up the partial deletion.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisRetirementRecord read(final Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) < 100 || Files.size(path) > 2048) {
            throw new IOException("retirement record missing or exceeds budget");
        }
        byte[] bytes = Files.readAllBytes(path);
        if (ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(java.util.Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("retirement checksum mismatch");
        }
        var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
        if (in.readInt() != 0x415A5252 || in.readInt() != 1) {
            throw new IOException("unsupported retirement record");
        }
        int length = in.readInt();
        if (length < 80 || length > 512 || length > in.available()) {
            throw new IOException("retirement owner length invalid");
        }
        byte[] owner = in.readNBytes(length);
        var target = RatisGenerationRecord.decode(owner);
        var result = new RatisRetirementRecord(target, in.readUTF(), in.readBoolean());
        if (in.available() != 0) {
            throw new IOException("trailing retirement record");
        }
        return result;
    }

    /**
     * The entire progress record is participating in validate.
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
