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

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Flow format for backend snapshot, strict key order and end count to prevent cut-off, repetition and unbounded distribution.
 */
final class WalBackupData {

    private static final int MAGIC = 0x415A4244;

    private static final int ROW = 0x415A4252;

    private static final int END = 0x415A4245;

    /**
     * Formatting tools do not allow for exemplification.
     */
    private WalBackupData() {
    }

    /**
     * Verify returns the number of lines after full flow, and the consumer of recovery should remain in recovery status until final validate.
     *
     * @param input input
     * @param expected expected
     * @param sink sink
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static long read(final InputStream input, final Header expected, final Sink sink) throws IOException {
        var data = new DataInputStream(input);
        try {
            if (data.readInt() != MAGIC || data.readInt() != 1) {
                throw new IOException("unsupported backup data");
            }
            var actual = new Header(new UUID(data.readLong(), data.readLong()), data.readUTF(), new WalCheckpoint(data.readLong(), data.readUTF()));
            if (!expected.equals(actual)) {
                throw new IOException("backup data identity mismatch");
            }
            String previous = null;
            long rows = 0;
            while (true) {
                int kind = data.readInt();
                if (kind == END) {
                    if (data.readLong() != rows || data.read() != -1) {
                        throw new IOException("invalid backup footer");
                    }
                    return rows;
                }
                if (kind != ROW) {
                    throw new IOException("invalid backup row marker");
                }
                String key = data.readUTF();
                long sequence = data.readLong();
                int size = data.readInt();
                if (size < -1 || size > 8 * 1024 * 1024 || sequence < 1 || sequence > expected.boundary().sequence()
                        || previous != null && Utf8KeyOrder.compare(previous, key) >= 0) {
                    throw new IOException("invalid backup row");
                }
                byte[] value = size < 0 ? null : data.readNBytes(size);
                if (value != null && value.length != size) {
                    throw new IOException("truncated backup value");
                }
                sink.accept(new WalBackupRow(key, new WalValue(sequence, value)));
                previous = key;
                rows++;
            }
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid backup data", failure);
        }
    }

    /**
     * Turning the version backend to a disk or a summaryer does not require that the entire database be loaded into memory.
     */
    @FunctionalInterface
    interface Sink {

        /**
         * Consuming an verify latest version, the anomaly must terminate backup or recovery.
         *
         * @param row row
         * @throws IOException if the operation cannot complete
         */
        void accept(WalBackupRow row) throws IOException;
    }

    /**
     * Saves a definitive head, backup ID does not participate in the data summary and allows back-end independent recalculation.
     *
     * @param engineId engine id
     * @param shard logical shard identifier
     * @param boundary boundary
     */
    record Header(UUID engineId, String shard, WalCheckpoint boundary) {

        /**
         * Verify Head engine, route and commit boundary.
         *
         * @param engineId engine id
         * @param shard logical shard identifier
         * @param boundary boundary
         */
        Header {
            new WalRestoreSpec(new UUID(0, 0), engineId, shard, boundary);
        }
    }

    /**
     * A single bounded line encoder, the end mark is allowed only once.
     */
    static final class Writer {

        private final DataOutputStream output;

        private final Header header;

        private String previous;

        private long rows;

        private boolean finished;

        /**
         * Write a fixed head and then receive the data in a back-end binary.
         *
         * @param output output
         * @param header header
         * @throws IOException if the operation cannot complete
         */
        Writer(final OutputStream output, final Header header) throws IOException {
            this.output = new DataOutputStream(output);
            this.header = header;
            this.output.writeInt(MAGIC);
            this.output.writeInt(1);
            this.output.writeLong(header.engineId().getMostSignificantBits());
            this.output.writeLong(header.engineId().getLeastSignificantBits());
            this.output.writeUTF(header.shard());
            this.output.writeLong(header.boundary().sequence());
            this.output.writeUTF(header.boundary().digest());
        }

        /**
         * Checks the strict increment key and version boundary and writes a full line of data.
         *
         * @param row row
         * @throws IOException if the operation cannot complete
         */
        void accept(final WalBackupRow row) throws IOException {
            if (finished || row.version().commitSequence() > header.boundary().sequence()
                    || previous != null && Utf8KeyOrder.compare(previous, row.key()) >= 0) {
                throw new IOException("invalid backup row order or boundary");
            }
            byte[] value = row.version().value();
            output.writeInt(ROW);
            output.writeUTF(row.key());
            output.writeLong(row.version().commitSequence());
            output.writeInt(value == null ? -1 : value.length);
            if (value != null) {
                output.write(value);
            }
            previous = row.key();
            rows++;
        }

        /**
         * Clearly write the final line number and refresh the flow. The file without the end mark is not a complete backup.
         *
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        long finish() throws IOException {
            if (finished) {
                throw new IOException("backup stream already finished");
            }
            output.writeInt(END);
            output.writeLong(rows);
            output.flush();
            finished = true;
            return rows;
        }
    }
}
