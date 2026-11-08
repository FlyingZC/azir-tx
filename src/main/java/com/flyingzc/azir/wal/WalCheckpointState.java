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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Recovery complete checkpoint: Borders, snapshot anchor points, route and receipt archive directories have been published.
 *
 * @param engineId engine id
 * @param shards shards
 * @param storedShards stored shards
 * @param sequence sequence
 * @param safePoint oldest retained snapshot timestamp
 * @param boundary boundary
 * @param versions versions
 * @param archives archives
 * @param archiveBloom archive bloom
 * @param retiredThrough retired through
 */
record WalCheckpointState(UUID engineId, Set<String> shards, Set<String> storedShards, long sequence,
        long safePoint, WalReceipt boundary, List<Version> versions,
        List<WalReceiptArchive.Descriptor> archives, byte[] archiveBloom, long retiredThrough) {

    /**
     * Fixed snapshot content, all recovery parameters must be consistent with the same published boundary.
     *
     * @param engineId engine id
     * @param shards shards
     * @param storedShards stored shards
     * @param sequence sequence
     * @param safePoint oldest retained snapshot timestamp
     * @param boundary boundary
     * @param versions versions
     * @param archives archives
     * @param archiveBloom archive bloom
     * @param retiredThrough retired through
     * @throws IllegalArgumentException if the operation cannot complete
     */
    WalCheckpointState {
        shards = Set.copyOf(shards);
        storedShards = Set.copyOf(storedShards);
        versions = List.copyOf(versions);
        archives = List.copyOf(archives);
        archiveBloom = archiveBloom.clone();
        if (engineId == null || shards.isEmpty() || shards.size() > 1024 || !shards.containsAll(storedShards)
                || sequence < 1 || safePoint < 0 || safePoint > sequence || boundary == null
                || boundary.commitSequence() != sequence || !boundary.digest().matches("[a-f0-9]{64}") || retiredThrough < 0 || retiredThrough > sequence
                || archives.size() > 100_000 || versions.size() > 1_000_000
                || archiveBloom.length < 128 || archiveBloom.length > 16 * 1024 * 1024 || Integer.bitCount(archiveBloom.length) != 1) {
            throw new IllegalArgumentException("invalid checkpoint state");
        }
    }

    /**
     * Returns the filter replica, the incremental update on running cannot change the generated snapshot.
     *
     * @return the operation result
     */
    @Override
    public byte[] archiveBloom() {
        return archiveBloom.clone();
    }

    /**
     * Encoding contains only the recovery required state and does not write local write set active transaction to a check point.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        checkEncodingBudget(shards, storedShards, versions, archives.size(), archiveBloom.length);
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        // The new format specifies that the backend is no longer an recovery state of necessity, and the old application must be rejected without missing conflict fence to continue.
        out.writeInt(0x415A4353);
        out.writeInt(2);
        uuid(out, engineId);
        strings(out, shards);
        strings(out, storedShards);
        out.writeLong(sequence);
        out.writeLong(safePoint);
        uuid(out, boundary.transactionId());
        out.writeUTF(boundary.digest());
        out.writeLong(retiredThrough);
        out.writeInt(archiveBloom.length);
        out.write(archiveBloom);
        out.writeInt(archives.size());
        for (var archive : archives) {
            out.writeUTF(archive.fileName());
            out.writeLong(archive.count());
            out.writeLong(archive.minSequence());
            out.writeLong(archive.maxSequence());
            out.writeLong(archive.expiresAtMillis());
            out.writeUTF(archive.metadataHash());
        }
        out.writeInt(versions.size());
        for (var version : versions) {
            out.writeLong(version.sequence);
            out.writeUTF(version.mutation.shard());
            out.writeUTF(version.mutation.key());
            byte[] value = version.mutation.value();
            out.writeInt(value == null ? -1 : value.length);
            if (value != null) {
                out.write(value);
            }
        }
        out.flush();
        if (buffer.size() > 128 * 1024 * 1024) {
            throw new IOException("checkpoint image exceeds size budget");
        }
        return buffer.toByteArray();
    }

    /**
     * The full mirror is estimated before file creation and large buffer distribution, over budget is maintained with back pressure only and does not suspend a healthy run.
     *
     * @param shards shards
     * @param storedShards stored shards
     * @param versions versions
     * @param archiveCount archive count
     * @param bloomBytes bloom bytes
     * @return the operation result
     * @throws WalMvccException if the operation cannot complete
     */
    static long checkEncodingBudget(final Set<String> shards, final Set<String> storedShards, final List<Version> versions, final int archiveCount, final int bloomBytes) {
        long size = 24 + stringsSize(shards) + stringsSize(storedShards) + 32 + 66 + 8 + 4 + bloomBytes + 4 + 4 + 149L * archiveCount;
        for (var version : versions) {
            size += 8L + utfSize(version.mutation().shard()) + utfSize(version.mutation().key()) + 4 + version.mutation().valueLength();
            if (size > 128L * 1024 * 1024) {
                break;
            }
        }
        if (size > 128L * 1024 * 1024) {
            throw new WalMvccException(com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED, "checkpoint encoding budget exhausted", null, null);
        }
        return size;
    }

    /**
     * The code number of the routed group does not create a middle UTF byte array.
     *
     * @param values values
     * @return the operation result
     */
    private static long stringsSize(final Set<String> values) {
        return 4L + values.stream().mapToLong(WalCheckpointState::utfSize).sum();
    }

    /**
     * DataOutputStream.writeUTF uses Modified UTF-8, empty bytes and agents having to be billed in actual format.
     *
     * @param value value to write
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static long utfSize(final String value) {
        long bytes = 2;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            bytes += c >= 1 && c <= 127 ? 1 : c <= 2047 ? 2 : 3;
        }
        if (bytes > 65537) {
            throw new IllegalArgumentException("checkpoint UTF field too long");
        }
        return bytes;
    }

    /**
     * After the overall summary has been decoded, all lengths will have to be independently checked to avoid malformation resulting in unbounded distribution.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalCheckpointState decode(final byte[] bytes) throws IOException {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != 0x415A4353) {
                throw new IOException("unsupported checkpoint image");
            }
            int format = in.readInt();
            if (format != 1 && format != 2) {
                throw new IOException("unsupported checkpoint image");
            }
            UUID engine = uuid(in);
            Set<String> shards = strings(in);
            Set<String> stored = strings(in);
            long sequence = in.readLong();
            long safe = in.readLong();
            UUID id = uuid(in);
            String digest = in.readUTF();
            long retired = in.readLong();
            int length = in.readInt();
            if (length < 128 || length > 16 * 1024 * 1024 || Integer.bitCount(length) != 1 || length > in.available()) {
                throw new IOException("invalid checkpoint receipt filter");
            }
            byte[] bloom = in.readNBytes(length);
            int count = count(in, 100_000, 100);
            var archives = new ArrayList<WalReceiptArchive.Descriptor>();
            var names = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                var archive = new WalReceiptArchive.Descriptor(in.readUTF(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readUTF());
                if (!names.add(archive.fileName()) || archive.maxSequence() > sequence) {
                    throw new IOException("invalid receipt catalog");
                }
                archives.add(archive);
            }
            count = count(in, 1_000_000, 16);
            var versions = new ArrayList<Version>();
            var identities = new HashSet<String>();
            for (int i = 0; i < count; i++) {
                long version = in.readLong();
                String shard = in.readUTF();
                String key = in.readUTF();
                int size = in.readInt();
                if (version < 1 || version > sequence || !shards.contains(shard) || size < -1 || size > 8 * 1024 * 1024
                        || size > in.available() || !identities.add(shard + "\u0000" + key + "\u0000" + version)) {
                    throw new IOException("invalid checkpoint version");
                }
                versions.add(new Version(version, new WalMutation(shard, key, size == -1 ? null : in.readNBytes(size))));
            }
            if (in.available() != 0 || !digest.matches("[a-f0-9]{64}")) {
                throw new IOException("trailing or invalid checkpoint state");
            }
            return new WalCheckpointState(engine, shards, stored, sequence, safe, new WalReceipt(id, sequence, digest), versions, archives, bloom, retired);
        } catch (final IllegalArgumentException invalid) {
            throw new IOException("invalid checkpoint image", invalid);
        }
    }

    /**
     * Write routers to snapshot in a stable order.
     *
     * @param out out
     * @param values values
     * @throws IOException if the operation cannot complete
     */
    private static void strings(final DataOutputStream out, final Set<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values.stream().sorted().toList()) {
            out.writeUTF(value);
        }
    }

    /**
     * The route must be the only one that can be expressed by a normal change of object.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Set<String> strings(final DataInputStream in) throws IOException {
        int count = count(in, 1024, 2);
        var result = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            String shard = in.readUTF();
            new WalMutation(shard, "", null);
            if (shard.contains("\u0000") || !result.add(shard)) {
                throw new IOException("duplicate or invalid checkpoint route");
            }
        }
        return result;
    }

    /**
     * Validate records the lower boundary between the quantity and the remaining bytes.
     *
     * @param in in
     * @param maximum maximum
     * @param minimumBytes minimum bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static int count(final DataInputStream in, final int maximum, final int minimumBytes) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum || count > in.available() / minimumBytes) {
            throw new IOException("invalid checkpoint count");
        }
        return count;
    }

    /**
     * Writes the complete UUID.
     *
     * @param out out
     * @param id id
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    /**
     * Reads the full UUD.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * Saves a secure snapshot history with no backend shard; decoding to match the discarded materialization head cache in the old mirror.
     *
     * @param sequence sequence
     * @param mutation mutation
     */
    record Version(long sequence, WalMutation mutation) {
    }
}
