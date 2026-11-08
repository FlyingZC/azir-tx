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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Back-up complete list of final release, fixed engine, commit boundary and size and summary of all files.
 *
 * @param backupId backup id
 * @param engineId engine id
 * @param boundary boundary
 * @param shards shards
 * @param storedShards stored shards
 * @param files files
 * @param data data
 * @param replica replica
 */
record WalBackupManifest(UUID backupId, UUID engineId, WalCheckpoint boundary, Set<String> shards,
        Set<String> storedShards, List<Entry> files, List<Data> data, WalReplicaIdentity replica) {
    static final int MAX_BYTES = 32 * 1024 * 1024;

    /**
     * Backup of the original single machine continues to output version, one, with no hidden changes in its disk format.
     *
     * @param backupId backup id
     * @param engineId engine id
     * @param boundary boundary
     * @param shards shards
     * @param storedShards stored shards
     * @param files files
     * @param data data
     */
    WalBackupManifest(final UUID backupId, final UUID engineId, final WalCheckpoint boundary, final Set<String> shards,
                      final Set<String> storedShards, final List<Entry> files, final List<Data> data) {
        this(backupId, engineId, boundary, shards, storedShards, files, data, null);
    }

    /**
     * Fixed list and refused to repeat path, repeat shard and incomplete data route.
     *
     * @param backupId backup id
     * @param engineId engine id
     * @param boundary boundary
     * @param shards shards
     * @param storedShards stored shards
     * @param files files
     * @param data data
     * @param replica replica
     * @throws IllegalArgumentException if the operation cannot complete
     */
    WalBackupManifest {
        new WalRestoreSpec(backupId, engineId, "manifest", boundary);
        if (replica != null && !replica.engineId().equals(engineId)) {
            throw new IllegalArgumentException("backup replica engine mismatch");
        }
        shards = Set.copyOf(shards);
        storedShards = Set.copyOf(storedShards);
        files = List.copyOf(files);
        data = List.copyOf(data);
        if (shards.isEmpty() || shards.size() > 1024 || !shards.containsAll(storedShards) || files.size() > 102_000) {
            throw new IllegalArgumentException("invalid backup routes");
        }
        for (String shard : shards) {
            new WalMutation(shard, "", null);
            if (shard.contains("\u0000")) {
                throw new IllegalArgumentException("invalid backup route");
            }
        }
        var paths = new HashSet<String>();
        for (var file : files) {
            if (!paths.add(file.path())) {
                throw new IllegalArgumentException("duplicate backup file");
            }
        }
        if (paths.contains("local/REPLICA") != (replica != null)) {
            throw new IllegalArgumentException("backup replica identity catalog mismatch");
        }
        var names = new HashSet<String>();
        var dataPaths = new HashSet<String>();
        for (var stream : data) {
            if (!names.add(stream.shard()) || !dataPaths.add(stream.file()) || !paths.contains(stream.file())) {
                throw new IllegalArgumentException("invalid backup data routes");
            }
        }
        if (!names.equals(storedShards)) {
            throw new IllegalArgumentException("missing backend snapshot");
        }
    }

    /**
     * Encode the full list and attach CRC, which must be synchronized with all cited documents before being published.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        out.writeInt(0x415A424D);
        out.writeInt(replica == null ? 1 : 2);
        uuid(out, backupId);
        uuid(out, engineId);
        if (replica != null) {
            byte[] identity = replica.encode();
            out.writeInt(identity.length);
            out.write(identity);
        }
        out.writeLong(boundary.sequence());
        out.writeUTF(boundary.digest());
        strings(out, shards);
        strings(out, storedShards);
        out.writeInt(files.size());
        for (var file : files) {
            out.writeUTF(file.path());
            out.writeLong(file.size());
            out.writeUTF(file.hash());
        }
        out.writeInt(data.size());
        for (var stream : data) {
            out.writeUTF(stream.shard());
            out.writeUTF(stream.file());
            out.writeLong(stream.rows());
        }
        out.flush();
        if (buffer.size() > MAX_BYTES - 4) {
            throw new IOException("backup manifest exceeds budget");
        }
        out.writeInt(WalFiles.crc(buffer.toByteArray()));
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * The overall length and CRC must be checked before decoded, and all counts must also be bytes remaining.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static WalBackupManifest decode(final byte[] bytes) throws IOException {
        if (bytes.length < 128 || bytes.length > MAX_BYTES || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != WalFiles.crc(bytes, 0, bytes.length - 4)) {
            throw new IOException("invalid backup manifest checksum");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A424D) {
                throw new IOException("unsupported backup manifest");
            }
            int version = in.readInt();
            if (version != 1 && version != 2) {
                throw new IOException("unsupported backup manifest");
            }
            UUID backup = uuid(in);
            UUID engine = uuid(in);
            WalReplicaIdentity replica = null;
            if (version == 2) {
                int length = count(in, 256, 1);
                replica = WalReplicaIdentity.decode(in.readNBytes(length));
            }
            var boundary = new WalCheckpoint(in.readLong(), in.readUTF());
            var shards = strings(in);
            var stored = strings(in);
            var files = new java.util.ArrayList<Entry>();
            int count = count(in, 102_000, 12);
            for (int i = 0; i < count; i++) {
                files.add(new Entry(in.readUTF(), in.readLong(), in.readUTF()));
            }
            count = count(in, 1024, 12);
            var data = new java.util.ArrayList<Data>();
            for (int i = 0; i < count; i++) {
                data.add(new Data(in.readUTF(), in.readUTF(), in.readLong()));
            }
            if (in.available() != 0) {
                throw new IOException("trailing backup manifest data");
            }
            return new WalBackupManifest(backup, engine, boundary, shards, stored, files, data, replica);
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid backup manifest", failure);
        }
    }

    /**
     * Encoded by a stable sequence.
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
     * Read only a limited number of routes and only one route.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Set<String> strings(final DataInputStream in) throws IOException {
        int count = count(in, 1024, 2);
        var result = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            if (!result.add(in.readUTF())) {
                throw new IOException("duplicate backup route");
            }
        }
        return result;
    }

    /**
     * Prevent deformities from triggering large-scale distribution.
     *
     * @param in in
     * @param maximum maximum
     * @param minimum minimum
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static int count(final DataInputStream in, final int maximum, final int minimum) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > maximum || count > in.available() / minimum) {
            throw new IOException("invalid backup count");
        }
        return count;
    }

    /**
     * Writes a fixed length UUID.
     *
     * @param out out
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    /**
     * Reads a fixed length UUID.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * The file path allows only the fixed format that this achieves, and cannot be entered into any target file through a backup list.
     *
     * @param path path
     * @param size size
     * @param hash hash
     */
    record Entry(String path, long size, String hash) {

        /**
         * The path must be a relative known file name and the size and summary must be clear.
         *
         * @param path path
         * @param size size
         * @param hash hash
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Entry {
            if (path == null || !path.matches("local/(ENGINE|REPLICA|CURRENT|transactions\\.wal|receipts-[a-f0-9-]{36}\\.sst|checkpoint-[a-f0-9-]{36}/(snapshot\\.bin|tail\\.wal))|data/[0-9]{4}\\.bin")
                    || size < 1 || hash == null || !hash.matches("[a-f0-9]{64}")) {
                throw new IllegalArgumentException("invalid backup file entry");
            }
        }
    }

    /**
     * A back-end data stream by which the path, file name and a closely checked total number of lines.
     *
     * @param shard logical shard identifier
     * @param file file
     * @param rows rows
     */
    record Data(String shard, String file, long rows) {

        /**
         * Shard identity cannot be replaced by a path, and the data stream can only refer to a fixed data directory.
         *
         * @param shard logical shard identifier
         * @param file file
         * @param rows rows
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Data {
            new WalMutation(shard, "", null);
            if (shard.contains("\u0000") || file == null || !file.matches("data/[0-9]{4}\\.bin") || rows < 0) {
                throw new IllegalArgumentException("invalid backup data catalog");
            }
        }
    }
}
