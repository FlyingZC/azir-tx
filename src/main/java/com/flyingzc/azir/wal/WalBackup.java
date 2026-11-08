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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Full backup validate with offline recovery; only new targets are operated, RESTORING tags prevent part of the recovery service.
 */
public final class WalBackup {

    private static final Set<Path> LOCAL_RESTORES = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private static final ForceHook NORMAL_FORCE = (path, channel) -> channel.force(true);

    /**
     * Backup tool does not allow for exemplification.
     */
    private WalBackup() {
    }

    /**
     * Check the backup integrity without changing the backup, running-time directory or database.
     *
     * @param directory directory
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static Info inspect(final Path directory, final WalBackupOptions options) throws IOException {
        return validate(directory, new Budget(options)).info();
    }

    /**
     * From verify backup recovery to an entirely new directory and exclusive database, the same backup break recovery allows safe continuation.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static Info restore(final Path backup, final Path target, final Map<String, WalMaterializedStore> stores,
                               final WalOptions runtimeOptions, final WalBackupOptions options) throws IOException {
        return restoreForTest(backup, target, stores, runtimeOptions, options, stage -> {
        });
    }

    /**
     * Full copy mirror only recovery to full new replica space, keeping business identity but using the receiving node 's own identity.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @param identity identity
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static Info restoreReplica(final Path backup, final Path target, final Map<String, WalMaterializedStore> stores,
                                      final WalOptions runtimeOptions, final WalBackupOptions options, final WalReplicaIdentity identity) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, stage -> {
        }, NORMAL_FORCE, java.util.Objects.requireNonNull(identity));
    }

    /**
     * Verify replica recovery in the package writes, brushes and releases boundaries without exposing production failure into interface.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @param identity identity
     * @param hook hook
     * @param force force
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Info restoreReplicaForTest(final Path backup, final Path target, final Map<String, WalMaterializedStore> stores,
                                      final WalOptions runtimeOptions, final WalBackupOptions options, final WalReplicaIdentity identity,
                                      final Consumer<WalMvccRuntime.Stage> hook, final ForceHook force) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, hook, force, java.util.Objects.requireNonNull(identity));
    }

    /**
     * Injection in the package is used only for recovery failure acceptances after actual writing and does not expose normal application processes.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Info restoreForTest(final Path backup, final Path target, final Map<String, WalMaterializedStore> stores,
                               final WalOptions runtimeOptions, final WalBackupOptions options, final Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        return restoreForTest(backup, target, stores, runtimeOptions, options, hook, NORMAL_FORCE);
    }

    /**
     * Injecting a real file brushes the boundary, verify must also resynchronize the complete document that exists at the time of renewal.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @param hook hook
     * @param force force
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Info restoreForTest(final Path backup, final Path target, final Map<String, WalMaterializedStore> stores,
                               final WalOptions runtimeOptions, final WalBackupOptions options, final Consumer<WalMvccRuntime.Stage> hook, final ForceHook force) throws IOException {
        return restoreInternal(backup, target, stores, runtimeOptions, options, hook, force, null);
    }

    /**
     * The same recovery protocol handles two distinct modes, and the normal entry cannot import a duplicate mirror or re-link already stored.
     *
     * @param backup backup
     * @param target target
     * @param stores stores
     * @param runtimeOptions runtime options
     * @param options options
     * @param hook hook
     * @param force force
     * @param replica replica
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Info restoreInternal(final Path backup, final Path target, Map<String, WalMaterializedStore> stores,
                                        final WalOptions runtimeOptions, final WalBackupOptions options, final Consumer<WalMvccRuntime.Stage> hook,
                                        final ForceHook force, final WalReplicaIdentity replica) throws IOException {
        var budget = new Budget(options);
        var validated = validate(backup, budget);
        var manifest = validated.manifest();
        if (replica == null ? manifest.replica() != null
                : manifest.replica() == null
                        || !replica.engineId().equals(manifest.engineId()) || !replica.groupId().equals(manifest.replica().groupId())) {
            throw new IOException("restore mode, replica engine or group mismatch");
        }
        stores = Map.copyOf(stores);
        if (!stores.keySet().equals(manifest.storedShards()) || stores.values().stream().anyMatch(store -> !store.supportsBackup()
                || replica != null && !store.supportsReplicaRestore())) {
            throw new IOException("restore backend routes or capabilities mismatch");
        }
        validateMemory(validated.image(), runtimeOptions);
        Path destination = canonicalTarget(target);
        if (destination.startsWith(validated.root()) || validated.root().startsWith(destination)) {
            throw new IOException("restore target overlaps backup");
        }
        String suffix = WalCodec.digest(destination.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)).substring(0, 24);
        String replicaSuffix = replica == null ? "" : "\u0000" + replica.fingerprint();
        byte[] operation = HexFormat.of().parseHex(WalCodec.digest((validated.manifestHash() + "\u0000" + destination + replicaSuffix)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        UUID operationId = new UUID(ByteBuffer.wrap(operation).getLong(), ByteBuffer.wrap(operation).getLong(8));
        Path lockPath = destination.getParent().resolve(".azir-restore-" + suffix + ".lock");
        if (!LOCAL_RESTORES.add(lockPath)) {
            throw new IOException("restore already running for target");
        }
        try (var lockChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var lock = lockChannel.tryLock();
            if (lock == null) {
                throw new IOException("restore target locked by another process");
            }
            try (lock) {
                byte[] marker = (validated.manifestHash() + "\n" + (replica == null ? "" : replica.fingerprint() + "\n")).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                Path stage = destination.getParent().resolve(".azir-restore-" + manifest.backupId() + "-" + suffix);
                Path working;
                if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                    requireMarker(destination, marker);
                    working = destination;
                } else {
                    if (!Files.exists(stage, LinkOption.NOFOLLOW_LINKS)) {
                        Files.createDirectory(stage);
                        WalFiles.forceDirectory(stage.getParent());
                    }
                    requireDirectory(stage);
                    if (!Files.exists(stage.resolve("RESTORING"), LinkOption.NOFOLLOW_LINKS)) {
                        try (var entries = Files.list(stage)) {
                            if (entries.findAny().isPresent()) {
                                throw new IOException("unowned restore staging contents");
                            }
                        }
                        WalFiles.writeNew(stage.resolve("RESTORING"), marker);
                        WalFiles.forceDirectory(stage);
                    }
                    requireMarker(stage, marker);
                    working = stage;
                }
                // All recovery portals are allowed to change a new database or temporary directory only after the full validate backup and target ownership.
                for (var entry : manifest.files()) {
                    if (!entry.path().startsWith("local/")) {
                        continue;
                    }
                    Path output = checked(working, entry.path().substring(6), false);
                    if (entry.path().equals("local/REPLICA")) {
                        writeReplicaIdentity(output, replica, budget, force);
                    } else {
                        copy(checked(validated.root(), entry.path(), true), output, entry.path(), budget, false, entry, force);
                    }
                    hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_FILE_COPY);
                }
                forceDirectories(working, manifest.files().stream().filter(file -> file.path().startsWith("local/"))
                        .map(file -> working.resolve(file.path().substring(6)).getParent()).toList());
                for (var data : manifest.data()) {
                    budget.check();
                    var store = stores.get(data.shard());
                    var spec = new WalRestoreSpec(manifest.backupId(), operationId, manifest.engineId(), data.shard(), manifest.boundary(), replica);
                    boolean importing = store.beginRestore(spec);
                    if (importing) {
                        importData(validated, data, store, spec, budget, hook);
                    }
                    String hash = file(manifest, data.file()).hash();
                    store.finishRestore(spec, data.rows(), hash);
                }
                validateLocal(working, manifest, budget, replica);
                budget.check();
                hook.accept(WalMvccRuntime.Stage.BEFORE_RESTORE_PUBLISH);
                if (!working.equals(destination)) {
                    if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("restore target appeared before publication");
                    }
                    Files.move(working, destination, StandardCopyOption.ATOMIC_MOVE);
                    WalFiles.forceDirectory(destination.getParent());
                }
                hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_MOVE);
                Files.delete(destination.resolve("RESTORING"));
                WalFiles.forceDirectory(destination);
                hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_READY);
                return info(manifest, validated.info().bytes(), budget);
            }
        } finally {
            LOCAL_RESTORES.remove(lockPath);
        }
    }

    /**
     * Commit decision and materialization have been suspended while running to create backups, leaving any failure before the tag is completed without available backup.
     *
     * @param destination destination
     * @param log log
     * @param shards shards
     * @param stores stores
     * @param budget budget
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Info create(final Path destination, final WalLog log, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                       final Budget budget, final Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        Path target = canonicalTarget(destination);
        if (target.startsWith(log.directory()) || log.directory().startsWith(target)) {
            throw new IOException("backup overlaps active WAL directory");
        }
        Files.createDirectory(target);
        Files.createDirectory(target.resolve("local"));
        Files.createDirectory(target.resolve("data"));
        var entries = new ArrayList<WalBackupManifest.Entry>();
        var data = new ArrayList<WalBackupManifest.Data>();
        var boundary = log.lastSequence() == 0 ? new WalCheckpoint(0, "") : new WalCheckpoint(log.lastSequence(), log.receipt(log.lastSequence()).digest());
        for (var file : log.backupFiles().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            Path source = checked(log.directory(), file.getKey().substring(6), true);
            entries.add(copy(source, checked(target, file.getKey(), false), file.getKey(), budget, true, null, NORMAL_FORCE));
            hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_FILE_COPY);
        }
        int ordinal = 0;
        for (String shard : stores.keySet().stream().sorted().toList()) {
            String name = String.format(java.util.Locale.ROOT, "data/%04d.bin", ordinal++);
            Path path = checked(target, name, false);
            var digest = sha();
            long rows;
            try (
                    var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    var output = new BufferedOutputStream(new CountedOutput(new DigestOutputStream(java.nio.channels.Channels.newOutputStream(channel), digest), budget), 64 * 1024)) {
                var writer = new WalBackupData.Writer(output, new WalBackupData.Header(log.engineId(), shard, boundary));
                stores.get(shard).exportSnapshot(boundary, row -> {
                    budget.check();
                    writer.accept(row);
                });
                rows = writer.finish();
                output.flush();
                channel.force(true);
            }
            entries.add(new WalBackupManifest.Entry(name, Files.size(path), HexFormat.of().formatHex(digest.digest())));
            data.add(new WalBackupManifest.Data(shard, name, rows));
        }
        var manifest = new WalBackupManifest(UUID.randomUUID(), log.engineId(), boundary, shards, stores.keySet(), entries, data, log.replicaIdentity());
        byte[] bytes = manifest.encode();
        budget.add(bytes.length);
        validateContents(target, manifest, budget);
        forceDirectories(target, entries.stream().map(entry -> target.resolve(entry.path()).getParent()).toList());
        Path temporary = target.resolve("COMPLETE.tmp");
        WalFiles.writeNew(temporary, bytes);
        hook.accept(WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE);
        Files.move(temporary, target.resolve("COMPLETE"), StandardCopyOption.ATOMIC_MOVE);
        WalFiles.forceDirectory(target);
        WalFiles.forceDirectory(target.getParent());
        hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE);
        return info(manifest, budget.bytes, budget);
    }

    /**
     * The decision pause only reproduces the local recovery input and fixes a backend reading view, and complete cold data is exported in the subsequent maintenance line.
     *
     * @param destination destination
     * @param log log
     * @param shards shards
     * @param stores stores
     * @param budget budget
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Prepared prepareReplica(final Path destination, final WalLog log, final Set<String> shards, final Map<String, WalMaterializedStore> stores,
                                   final Budget budget, final Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        Path target = canonicalTarget(destination);
        checkDestination(target, log.directory());
        Files.createDirectory(target);
        Files.createDirectory(target.resolve("local"));
        Files.createDirectory(target.resolve("data"));
        var entries = new ArrayList<WalBackupManifest.Entry>();
        var views = new java.util.LinkedHashMap<String, WalMaterializedStore.SnapshotView>();
        var boundary = log.lastSequence() == 0 ? new WalCheckpoint(0, "") : new WalCheckpoint(log.lastSequence(), log.receipt(log.lastSequence()).digest());
        try {
            for (var file : log.backupFiles().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
                entries.add(copy(checked(log.directory(), file.getKey().substring(6), true), checked(target, file.getKey(), false), file.getKey(), budget, true, null, NORMAL_FORCE));
                hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_FILE_COPY);
            }
            for (String shard : stores.keySet().stream().sorted().toList()) {
                budget.check();
                views.put(shard, stores.get(shard).openSnapshot(boundary));
            }
            budget.check();
            return new Prepared(target, log.engineId(), log.replicaIdentity(), boundary, shards, entries, views, budget, hook);
        } catch (final Throwable failure) {
            for (var view : views.values()) {
                try {
                    view.close();
                } catch (final Throwable close) {
                    failure.addSuppressed(close);
                }
            }
            if (failure instanceof IOException io) {
                throw io;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IOException("prepare replica backup failed", failure);
        }
    }

    /**
     * Maintains overlapping targets that reject occupied directories and activity log catalogues before starting.
     *
     * @param destination destination
     * @param active active
     * @throws IOException if the operation cannot complete
     */
    static void checkDestination(final Path destination, final Path active) throws IOException {
        Path target = canonicalTarget(destination);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS) || target.startsWith(active) || active.startsWith(target)) {
            throw new IOException("backup target occupied or overlaps WAL");
        }
    }

    /**
     * For full backup file-by-file validate size, summary, content identity and data flow syntax.
     *
     * @param directory directory
     * @param budget budget
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Validated validate(final Path directory, final Budget budget) throws IOException {
        Path root = directory.toRealPath();
        requireDirectory(root);
        byte[] raw = WalFiles.readBounded(checked(root, "COMPLETE", true), WalBackupManifest.MAX_BYTES);
        var manifest = WalBackupManifest.decode(raw);
        budget.add(raw.length);
        for (var entry : manifest.files()) {
            budget.add(entry.size());
        }
        WalCheckpointState image = validateContents(root, manifest, budget);
        return new Validated(root, manifest, WalCodec.digest(raw), image, info(manifest, budget.bytes, budget));
    }

    /**
     * Summary protects the content of the file, format verify and the order of the key, boundaries and required collections.
     *
     * @param root root
     * @param manifest manifest
     * @param budget budget
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static WalCheckpointState validateContents(final Path root, final WalBackupManifest manifest, final Budget budget) throws IOException {
        for (var entry : manifest.files()) {
            Path path = checked(root, entry.path(), true);
            if (Files.size(path) != entry.size() || !checksum(path, budget).equals(entry.hash())) {
                throw new IOException("backup file checksum mismatch: " + entry.path());
            }
        }
        WalCheckpointState image = validateLocal(checked(root, "local", false), manifest, budget);
        for (var data : manifest.data()) {
            try (InputStream input = Files.newInputStream(checked(root, data.file(), true), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                long rows = WalBackupData.read(input, new WalBackupData.Header(manifest.engineId(), data.shard(), manifest.boundary()), row -> budget.check());
                if (rows != data.rows()) {
                    throw new IOException("backup row count mismatch");
                }
            }
        }
        return image;
    }

    /**
     * Check local recovery entry, end log, route and page commit receipt not only for the newly generated summary when copying.
     *
     * @param local local
     * @param manifest manifest
     * @param budget budget
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static WalCheckpointState validateLocal(final Path local, final WalBackupManifest manifest, final Budget budget) throws IOException {
        return validateLocal(local, manifest, budget, manifest.replica());
    }

    /**
     * Original mirror check source identity, recovery on-site check receiving identity; engines and groups are still immutable.
     *
     * @param local local
     * @param manifest manifest
     * @param budget budget
     * @param replica replica
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static WalCheckpointState validateLocal(final Path local, final WalBackupManifest manifest, final Budget budget, final WalReplicaIdentity replica) throws IOException {
        requireDirectory(local);
        // Rejects any embedded sign link before decrypting CURRENT and does not allow the unlisted entry to guide access to external files.
        try (var paths = Files.walk(local, 2)) {
            var iterator = paths.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (++count > 104_100 || Files.isSymbolicLink(path)
                        || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("invalid local backup file tree");
                }
            }
        }
        var current = WalCheckpointFiles.read(local);
        var expected = new HashSet<String>();
        expected.add("local/ENGINE");
        if (replica != null) {
            if (!replica.equals(WalReplicaIdentity.read(local))) {
                throw new IOException("backup replica identity mismatch");
            }
            expected.add("local/REPLICA");
        }
        byte[] engine = WalFiles.readBounded(checked(local, "ENGINE", true), 60);
        validateHeader(engine, manifest, replica == null ? 1 : 3, null);
        if (manifest.boundary().sequence() == 0) {
            if (current != null) {
                throw new IOException("empty backup has a checkpoint");
            }
            byte[] wal = WalFiles.readBounded(checked(local, "transactions.wal", true), 60);
            if (!java.util.Arrays.equals(engine, wal)) {
                throw new IOException("empty backup WAL identity mismatch");
            }
            expected.add("local/transactions.wal");
        } else {
            if (current == null || !current.state().engineId().equals(manifest.engineId()) || !current.state().shards().equals(manifest.shards())
                    || !current.state().storedShards().equals(manifest.storedShards()) || current.state().sequence() != manifest.boundary().sequence()
                    || !current.state().boundary().digest().equals(manifest.boundary().digest())) {
                throw new IOException("backup checkpoint identity mismatch");
            }
            String prefix = "local/" + current.generation().getFileName() + "/";
            expected.add("local/CURRENT");
            expected.add(prefix + "snapshot.bin");
            expected.add(prefix + "tail.wal");
            validateHeader(WalFiles.readBounded(checked(local, current.generation().getFileName() + "/tail.wal", true), 100), manifest, replica == null ? 2 : 4, current.imageHash());
            for (var archive : current.state().archives()) {
                budget.check();
                expected.add("local/" + archive.fileName());
                WalReceiptArchive.open(local, manifest.engineId(), archive).forEach(receipt -> {
                    try {
                        budget.check();
                    } catch (final IOException failure) {
                        throw new java.io.UncheckedIOException(failure);
                    }
                });
            }
        }
        for (var data : manifest.data()) {
            expected.add(data.file());
        }
        if (!expected.equals(manifest.files().stream().map(WalBackupManifest.Entry::path).collect(java.util.stream.Collectors.toSet()))) {
            throw new IOException("backup file catalog is incomplete or contains unknown files");
        }
        return current == null ? null : current.state();
    }

    /**
     * The header must be bound to the same engine, route and mirror, and a complete but damaged loghead cannot be legalized by backup.
     *
     * @param bytes bytes
     * @param manifest manifest
     * @param version version
     * @param imageHash image hash
     * @throws IOException if the operation cannot complete
     */
    private static void validateHeader(final byte[] bytes, final WalBackupManifest manifest, final int version, final String imageHash) throws IOException {
        int size = version == 1 || version == 3 ? 60 : 100;
        if (bytes.length != size) {
            throw new IOException("backup WAL header length mismatch");
        }
        var input = ByteBuffer.wrap(bytes);
        byte[] routes = HexFormat.of().parseHex(WalCodec.digest(String.join("\u0000", manifest.shards().stream().sorted().toList()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        if (input.getInt() != 0x415A574C || input.getInt() != version || !new UUID(input.getLong(), input.getLong()).equals(manifest.engineId())) {
            throw new IOException("backup WAL header identity mismatch");
        }
        byte[] actualRoutes = new byte[32];
        input.get(actualRoutes);
        if (!java.util.Arrays.equals(routes, actualRoutes) || ByteBuffer.wrap(bytes).getInt(size - 4) != WalFiles.crc(bytes, 0, size - 4)) {
            throw new IOException("backup WAL header checksum mismatch");
        }
        if (version == 2 || version == 4) {
            long cut = input.getLong();
            byte[] hash = new byte[32];
            input.get(hash);
            if (cut != manifest.boundary().sequence() || !HexFormat.of().formatHex(hash).equals(imageHash)) {
                throw new IOException("backup WAL checkpoint mismatch");
            }
        }
    }

    /**
     * Only new space with this recovery mark will be written for identity, and retry will be recreated.
     *
     * @param target target
     * @param replica replica
     * @param budget budget
     * @param force force
     * @throws IOException if the operation cannot complete
     */
    private static void writeReplicaIdentity(final Path target, final WalReplicaIdentity replica, final Budget budget, final ForceHook force) throws IOException {
        budget.check();
        byte[] bytes = java.util.Objects.requireNonNull(replica).encode();
        try (
                var channel = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            WalFiles.write(channel, ByteBuffer.wrap(bytes));
            force.force(target, channel);
        }
    }

    /**
     * Flow to a limited batch, and then enter the validate file summary, and keep the recovery at the back end when it fails.
     *
     * @param validated validated
     * @param data data
     * @param store store
     * @param spec spec
     * @param budget budget
     * @param hook hook
     * @throws IOException if the operation cannot complete
     */
    private static void importData(final Validated validated, final WalBackupManifest.Data data, final WalMaterializedStore store, final WalRestoreSpec spec,
                                   final Budget budget, final Consumer<WalMvccRuntime.Stage> hook) throws IOException {
        var batch = new ArrayList<WalBackupRow>();
        long[] bytes = {0};
        var digest = sha();
        try (
                InputStream raw = Files.newInputStream(checked(validated.root(), data.file(), true), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                InputStream input = new java.security.DigestInputStream(raw, digest)) {
            long rows = WalBackupData.read(input, new WalBackupData.Header(spec.engineId(), spec.shard(), spec.boundary()), row -> {
                budget.check();
                long size = 256L + row.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + row.version().valueLength();
                if (size > budget.options.batchBytes()) {
                    throw new IOException("single restore row exceeds batch budget");
                }
                if (!batch.isEmpty() && (batch.size() >= budget.options.batchKeys() || bytes[0] + size > budget.options.batchBytes())) {
                    store.restoreBatch(spec, List.copyOf(batch));
                    hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_BATCH);
                    batch.clear();
                    bytes[0] = 0;
                }
                batch.add(row);
                bytes[0] += size;
            });
            if (rows != data.rows() || !HexFormat.of().formatHex(digest.digest()).equals(file(validated.manifest(), data.file()).hash())) {
                throw new IOException("backup changed during restore");
            }
        }
        if (!batch.isEmpty()) {
            store.restoreBatch(spec, List.copyOf(batch));
            hook.accept(WalMvccRuntime.Stage.AFTER_RESTORE_BATCH);
        }
    }

    /**
     * Check recovery budget with no backend version, and cannot release a directory that could not be started due to insufficient memory.
     *
     * @param image image
     * @param options options
     * @throws IOException if the operation cannot complete
     */
    private static void validateMemory(final WalCheckpointState image, final WalOptions options) throws IOException {
        java.util.Objects.requireNonNull(options);
        if (image == null) {
            return;
        }
        var keys = new HashSet<WalMvccRuntime.Key>();
        long bytes = 0;
        for (var version : image.versions()) {
            var mutation = version.mutation();
            if (image.storedShards().contains(mutation.shard())) {
                continue;
            }
            if (keys.add(new WalMvccRuntime.Key(mutation.shard(), mutation.key()))) {
                bytes += 128L + 2L * (mutation.shard().length() + mutation.key().length());
            }
            bytes += mutation.retainedBytes();
            if (bytes > options.maxMemoryBytes()) {
                throw new IOException("restore exceeds local version memory budget");
            }
        }
    }

    /**
     * Finds file descriptions from the immutable list, missing entries cannot be defaulted to empty files.
     *
     * @param manifest manifest
     * @param name name
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static WalBackupManifest.Entry file(final WalBackupManifest manifest, final String name) throws IOException {
        return manifest.files().stream().filter(entry -> entry.path().equals(name)).findFirst().orElseThrow(() -> new IOException("missing backup file"));
    }

    /**
     * Copy pieces, validate and sync files to continue with only your own suspense files with valid recovery tags.
     *
     * @param source database connection source
     * @param target target
     * @param name name
     * @param budget budget
     * @param account account
     * @param expected expected
     * @param force force
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static WalBackupManifest.Entry copy(final Path source, final Path target, final String name, final Budget budget, final boolean account,
                                                final WalBackupManifest.Entry expected, final ForceHook force) throws IOException {
        Files.createDirectories(target.getParent());
        if (expected != null && Files.exists(target, LinkOption.NOFOLLOW_LINKS) && Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)
                && Files.size(target) == expected.size() && checksum(target, budget).equals(expected.hash())) {
            // The previous process may exit when it has been completed but is not brushed; the summary correctly only proves that the content is readable and does not prove that it is durable storage
            // .
            try (var channel = FileChannel.open(target, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                force.force(target, channel);
            }
            return expected;
        }
        var digest = sha();
        long size = 0;
        try (
                var input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                var output = FileChannel.open(target, account ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, account ? StandardOpenOption.WRITE : StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(64 * 1024);
            while (true) {
                budget.check();
                int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    throw new IOException("backup copy made no progress");
                }
                if (account) {
                    budget.add(read);
                }
                size += read;
                digest.update(buffer.array(), 0, read);
                buffer.flip();
                WalFiles.write(output, buffer);
                buffer.clear();
            }
            force.force(target, output);
        }
        var entry = new WalBackupManifest.Entry(name, size, HexFormat.of().formatHex(digest.digest()));
        if (expected != null && !expected.equals(entry)) {
            throw new IOException("backup changed while copying");
        }
        return entry;
    }

    /**
     * Calculates the file summary with a fixed memory, single archive can be larger than the runtime cache budget.
     *
     * @param path path
     * @param budget budget
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static String checksum(final Path path, final Budget budget) throws IOException {
        var digest = sha();
        byte[] buffer = new byte[64 * 1024];
        try (var input = Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            while (true) {
                budget.check();
                int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    throw new IOException("backup checksum made no progress");
                }
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /**
     * Retrieving the location of a file without a symbol link for all sections of the path is forbidden to go beyond the specified root directory.
     *
     * @param root root
     * @param relative relative
     * @param file file
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Path checked(final Path root, final String relative, final boolean file) throws IOException {
        Path path = root.resolve(relative).normalize();
        if (!path.startsWith(root)) {
            throw new IOException("backup path escapes root");
        }
        Path current = root;
        for (Path part : root.relativize(path)) {
            current = current.resolve(part);
            if (Files.isSymbolicLink(current)) {
                throw new IOException("backup path contains symbolic link");
            }
        }
        if (file && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("backup file missing or not regular");
        }
        return path;
    }

    /**
     * Sets the physical location of the target parent directory, and the target itself must have a normal filename.
     *
     * @param requested requested
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static Path canonicalTarget(final Path requested) throws IOException {
        Path path = requested.toAbsolutePath().normalize();
        if (path.getFileName() == null) {
            throw new IOException("invalid backup target");
        }
        return path.getParent().toRealPath().resolve(path.getFileName());
    }

    /**
     * The recovery mark allows only the continuation of this complete list, and a common directory can never be covered in secret.
     *
     * @param root root
     * @param expected expected
     * @throws IOException if the operation cannot complete
     */
    private static void requireMarker(final Path root, final byte[] expected) throws IOException {
        requireDirectory(root);
        if (!java.util.Arrays.equals(expected, WalFiles.readBounded(checked(root, "RESTORING", true), 256))) {
            throw new IOException("restore target belongs to another operation");
        }
    }

    /**
     * Rejects a symbolic link to a fixed recovery directory and a non-directory.
     *
     * @param root root
     * @throws IOException if the operation cannot complete
     */
    private static void requireDirectory(final Path root) throws IOException {
        if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("backup directory missing or symbolic");
        }
    }

    /**
     * The subdirectories are synchronized before the parent directory and are finally cleared by COMPLETE or recovery tags for release.
     *
     * @param root root
     * @param parents parents
     * @throws IOException if the operation cannot complete
     */
    private static void forceDirectories(final Path root, final List<Path> parents) throws IOException {
        var directories = new HashSet<Path>();
        directories.add(root);
        for (Path parent : parents) {
            for (Path current = parent; current != null && current.startsWith(root); current = current.getParent()) {
                directories.add(current);
            }
        }
        for (Path directory : directories.stream().sorted(java.util.Comparator.comparingInt(Path::getNameCount).reversed()).toList()) {
            WalFiles.forceDirectory(directory);
        }
    }

    /**
     * Creates a fixed SHA-256 Abstract without explaining the absence of an environmental algorithm as a success.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static MessageDigest sha() throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final java.security.NoSuchAlgorithmException failure) {
            throw new IOException(failure);
        }
    }

    /**
     * Generates a public maintenance result consistent with the verify list.
     *
     * @param manifest manifest
     * @param bytes bytes
     * @param budget budget
     * @return the operation result
     */
    private static Info info(final WalBackupManifest manifest, final long bytes, final Budget budget) {
        var rows = new HashMap<String, Long>();
        for (var data : manifest.data()) {
            rows.put(data.shard(), data.rows());
        }
        return new Info(manifest.backupId(), manifest.engineId(), manifest.boundary().sequence(), manifest.boundary().digest(),
                manifest.shards(), manifest.storedShards(), rows, bytes, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - budget.start));
    }

    /**
     * Only for the receipt and inspection of real file brushes, normal recovery always executes force (true).
     */
    @FunctionalInterface
    interface ForceHook {

        /**
         * Syncs a custom recovery file that has been written, and the recovery directory cannot be published if it fails.
         *
         * @param path path
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        void force(Path path, FileChannel channel) throws IOException;
    }

    /**
     * Immutable 's dimensional result, bytes are the total number of backup files rather than the actual database disk volume.
     *
     * @param backupId backup id
     * @param engineId engine id
     * @param sequence sequence
     * @param digest digest
     * @param shards shards
     * @param storedShards stored shards
     * @param rows rows
     * @param bytes bytes
     * @param elapsedMillis elapsed millis
     */
    public record Info(UUID backupId, UUID engineId, long sequence, String digest, Set<String> shards,
            Set<String> storedShards, Map<String, Long> rows, long bytes, long elapsedMillis) {

        /**
         * Fixed route and count, the caller cannot modify the validate result.
         *
         * @param backupId backup id
         * @param engineId engine id
         * @param sequence sequence
         * @param digest digest
         * @param shards shards
         * @param storedShards stored shards
         * @param rows rows
         * @param bytes bytes
         * @param elapsedMillis elapsed millis
         */
        public Info {
            shards = Set.copyOf(shards);
            storedShards = Set.copyOf(storedShards);
            rows = Map.copyOf(rows);
        }
    }

    /**
     * Bounded I/O public time budget, cancels only this maintenance and does not cancel shared log channels.
     */
    static final class Budget {

        final WalBackupOptions options;

        final long start = System.nanoTime();

        long bytes;

        /**
         * Create an independent budget for a backup, validate or recovery.
         *
         * @param options options
         */
        Budget(final WalBackupOptions options) {
            this.options = java.util.Objects.requireNonNull(options);
        }

        /**
         * Each segment is checked for interruptions and unmodified durations, and the maintenance window cannot be extended by changes in the wall clock.
         *
         * @throws IOException if the operation cannot complete
         */
        void check() throws IOException {
            if (Thread.currentThread().isInterrupted() || System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis())) {
                throw new IOException("backup or restore interrupted or timed out");
            }
        }

        /**
         * Check the byte budget before writing to avoid an incomplete backup of target file systems.
         *
         * @param count count
         * @throws IOException if the operation cannot complete
         */
        void add(final long count) throws IOException {
            check();
            if (count < 0 || count > options.maxBytes() - bytes) {
                throw new IOException("backup exceeds byte budget");
            }
            bytes += count;
        }
    }

    /**
     * The verify list, the summary and the recovery mirror are still being processed in a fluid manner.
     *
     * @param root root
     * @param manifest manifest
     * @param manifestHash manifest hash
     * @param image image
     * @param info info
     */
    private record Validated(Path root, WalBackupManifest manifest, String manifestHash, WalCheckpointState image, Info info) {
    }

    /**
     * The copied local status is independent of the source follow-up check point, and the reading view is independent of the source follow-up materialization and GC.
     */
    static final class Prepared implements AutoCloseable {

        private final Path target;

        private final UUID engine;

        private final WalReplicaIdentity replica;

        private final WalCheckpoint boundary;

        private final Set<String> shards;

        private final List<WalBackupManifest.Entry> entries;

        private final Map<String, WalMaterializedStore.SnapshotView> views;

        private final Budget budget;

        private final Consumer<WalMvccRuntime.Stage> hook;

        /**
         * All inputs can be constructed only after the same confirmed boundary has been fixed.
         *
         * @param target target
         * @param engine engine
         * @param replica replica
         * @param boundary boundary
         * @param shards shards
         * @param entries entries
         * @param views views
         * @param budget budget
         * @param hook hook
         */
        Prepared(final Path target, final UUID engine, final WalReplicaIdentity replica, final WalCheckpoint boundary, final Set<String> shards,
                 final List<WalBackupManifest.Entry> entries, final Map<String, WalMaterializedStore.SnapshotView> views, final Budget budget, final Consumer<WalMvccRuntime.Stage> hook) {
            this.target = target;
            this.engine = engine;
            this.replica = replica;
            this.boundary = boundary;
            this.shards = Set.copyOf(shards);
            this.entries = new ArrayList<>(entries);
            this.views = Map.copyOf(views);
            this.budget = budget;
            this.hook = hook;
        }

        /**
         * Fixed borders no longer read the current position of the source log.
         *
         * @return the operation result
         */
        WalCheckpoint boundary() {
            return boundary;
        }

        /**
         * Background completes all cold data output, file-by-file verify and tag release.
         *
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        Info complete() throws IOException {
            var data = new ArrayList<WalBackupManifest.Data>();
            int ordinal = 0;
            for (String shard : views.keySet().stream().sorted().toList()) {
                budget.check();
                String name = String.format(java.util.Locale.ROOT, "data/%04d.bin", ordinal++);
                Path path = checked(target, name, false);
                var digest = sha();
                long rows;
                try (
                        var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                        var output = new BufferedOutputStream(new CountedOutput(new DigestOutputStream(java.nio.channels.Channels.newOutputStream(channel), digest), budget), 64 * 1024)) {
                    var writer = new WalBackupData.Writer(output, new WalBackupData.Header(engine, shard, boundary));
                    views.get(shard).export(row -> {
                        budget.check();
                        writer.accept(row);
                    });
                    rows = writer.finish();
                    output.flush();
                    channel.force(true);
                }
                entries.add(new WalBackupManifest.Entry(name, Files.size(path), HexFormat.of().formatHex(digest.digest())));
                data.add(new WalBackupManifest.Data(shard, name, rows));
            }
            var manifest = new WalBackupManifest(UUID.randomUUID(), engine, boundary, shards, views.keySet(), entries, data, replica);
            byte[] bytes = manifest.encode();
            budget.add(bytes.length);
            validateContents(target, manifest, budget);
            forceDirectories(target, entries.stream().map(entry -> target.resolve(entry.path()).getParent()).toList());
            WalFiles.writeNew(target.resolve("COMPLETE.tmp"), bytes);
            hook.accept(WalMvccRuntime.Stage.BEFORE_BACKUP_COMPLETE);
            Files.move(target.resolve("COMPLETE.tmp"), target.resolve("COMPLETE"), StandardCopyOption.ATOMIC_MOVE);
            WalFiles.forceDirectory(target);
            WalFiles.forceDirectory(target.getParent());
            hook.accept(WalMvccRuntime.Stage.AFTER_BACKUP_COMPLETE);
            return info(manifest, budget.bytes, budget);
        }

        /**
         * Any view closing failed also to close the remaining connection, leaving no indefinite database reading transaction.
         *
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (var view : views.values()) {
                try {
                    view.close();
                } catch (final Exception close) {
                    if (failure == null) {
                        failure = new IOException("close pinned backup views failed", close);
                    } else {
                        failure.addSuppressed(close);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    /**
     * Deducts the budget file before it is actually written, without relying on a disk size check after writing.
     */
    private static final class CountedOutput extends OutputStream {

        private final OutputStream output;

        private final Budget budget;

        /**
         * Configure the independent output of this backup budget.
         *
         * @param output output
         * @param budget budget
         */
        CountedOutput(final OutputStream output, final Budget budget) {
            this.output = output;
            this.budget = budget;
        }

        /**
         * Enforce the budget even for a single-byte write.
         *
         * @param value value to write
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final int value) throws IOException {
            budget.add(1);
            output.write(value);
        }

        /**
         * Check the resources before they are written and then hand them over to a summary and disk flow.
         *
         * @param bytes bytes
         * @param offset offset
         * @param length length
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final byte[] bytes, final int offset, final int length) throws IOException {
            budget.add(length);
            output.write(bytes, offset, length);
        }

        /**
         * Refreshs the current own output and does not operate to share WAL.
         *
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void flush() throws IOException {
            output.flush();
        }

        /**
         * Turns off your own backup output, and cannot close the application database or run time.
         *
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
