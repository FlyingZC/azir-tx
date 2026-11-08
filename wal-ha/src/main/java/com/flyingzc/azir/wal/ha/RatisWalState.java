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

import com.flyingzc.azir.wal.WalBackup;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalRetentionOptions;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Applies replicated WAL decisions while preserving snapshot, generation and materialization ordering.
 */
final class RatisWalState implements AutoCloseable {

    private static final Set<Path> LOCAL = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private final ReentrantLock gate = new ReentrantLock();

    private final Path directory;

    private final WalReplicaIdentity identity;

    private final Set<String> shards;

    private final WalOptions options;

    private final WalCommitAuthority authority;

    private final WalReplicaBackendFactory factory;

    private final WalBackupOptions limits;

    private final Hook hook;

    private FileChannel lockChannel;

    private FileLock lock;

    private WalReplicaBackendFactory.Lease lease;

    private RatisStartupException pendingBackend;

    private volatile WalMvccRuntime core;

    private volatile boolean ready;

    private boolean closed;

    private RatisGenerationRecord active;

    private final RetirementHook retirementHook;

    /**
     * Failure verify in the package is suspended at a real automatic maintenance position, and the hooks at the official entrance are empty and do not expose network control interface.
     *
     * @param stage stage
     * @throws IOException if the operation cannot complete
     */
    void lifecycleStage(final Stage stage) throws IOException {
        hook.hit(stage);
    }

    /**
     * Collection proves that it comes from the actual original birthday and full mirror, and does not use speculative positions.
     *
     * @param stage stage
     * @param firstIndex first index
     * @param coveredIndex covered index
     * @throws IOException if the operation cannot complete
     */
    void lifecycleStage(final Stage stage, final long firstIndex, final long coveredIndex) throws IOException {
        hook.hit(stage, firstIndex, coveredIndex);
    }

    /**
     * The original fixed backend assembly retains the existing local path and does not automatically upgrade to a covered generation plant.
     *
     * @param core core
     */
    RatisWalState(final WalMvccRuntime core) {
        this.core = core;
        ready = true;
        directory = null;
        identity = null;
        shards = Set.of();
        options = null;
        authority = null;
        factory = null;
        limits = null;
        hook = stage -> {
        };
        retirementHook = stage -> {
        };
    }

    /**
     * Creates a unique business root directory; the old generation is not opened when installation operations are in place and the residue catalogue is not used as an empty node.
     *
     * @param requested requested
     * @param identity identity
     * @param shards shards
     * @param options options
     * @param authority authority
     * @param factory factory
     * @param limits limits
     * @param hook hook
     * @throws IOException if the operation cannot complete
     */
    RatisWalState(final Path requested, final WalReplicaIdentity identity, final Set<String> shards, final WalOptions options,
                  final WalCommitAuthority authority, final WalReplicaBackendFactory factory, final WalBackupOptions limits, final Hook hook) throws IOException {
        this(requested, identity, shards, options, authority, factory, limits, hook, stage -> {
        });
    }

    /**
     * The formal assembly is not injected by default, and the retirement test is independent of the installation test.
     *
     * @param requested requested
     * @param identity identity
     * @param shards shards
     * @param options options
     * @param authority authority
     * @param factory factory
     * @param limits limits
     * @param hook hook
     * @param retirementHook retirement hook
     * @throws IOException if the operation cannot complete
     */
    RatisWalState(final Path requested, final WalReplicaIdentity identity, final Set<String> shards, final WalOptions options,
                  final WalCommitAuthority authority, final WalReplicaBackendFactory factory, final WalBackupOptions limits, final Hook hook, final RetirementHook retirementHook) throws IOException {
        this.identity = identity;
        this.shards = Set.copyOf(shards);
        this.options = options;
        this.authority = authority;
        this.factory = factory;
        this.limits = limits;
        this.hook = hook;
        this.retirementHook = retirementHook;
        if (Files.isSymbolicLink(requested)) {
            throw new IOException("business generation root is symbolic");
        }
        Files.createDirectories(requested);
        directory = requested.toRealPath();
        if (!LOCAL.add(directory)) {
            throw new IOException("business generations already open in this JVM");
        }
        try {
            lockChannel = FileChannel.open(directory.resolve("writer.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
            lock = lockChannel.tryLock();
            if (lock == null) {
                throw new IOException("business generations already owned");
            }
            if (Files.exists(directory.resolve("ACTIVE"), LinkOption.NOFOLLOW_LINKS)) {
                active = checkedRecord("ACTIVE");
            }
            if (!Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) {
                if (active == null) {
                    if (Files.exists(directory.resolve("BOOTSTRAPPING"))) {
                        active = checkedRecord("BOOTSTRAPPING");
                        if (active.term() != -1) {
                            throw new IOException("invalid bootstrap phase");
                        }
                    } else {
                        try (var paths = Files.list(directory)) {
                            if (paths.anyMatch(path -> !path.getFileName().toString().equals("writer.lock"))) {
                                throw new IOException("orphan generation state cannot bootstrap");
                            }
                        }
                        active = new RatisGenerationRecord(identity, UUID.randomUUID(), -1, -1, 0, "");
                        active.publish(directory.resolve("BOOTSTRAPPING"));
                    }
                    openCore(active, true);
                    if (core.stats().publishedSequence() != 0) {
                        throw new IOException("bootstrap generation has unexpected decisions");
                    }
                    active.publish(directory.resolve("ACTIVE"));
                } else {
                    openCore(active, false);
                    if (Files.exists(directory.resolve("BOOTSTRAPPING")) && (!active.equals(checkedRecord("BOOTSTRAPPING")) || core.stats().publishedSequence() != 0)) {
                        throw new IOException("bootstrap generation phase mismatch");
                    }
                }
            } else {
                checkedRecord("INSTALLING");
            }
        } catch (final Throwable failure) {
            throw RatisStartupException.afterFailure(failure, this::close);
        }
    }

    /**
     * Fixed configuration does not support snapshot replacement, avoiding overwriting the existing database caller.
     *
     * @return the operation result
     */
    boolean supportsSnapshots() {
        return directory != null;
    }

    /**
     * The automatic life cycle must have a restricted backend retirement protocol, and the custom storage cannot be deleted by default.
     *
     * @return the operation result
     */
    boolean supportsRetirement() {
        return factory != null && factory.supportsRetirement();
    }

    /**
     * Automatic background mirror requires that each backend fixes a physical reading view at the preparation stage.
     *
     * @return the operation result
     */
    boolean supportsPinnedSnapshots() {
        gate.lock();
        try {
            return lease != null && lease.stores().values().stream().allMatch(com.flyingzc.azir.wal.WalMaterializedStore::supportsPinnedSnapshot);
        } finally {
            gate.unlock();
        }
    }

    /**
     * The configured automatic version collection requires a limited backend maintenance capability for each current and subsequent generation.
     */
    private boolean boundedGcRequired;

    /**
     * The durable Auto snapshot policy is also limited by the recovery or installed lease.
     */
    private boolean pinnedSnapshotsRequired;

    /**
     * Check current lease before commissioning, and subsequent installation must also satisfy the same capability contract.
     *
     * @throws IOException if the operation cannot complete
     */
    void requireBoundedGc() throws IOException {
        gate.lock();
        try {
            if (lease == null) {
                requirePendingLease();
            } else if (lease.stores().values().stream().anyMatch(store -> !store.supportsBoundedMaintenance())) {
                throw new IOException("replica backend cannot perform bounded version GC");
            }
            boundedGcRequired = true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * Lease is not yet complete for installation, and the registration capability requires that validate be enforced when lease is actually opened in recovery.
     *
     * @throws IOException if the operation cannot complete
     */
    void requirePinnedSnapshots() throws IOException {
        gate.lock();
        try {
            if (lease == null) {
                requirePendingLease();
            } else if (lease.stores().values().stream().anyMatch(store -> !store.supportsPinnedSnapshot())) {
                throw new IOException("replica backend cannot pin snapshot views");
            }
            pinnedSnapshotsRequired = true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * Only identity -matched durable recovery operation is allowed to delay the lease check, normal missing lease still fails.
     *
     * @throws IOException if the operation cannot complete
     */
    private void requirePendingLease() throws IOException {
        if (directory == null || closed || !Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("replica backend lease unavailable");
        }
        checkedRecord("INSTALLING");
    }

    /**
     * The independent collection has actually applied version, and transaction safe point is still determined by kernel activity snapshot.
     *
     * @param options options
     * @return the operation result
     */
    GcSample collectVersionGarbage(final RatisGcOptions options) {
        if (!gate.tryLock()) {
            return null;
        }
        try {
            if (!boundedGcRequired || !ready || closed || core == null || !coreHealthy() || !core.lifecycle().accepting()) {
                return null;
            }
            var progress = core.collectGarbageBounded(options.keyLimit(), options.rowLimit(), options.workMillis());
            return new GcSample(progress, core.stats(), active.generation());
        } finally {
            gate.unlock();
        }
    }

    /**
     * The active snapshot boundary must be covered by the collection position, and collection permissions are not provided when installation is incomplete.
     *
     * @param index index
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    boolean activatedSnapshotCovers(final long index) throws IOException {
        return activatedSnapshotCovers(index, RatisRecordFile.REAL);
    }

    /**
     * See that ACTIVE may have failed from the last synchronised directory, and actual records must be reconfirmed before deleted.
     *
     * @param index index
     * @param io io
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    boolean activatedSnapshotCovers(final long index, final RatisRecordFile.Io io) throws IOException {
        if (!gate.tryLock()) {
            return false;
        }
        try {
            if (!ready || closed || active == null || Files.exists(directory.resolve("INSTALLING"))) {
                return false;
            }
            var actual = checkedRecord("ACTIVE");
            if (!actual.generation().equals(active.generation()) || actual.index() < active.index() || actual.sequence() > core.stats().publishedSequence()) {
                throw new IOException("active snapshot durability evidence conflicts with current lease");
            }
            if (actual.index() < index) {
                return false;
            }
            RatisRecordFile.confirm(directory.resolve("ACTIVE"), actual.encode(), io);
            active = actual;
            return true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * The readiness mark is only real after complete installation, synchronisation of the pointer and operational clean-up.
     *
     * @return the operation result
     */
    boolean ready() {
        return ready;
    }

    /**
     * The local kernel is immediately disqualified from operating without access to database in probe readiness.
     *
     * @return the operation result
     */
    boolean coreHealthy() {
        var current = core;
        if (current == null) {
            return false;
        }
        var status = current.lifecycle();
        return status.fatal() == null && !status.closed();
    }

    /**
     * The actual check backend readability and prefixes before a member ' s promotion do not go back, and cannot only be trusted with a cached health marker.
     *
     * @return the operation result
     */
    boolean backendHealthy() {
        return backendHealthy(core, 1000);
    }

    /**
     * Detection of fixed kernel generation, time budget; database call still subject to timeout driver already verify.
     *
     * @param expected expected
     * @param budgetMillis budget millis
     * @return the operation result
     */
    boolean backendHealthy(final WalMvccRuntime expected, final long budgetMillis) {
        long until = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(budgetMillis);
        boolean locked = false;
        try {
            locked = gate.tryLock(budgetMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!locked || !ready || core == null || core != expected || !coreHealthy()) {
                return false;
            }
            var before = core.stats();
            var checkpoints = new java.util.HashMap<String, Long>();
            if (lease != null) {
                for (var entry : lease.stores().entrySet()) {
                    if (System.nanoTime() >= until) {
                        return false;
                    }
                    checkpoints.put(entry.getKey(), entry.getValue().checkpoint().sequence());
                }
            }
            if (System.nanoTime() >= until) {
                return false;
            }
            var after = core.stats();
            if (System.nanoTime() >= until) {
                return false;
            }
            // materialization may advance during a query, the upper boundary must retrieve the post-survey release watermark and the lower bound must have been known before the search materialization
            // watermark .
            for (var entry : checkpoints.entrySet()) {
                if (entry.getValue() < before.materializedSequences().getOrDefault(entry.getKey(), 0L) || entry.getValue() > after.publishedSequence()) {
                    return false;
                }
            }
            return true;
        } catch (final RuntimeException unavailable) {
            return false;
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            if (locked) {
                gate.unlock();
            }
        }
    }

    /**
     * The installation of the entrance is preceded by the de-eligibility of the service, followed by the vacating of applications already commit.
     */
    void stopServing() {
        ready = false;
    }

    /**
     * Internal installers can access the kernel during start-up, and operational entry points are still subject to ready inspection.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    WalMvccRuntime core() {
        var result = core;
        if (result == null) {
            throw new IllegalStateException("business state not open");
        }
        return result;
    }

    /**
     * The observation is called by independent workers, while the generation rush skips and cannot be blocked for installation pending monitoring.
     *
     * @return the operation result
     */
    LocalSample localSample() {
        if (!gate.tryLock()) {
            return null;
        }
        try {
            if (!ready || closed || core == null || active == null || !coreHealthy()) {
                return null;
            }
            return new LocalSample(core.stats(), active.generation());
        } finally {
            gate.unlock();
        }
    }

    /**
     * Closed old sentence handles cannot be used to override generation, and only local retirement is done for the current kernel by a character event.
     *
     * @param epoch epoch
     */
    void fence(final long epoch) {
        var current = core;
        if (current != null && epoch > 0) {
            current.fenceEpoch(epoch);
        }
    }

    /**
     * Disallow service first and empty the old kernel without interrupting WAL I/O or deleting the backend.
     *
     * @throws IOException if the operation cannot complete
     */
    void pause() throws IOException {
        gate.lock();
        try {
            ready = false;
            closeCore();
        } finally {
            gate.unlock();
        }
    }

    /**
     * Recovery after start-up or Ratis installation has been completed; the existing installation must be given priority.
     *
     * @param snapshot snapshot
     * @param position position
     * @throws IOException if the operation cannot complete
     */
    void initialize(final Path snapshot, final TermIndex position) throws IOException {
        if (!supportsSnapshots()) {
            return;
        }
        gate.lock();
        try {
            if (closed) {
                throw new IOException("generation manager closed");
            }
            ready = false;
            if (Files.exists(directory.resolve("INSTALLING"), LinkOption.NOFOLLOW_LINKS)) {
                complete(checkedRecord("INSTALLING"));
            }
            if (snapshot != null) {
                String hash = hash(snapshot);
                var header = RatisSnapshotBundle.inspect(snapshot, RaftGroupId.valueOf(identity.groupId()), position, limits);
                if (active != null && active.index() == position.getIndex()) {
                    if (active.term() != position.getTerm() || !active.hash().equals(hash)) {
                        throw new IOException("active snapshot identity mismatch");
                    }
                } else {
                    if (active != null && active.index() > position.getIndex()) {
                        throw new IOException("Raft snapshot behind active generation");
                    }
                    complete(prepare(snapshot, position, header, hash));
                }
            } else if (active == null || active.index() >= 0) {
                throw new IOException("active generation requires its Raft snapshot");
            }
            if (core == null) {
                openCore(active, false);
            }
            // The formal assembly is preceded by the synchronization of the Raft voting directory and then by a call to this method; this mark shall be deleted without losing the re-formatization of
            // the voting status.
            if (Files.exists(directory.resolve("BOOTSTRAPPING"))) {
                RatisGenerationRecord.remove(directory.resolve("BOOTSTRAPPING"));
            }
            // If the synchronisation of a directory fails after last deleted control records, retry may see only that the record has disappeared; the directory is reconfirmed before release.
            hook.hit(Stage.BEFORE_READY_DIRECTORY_FORCE);
            RatisGenerationRecord.forceDirectory(directory);
            hook.hit(Stage.AFTER_READY_DIRECTORY_FORCE);
            ready = true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * Snapshot RPC confirms the pre-independent durable storage input and operation, and will also be safe if the walker entry is not completed.
     *
     * @param snapshot snapshot
     * @param position position
     * @throws IOException if the operation cannot complete
     */
    void durableBeforeSnapshotAck(final Path snapshot, final TermIndex position) throws IOException {
        gate.lock();
        try {
            if (closed) {
                throw new IOException("generation manager closed");
            }
            String hash = hash(snapshot);
            if (active != null && active.index() >= position.getIndex()) {
                if (active.index() == position.getIndex() && (active.term() != position.getTerm() || !active.hash().equals(hash))) {
                    throw new IOException("snapshot ACK active identity mismatch");
                }
                return;
            }
            if (Files.exists(directory.resolve("INSTALLING"))) {
                var pending = checkedRecord("INSTALLING");
                if (pending.index() >= position.getIndex()) {
                    if (pending.index() == position.getIndex() && (pending.term() != position.getTerm() || !pending.hash().equals(hash))) {
                        throw new IOException("snapshot ACK pending identity mismatch");
                    }
                    if (!hash(work(pending).resolve("snapshot.bundle")).equals(pending.hash())) {
                        throw new IOException("snapshot ACK recovery input changed");
                    }
                    return;
                }
                complete(pending);
            }
            ready = false;
            var header = RatisSnapshotBundle.inspect(snapshot, RaftGroupId.valueOf(identity.groupId()), position, limits);
            prepare(snapshot, position, header, hash);
        } finally {
            gate.unlock();
        }
    }

    /**
     * Synchronizes the input and operation, then allows any confirmation or import; the actual generation is derived only from the UUID generated by this machine.
     *
     * @param snapshot snapshot
     * @param position position
     * @param header header
     * @param hash hash
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisGenerationRecord prepare(final Path snapshot, final TermIndex position, final RatisSnapshotBundle.Header header, final String hash) throws IOException {
        var plan = new RatisGenerationRecord(identity, UUID.randomUUID(), position.getTerm(), position.getIndex(), header.boundary().sequence(), hash);
        Path work = work(plan);
        Files.createDirectory(work);
        plan.publish(work.resolve("INSTALLATION"));
        copyBundle(snapshot, work.resolve("snapshot.bundle"));
        RatisGenerationRecord.forceDirectory(directory);
        plan.publish(directory.resolve("INSTALLING"));
        hook.hit(Stage.AFTER_INSTALL_RECORD);
        return plan;
    }

    /**
     * The large mirror summary leaves the application thread, and when a signature changes, it is refused to be published and cannot be mixed with the contents of different documents.
     *
     * @param header header
     * @param bundle bundle
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    SnapshotNote prepareSnapshotNote(final RatisSnapshotBundle.Header header, final Path bundle) throws IOException {
        var before = Files.readAttributes(bundle, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        String digest = hash(bundle);
        var after = Files.readAttributes(bundle, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!after.isRegularFile() || !java.util.Objects.equals(before.fileKey(), after.fileKey()) || before.size() != after.size() || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
            throw new IOException("snapshot file changed during activation preparation");
        }
        return new SnapshotNote(header, bundle, digest, after.fileKey(), after.size(), after.lastModifiedTime());
    }

    /**
     * Local Full snapshot Boundaries is issued short, restarting without importing a full backend into the new space.
     *
     * @param note note
     * @throws IOException if the operation cannot complete
     */
    void noteSnapshot(final SnapshotNote note) throws IOException {
        noteSnapshot(note, RatisRecordFile.REAL);
    }

    /**
     * The I/O in the package injects an uncertain result of actual activation of verify replacement, and the official entry is always using a real file system.
     *
     * @param note note
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    void noteSnapshot(final SnapshotNote note, final RatisRecordFile.Io io) throws IOException {
        gate.lock();
        try {
            if (closed || !ready || active == null || Files.exists(directory.resolve("INSTALLING"))) {
                throw new IOException("generation not ready for snapshot publication");
            }
            var current = Files.readAttributes(note.bundle(), java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!current.isRegularFile() || !java.util.Objects.equals(current.fileKey(), note.fileKey()) || current.size() != note.bytes() || !current.lastModifiedTime().equals(note.modified())) {
                throw new IOException("snapshot file changed before activation");
            }
            var header = note.header();
            if (header.position().getIndex() < active.index() || header.boundary().sequence() > core.stats().publishedSequence()
                    || header.position().getIndex() == active.index() && (header.position().getTerm() != active.term() || !note.hash().equals(active.hash()))) {
                throw new IOException("snapshot activation regressed or conflicts with generation");
            }
            var next = new RatisGenerationRecord(identity, active.generation(), header.position().getTerm(), header.position().getIndex(), header.boundary().sequence(), note.hash());
            next.publish(directory.resolve("ACTIVE"), io);
            active = next;
        } finally {
            gate.unlock();
        }
    }

    /**
     * When the Ratis status directory is missing or lagging, separate durable replica can be used to re-distribut the installed snapshot.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    Path pendingBundle() throws IOException {
        return supportsSnapshots() && Files.exists(directory.resolve("INSTALLING")) ? work(checkedRecord("INSTALLING")).resolve("snapshot.bundle") : null;
    }

    /**
     * The position in the durable record is checked by a status machine and cannot guess business commit serial numbers from the file name.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    TermIndex pendingPosition() throws IOException {
        var plan = checkedRecord("INSTALLING");
        return TermIndex.valueOf(plan.term(), plan.index());
    }

    /**
     * Full recovery new space, reissue active pointer; late failure cannot return to the old generation service.
     *
     * @param plan plan
     * @throws IOException if the operation cannot complete
     */
    private void complete(final RatisGenerationRecord plan) throws IOException {
        closeCore();
        Path bundle = work(plan).resolve("snapshot.bundle");
        if (!hash(bundle).equals(plan.hash())) {
            throw new IOException("durable installation bundle changed");
        }
        var position = TermIndex.valueOf(plan.term(), plan.index());
        Path backup = work(plan).resolve("backup");
        if (!Files.exists(backup)) {
            RatisSnapshotBundle.extract(bundle, backup, RaftGroupId.valueOf(identity.groupId()), position, limits);
        }
        var info = WalBackup.inspect(backup, limits);
        if (info.sequence() != plan.sequence()) {
            throw new IOException("installation business sequence changed");
        }
        Path generation = generation(plan);
        ensureGeneration(plan);
        lease = factory.open(identity, plan.generation());
        validateLease();
        Path wal = generation.resolve("wal");
        if (!Files.exists(wal) || Files.exists(wal.resolve("RESTORING"))) {
            WalBackup.restoreReplica(backup, wal, lease.stores(), options, limits, identity);
        }
        hook.hit(Stage.AFTER_RESTORE);
        core = WalMvccRuntime.openReplica(wal, shards, lease.stores(), options, WalRetentionOptions.preserveAll(), identity, authority);
        if (core.stats().publishedSequence() != plan.sequence()) {
            throw new IOException("installed generation boundary mismatch");
        }
        hook.hit(Stage.BEFORE_ACTIVE);
        plan.publish(directory.resolve("ACTIVE"));
        active = plan;
        hook.hit(Stage.AFTER_ACTIVE);
        RatisGenerationRecord.remove(directory.resolve("INSTALLING"));
        hook.hit(Stage.AFTER_CLEARED);
    }

    /**
     * Generation must be available when it is opened. The missing directory cannot be used as a hidden initial.
     *
     * @param record record
     * @param fresh fresh
     * @throws IOException if the operation cannot complete
     */
    private void openCore(final RatisGenerationRecord record, final boolean fresh) throws IOException {
        Path generation = generation(record);
        Path wal = generation.resolve("wal");
        ensureNotRetired(record);
        if (!fresh && !Files.isRegularFile(wal.resolve("REPLICA"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("active generation data missing");
        }
        Files.createDirectories(generation);
        RatisGenerationRecord.forceDirectory(directory);
        if (fresh) {
            ensureGeneration(record);
        }
        // Backend opening failure may still hold a pool and space protection, leaving clean-up responsibilities open without creating the same generation.
        if (pendingBackend != null) {
            pendingBackend.retryCleanup();
            pendingBackend = null;
        }
        try {
            lease = factory.open(identity, record.generation());
        } catch (final RatisStartupException failure) {
            pendingBackend = failure;
            throw failure;
        }
        validateLease();
        core = WalMvccRuntime.openReplica(wal, shards, lease.stores(), options, WalRetentionOptions.preserveAll(), identity, authority);
        if (core.stats().publishedSequence() < record.sequence()) {
            throw new IOException("active generation lost snapshot prefix");
        }
        if (!fresh) {
            ensureGeneration(record);
        }
    }

    /**
     * This round is verify 's current generation refillable title tag and the unknown old directory does not automatically take over.
     *
     * @param record record
     * @throws IOException if the operation cannot complete
     */
    private void ensureGeneration(final RatisGenerationRecord record) throws IOException {
        ensureNotRetired(record);
        Path path = generation(record);
        if (Files.isSymbolicLink(path)) {
            throw new IOException("generation directory is symbolic");
        }
        Files.createDirectories(path);
        Path marker = path.resolve("GENERATION");
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            verifyOwner(RatisGenerationRecord.read(marker), record.generation());
        } else {
            new RatisGenerationRecord(identity, record.generation(), -1, -1, 0, "").publish(marker);
        }
        RatisGenerationRecord.forceDirectory(directory);
    }

    /**
     * Neither local tombstone nor the object that is retirement can be re-serving generation.
     *
     * @param record record
     * @throws IOException if the operation cannot complete
     */
    private void ensureNotRetired(final RatisGenerationRecord record) throws IOException {
        if (Files.exists(directory.resolve("retired").resolve(record.generation().toString()), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("generation already retired");
        }
        Path pending = directory.resolve("RETIRING");
        if (Files.exists(pending, LinkOption.NOFOLLOW_LINKS) && RatisRetirementRecord.read(pending).target().generation().equals(record.generation())) {
            throw new IOException("generation retirement in progress");
        }
    }

    /**
     * One round to advance only one old object and keep the current generation and current installation input, all deletions having actual budget.
     *
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    void retire(final RatisRetirementWork work) throws IOException {
        if (!supportsSnapshots() || !factory.supportsRetirement()) {
            throw new IOException("node backend cannot retire safely");
        }
        if (!gate.tryLock()) {
            throw new IOException("node state maintenance busy");
        }
        var alive = new java.util.concurrent.atomic.AtomicBoolean(true);
        try {
            if (!ready || closed || core == null || core.lifecycle().fatal() != null) {
                throw new IOException("node not ready for retirement");
            }
            RatisRetirementFiles.scratch(directory, false, work);
            RatisRetirementFiles.scratch(directory.resolve("retired"), false, work);
            Path journal = directory.resolve("RETIRING");
            RatisRetirementRecord progress;
            if (Files.exists(journal, LinkOption.NOFOLLOW_LINKS)) {
                progress = RatisRetirementRecord.read(journal);
                verifyOwner(progress.target(), progress.target().generation());
            } else {
                var target = selectRetirement(work);
                if (target == null) {
                    return;
                }
                verifyRetirable(target.generation());
                checkTombstoneBudget(work);
                progress = new RatisRetirementRecord(target, "", false);
                progress.publish(journal);
                retirementHook.hit(RetirementStage.AFTER_RECORD);
            }
            final var target = progress.target();
            var permit = new WalGenerationRetirement(identity, target.generation(), () -> {
                if (!alive.get() || !gate.isHeldByCurrentThread() || lock == null || !lock.isValid() || closed || !ready) {
                    throw new IOException("retirement permit no longer valid");
                }
                work.check();
                verifyRetirable(target.generation());
            });
            permit.verify();
            while (!progress.backendDone() && work.inspected < work.options.backendShardLimit()) {
                var backend = factory.retire(permit, progress.cursor(), 1);
                work.inspected += backend.inspected();
                work.dropped += backend.dropped();
                progress = new RatisRetirementRecord(target, backend.cursor(), backend.complete());
                progress.publish(journal);
                retirementHook.hit(RetirementStage.AFTER_BACKEND_PROGRESS);
                if (backend.inspected() == 0 && !backend.complete()) {
                    throw new IOException("backend retirement made no progress");
                }
            }
            if (!progress.backendDone()) {
                return;
            }
            var deletion = (RatisRetirementFiles.Hook) path -> retirementHook.hit(RetirementStage.AFTER_FILE_DELETE);
            permit.verify();
            if (!RatisRetirementFiles.clean(generation(target), true, work, deletion)) {
                return;
            }
            permit.verify();
            if (!RatisRetirementFiles.clean(work(target), false, work, deletion)) {
                return;
            }
            Path tombstones = directory.resolve("retired");
            if (Files.isSymbolicLink(tombstones)) {
                throw new IOException("retired catalog is symbolic");
            }
            Files.createDirectories(tombstones);
            Path tombstone = tombstones.resolve(target.generation().toString());
            if (Files.exists(tombstone)) {
                verifyOwner(RatisGenerationRecord.read(tombstone), target.generation());
            } else {
                target.publish(tombstone);
            }
            retirementHook.hit(RetirementStage.AFTER_TOMBSTONE);
            work.deleting();
            RatisGenerationRecord.remove(journal);
            work.removed++;
            work.completed++;
        } catch (final RatisRetirementWork.Limit exhausted) {
            work.exhausted = true;
        } finally {
            alive.set(false);
            gate.unlock();
        }
    }

    /**
     * The durable pointer protection collection is used every time and cannot be deleted from the expired candidate list.
     *
     * @param generation generation
     * @throws IOException if the operation cannot complete
     */
    private void verifyRetirable(final java.util.UUID generation) throws IOException {
        var current = checkedRecord("ACTIVE");
        if (current.generation().equals(generation)) {
            throw new IOException("cannot retire active generation");
        }
        Path installing = directory.resolve("INSTALLING");
        if (Files.exists(installing) && checkedRecord("INSTALLING").generation().equals(generation)) {
            throw new IOException("cannot retire installing generation");
        }
        if (Files.exists(directory.resolve("BOOTSTRAPPING"))) {
            throw new IOException("cannot retire during bootstrap");
        }
    }

    /**
     * Scan a limited root directory, with candidates only from protocol catalogues with fixed title tags.
     *
     * @param work work
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisGenerationRecord selectRetirement(final RatisRetirementWork work) throws IOException {
        var candidates = new java.util.TreeMap<String, RatisGenerationRecord>();
        try (var entries = Files.list(directory)) {
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                work.scan();
                Path path = iterator.next();
                String name = path.getFileName().toString();
                if (!name.matches("(generation|install)-[a-f0-9-]{36}")) {
                    continue;
                }
                java.util.UUID generation = java.util.UUID.fromString(name.substring(name.indexOf('-') + 1));
                if (generation.equals(active.generation())) {
                    continue;
                }
                if (Files.exists(directory.resolve("INSTALLING")) && checkedRecord("INSTALLING").generation().equals(generation)) {
                    continue;
                }
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("retirement candidate is symbolic");
                }
                Path marker = path.resolve(name.startsWith("generation-") ? "GENERATION" : "INSTALLATION");
                var owner = RatisGenerationRecord.read(marker);
                verifyOwner(owner, generation);
                candidates.put(generation.toString(), owner);
            }
        }
        return candidates.isEmpty() ? null : candidates.firstEntry().getValue();
    }

    /**
     * The metadata tombstone has a limited total and is allowed to revive after the upper limit back pressure instead of removing history.
     *
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    private void checkTombstoneBudget(final RatisRetirementWork work) throws IOException {
        Path retired = directory.resolve("retired");
        if (!Files.exists(retired)) {
            return;
        }
        if (Files.isSymbolicLink(retired)) {
            throw new IOException("retired catalog is symbolic");
        }
        int count = 0;
        try (var entries = Files.list(retired)) {
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                work.scan();
                iterator.next();
                if (++count >= 4096) {
                    throw new IOException("local retirement tombstone budget exhausted");
                }
            }
        }
    }

    /**
     * Ownership covers the whole node and complete generation UUID, not authorized by a folder short name.
     *
     * @param record record
     * @param generation generation
     * @throws IOException if the operation cannot complete
     */
    private void verifyOwner(final RatisGenerationRecord record, final java.util.UUID generation) throws IOException {
        if (!record.identity().equals(identity) || !record.generation().equals(generation)) {
            throw new IOException("retirement ownership mismatch");
        }
    }

    /**
     * The protection assembly is within the same generation lock as the actual clean-up, and the installation cannot change the protective boundary after sampling.
     *
     * @param work work
     * @param cleaner cleaner
     * @throws IOException if the operation cannot complete
     */
    void cleanupSnapshots(final RatisRetirementWork work, final SnapshotCleaner cleaner) throws IOException {
        if (!gate.tryLock()) {
            throw new IOException("node snapshot maintenance busy");
        }
        try {
            work.check();
            if (!ready || closed) {
                throw new IOException("node not ready for snapshot maintenance");
            }
            var protectedIndices = new java.util.HashSet<Long>();
            if (active != null && active.index() >= 0) {
                protectedIndices.add(checkedRecord("ACTIVE").index());
            }
            if (Files.exists(directory.resolve("INSTALLING"))) {
                protectedIndices.add(checkedRecord("INSTALLING").index());
            }
            cleaner.clean(work, java.util.Set.copyOf(protectedIndices));
        } catch (final RatisRetirementWork.Limit exhausted) {
            work.exhausted = true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * Unfinished retirement is an observable state and the budget expires does not drop a continuation object.
     *
     * @return the operation result
     */
    boolean retirementPending() {
        return supportsSnapshots() && Files.exists(directory.resolve("RETIRING"));
    }

    /**
     * Back-end capability, route, reliable durable storage and limited timeout are checked before import or initialization.
     *
     * @throws IOException if the operation cannot complete
     */
    private void validateLease() throws IOException {
        if (!shards.containsAll(lease.stores().keySet()) || lease.stores().values().stream().anyMatch(store -> !store.supportsReplicaOwnership() || !store.supportsReplicaRestore())) {
            throw new IOException("generation backend capabilities mismatch");
        }
        for (var store : lease.stores().values()) {
            if (boundedGcRequired && !store.supportsBoundedMaintenance()) {
                throw new IOException("new generation cannot perform configured bounded version GC");
            }
            if (pinnedSnapshotsRequired && !store.supportsPinnedSnapshot()) {
                throw new IOException("new generation cannot pin configured automatic snapshots");
            }
            store.validateProduction();
        }
    }

    /**
     * The metadata must match the current receiving node and cannot clone a business root directory for another node.
     *
     * @param name name
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisGenerationRecord checkedRecord(final String name) throws IOException {
        var record = RatisGenerationRecord.read(directory.resolve(name));
        if (!record.identity().equals(identity)) {
            throw new IOException("generation node identity mismatch");
        }
        return record;
    }

    /**
     * The private operating directories and the generation directory can only be created by a fixed UUID.
     *
     * @param plan plan
     * @return the operation result
     */
    private Path work(final RatisGenerationRecord plan) {
        return directory.resolve("install-" + plan.generation());
    }

    /**
     * Database or local directory names provided from a remote location are not accepted.
     *
     * @param plan plan
     * @return the operation result
     */
    private Path generation(final RatisGenerationRecord plan) {
        return directory.resolve("generation-" + plan.generation());
    }

    /**
     * Copy the received file to a stand-alone durable operation directory, avoiding subsequent Ratis replacement status directories dropping and continuing input.
     *
     * @param source database connection source
     * @param target target
     * @throws IOException if the operation cannot complete
     */
    private void copyBundle(final Path source, final Path target) throws IOException {
        if (Files.size(source) > limits.maxBytes()) {
            throw new IOException("installation bundle exceeds budget");
        }
        try (
                var input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                var output = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            var buffer = ByteBuffer.allocate(65_536);
            long bytes = 0;
            long started = System.nanoTime();
            for (int read; (read = input.read(buffer)) >= 0;) {
                if (read == 0 || (bytes += read) > limits.maxBytes() || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) {
                    throw new IOException("installation copy exceeds budget or made no progress");
                }
                buffer.flip();
                RatisGenerationRecord.write(output, buffer);
                buffer.clear();
            }
            hook.hit(Stage.BEFORE_INPUT_FORCE);
            output.force(true);
        }
        RatisGenerationRecord.forceDirectory(target.getParent());
    }

    /**
     * Fixed memory check complete installed input, and source summary changes cannot continue the original operation.
     *
     * @param path path
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private String hash(final Path path) throws IOException {
        try {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > limits.maxBytes()) {
                throw new IOException("installation bundle missing or exceeds budget");
            }
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            var buffer = new byte[65_536];
            long started = System.nanoTime();
            try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
                for (int read; (read = input.read(buffer)) != -1;) {
                    if (read == 0 || System.nanoTime() - started > java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limits.timeoutMillis())) {
                        throw new IOException("installation checksum exceeds budget or made no progress");
                    }
                    digest.update(buffer, 0, read);
                }
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (final java.security.NoSuchAlgorithmException failure) {
            throw new IOException(failure);
        }
    }

    /**
     * Drop the core first and then close connection pool. Failed to keep the enantio; old data is kept for subsequent security retirement.
     *
     * @throws IOException if the operation cannot complete
     */
    private void closeCore() throws IOException {
        var previous = core;
        if (previous != null) {
            previous.fenceEpoch(Long.MAX_VALUE);
            previous.close();
            core = null;
        }
        if (pendingBackend != null) {
            pendingBackend.retryCleanup();
            pendingBackend = null;
        }
        var resource = lease;
        if (resource != null) {
            resource.close();
            lease = null;
        }
    }

    /**
     * Both the original stationary and generation configurations only shut off their own resources, locking in and finally releasing them.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        gate.lock();
        try {
            if (closed) {
                return;
            }
            ready = false;
            // Keep generation locks with lease when kernel shutdown failed. The background writer and the new process cannot use backend at the same time.
            closeCore();
            if (lock != null) {
                lock.release();
                lock = null;
            }
            if (lockChannel != null) {
                lockChannel.close();
                lockChannel = null;
            }
            if (directory != null) {
                LOCAL.remove(directory);
            }
            closed = true;
        } finally {
            gate.unlock();
        }
    }

    /**
     * The observable installation boundary is open only to failure tests in the package.
     */
    enum Stage {
        BEFORE_INPUT_FORCE, AFTER_INSTALL_RECORD, AFTER_RESTORE, BEFORE_ACTIVE, AFTER_ACTIVE, AFTER_CLEARED,
        AFTER_IMAGE_PREPARED, AFTER_IMAGE_COMPLETE, AFTER_IMAGE_PUBLISHED, BEFORE_IMAGE_ACTIVE, AFTER_IMAGE_ACTIVE, BEFORE_LOG_PURGE, AFTER_LOG_PURGE,
        BEFORE_READY_DIRECTORY_FORCE, AFTER_READY_DIRECTORY_FORCE, BEFORE_COMMIT_APPEND, BEFORE_COMMIT_APPLIED, AFTER_COMMIT_APPLIED
    }

    /**
     * The failure border of the real retirement progress package.
     */
    enum RetirementStage {
        AFTER_RECORD, AFTER_BACKEND_PROGRESS, AFTER_FILE_DELETE, AFTER_TOMBSTONE
    }

    /**
     * The test injection only occurs after the actual operation.
     */
    @FunctionalInterface
    interface RetirementHook {

        /**
         * Interrupted injection at durable retirement border.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        void hit(RetirementStage stage) throws IOException;
    }

    /**
     * The failure test is not a substitute for the brushing and release of real files.
     */
    @FunctionalInterface
    interface Hook {

        /**
         * Injection failed after actual borders.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        void hit(Stage stage) throws IOException;

        /**
         * Log collection carries the actual primary index and covers the upper boundary, while the normal assembly still uses empty hooks.
         *
         * @param stage stage
         * @param firstIndex first index
         * @param coveredIndex covered index
         * @throws IOException if the operation cannot complete
         */
        default void hit(final Stage stage, final long firstIndex, final long coveredIndex) throws IOException {
            hit(stage);
        }
    }

    /**
     * Collection is the same fixed generation indicator and cannot close this generation connection pool during the collection process.
     *
     * @param progress progress
     * @param stats stats
     * @param generation generation
     */
    record GcSample(com.flyingzc.azir.wal.WalMaterializedStore.GcProgress progress, WalMvccRuntime.Stats stats, java.util.UUID generation) {
    }

    /**
     * Storage samples from the hosting observations are fixed with actual generation, and cache consumers do not mix up before-and-after-generation counts.
     *
     * @param stats stats
     * @param generation generation
     */
    record LocalSample(WalMvccRuntime.Stats stats, java.util.UUID generation) {
    }

    /**
     * Background calculates a full summary of the document to be signed with the immutable file, and only the signature is verified during the short release phase.
     *
     * @param header header
     * @param bundle bundle
     * @param hash hash
     * @param fileKey file key
     * @param bytes bytes
     * @param modified modified
     */
    record SnapshotNote(RatisSnapshotBundle.Header header, Path bundle, String hash, Object fileKey, long bytes, java.nio.file.attribute.FileTime modified) {
    }

    /**
     * Clearing the callback can only run within the generation lock, and snapshot storage cannot reverse to get the generation lock.
     */
    @FunctionalInterface
    interface SnapshotCleaner {

        /**
         * Execute bounded file clean-up in a fixed protection group.
         *
         * @param work work
         * @param protectedIndices protected indices
         * @throws IOException if the operation cannot complete
         */
        void clean(RatisRetirementWork work, java.util.Set<Long> protectedIndices) throws IOException;
    }
}
