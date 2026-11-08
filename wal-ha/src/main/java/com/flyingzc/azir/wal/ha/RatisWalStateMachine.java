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

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalReplicaBackup;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalReplicationBatch;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;

import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.LifeCycle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Only real Raft commit batch is applied, actual installation and non-business index updates with the same bounded sequence queue.
 */
final class RatisWalStateMachine extends BaseStateMachine {

    private final WalReplicaIdentity identity;

    private final RatisWalState state;

    private final RatisWalNode.Authority authority;

    private final java.util.concurrent.ExecutorService apply;

    private final java.util.concurrent.ExecutorService probes;

    private final java.util.concurrent.ExecutorService images;

    private final Object imageGuard = new Object();

    private CompletableFuture<Long> imageJob;

    private volatile Throwable applicationFailure;

    private final java.util.function.BooleanSupplier eligible;

    private volatile long probeMillis = 1000;

    private final Object budget = new Object();

    private long queuedBytes;

    private int queuedTasks;

    private volatile boolean closed;

    private final WalBackupOptions limits;

    private final RatisSnapshotStorage snapshots;

    private final java.util.concurrent.atomic.AtomicLong durableAckIndex = new java.util.concurrent.atomic.AtomicLong(-1);

    /**
     * A platform installer avoids a disk waiting to fix all virtual carriers, queuing for up to 32 MiB and 64 missions.
     *
     * @param identity identity
     * @param state state
     * @param authority authority
     * @param limits limits
     */
    RatisWalStateMachine(final WalReplicaIdentity identity, final RatisWalState state, final RatisWalNode.Authority authority, final WalBackupOptions limits) {
        this(identity, state, authority, limits, () -> true);
    }

    /**
     * Qualifications for diagnosis are derived from the health strategy of this node, and back-end detection is performed on an independent line.
     *
     * @param identity identity
     * @param state state
     * @param authority authority
     * @param limits limits
     * @param eligible eligible
     */
    RatisWalStateMachine(final WalReplicaIdentity identity, final RatisWalState state, final RatisWalNode.Authority authority, final WalBackupOptions limits,
                         final java.util.function.BooleanSupplier eligible) {
        this.identity = identity;
        this.state = state;
        this.authority = authority;
        this.limits = limits;
        this.eligible = eligible;
        snapshots = state.supportsSnapshots() ? new RatisSnapshotStorage(RaftGroupId.valueOf(identity.groupId()), limits) : null;
        apply = Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-raft-apply-" + identity.nodeId()).factory());
        images = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1),
                Thread.ofPlatform().name("wal-snapshot-image-" + identity.nodeId()).factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        probes = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new java.util.concurrent.ArrayBlockingQueue<>(1),
                Thread.ofPlatform().name("wal-health-probe-" + identity.nodeId()).factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * The start-up requires recovery durable installation operations and complete mirrors before the status machine can be run.
     *
     * @param server server
     * @param group group
     * @param storage storage
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void initialize(final RaftServer server, final RaftGroupId group, final RaftStorage storage) throws IOException {
        getLifeCycle().transition(LifeCycle.State.STARTING);
        super.initialize(server, group, storage);
        if (snapshots != null) {
            RaftDurability.initialize(storage);
            snapshots.init(storage);
            restoreLatest();
        }
        getLifeCycle().transition(LifeCycle.State.RUNNING);
    }

    /**
     * Generation assembly returns real snapshot storage and the original fixed backend continues to be unsupported by snapshot.
     *
     * @return the operation result
     */
    @Override
    public StateMachineStorage getStateMachineStorage() {
        return snapshots == null ? super.getStateMachineStorage() : snapshots;
    }

    /**
     * The suspension was withdrawn immediately and the old kernel was shut down in the order of its practical application to meet the life cycle of Ratis installation.
     *
     * @throws java.util.concurrent.CompletionException if the operation cannot complete
     */
    @Override
    public void pause() {
        state.stopServing();
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        try {
            drainImage();
            await(enqueue(1, () -> {
                state.pause();
                return Message.valueOf("PAUSED");
            }));
            getLifeCycle().transition(LifeCycle.State.PAUSED);
        } catch (final IOException failure) {
            throw new java.util.concurrent.CompletionException(failure);
        }
    }

    /**
     * Sync, validate and install a new generation after the arrival of the new status directory, which will last update the application position and life cycle.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void reinitialize() throws IOException {
        if (snapshots == null) {
            throw new IOException("fixed backend node cannot install snapshots");
        }
        getLifeCycle().transition(LifeCycle.State.STARTING);
        restoreLatest();
        applicationFailure = null;
        getLifeCycle().transition(LifeCycle.State.RUNNING);
    }

    /**
     * Finish the snapshot file that can be done independently, then move from the top position of verify to recovery Operations generation.
     *
     * @throws IOException if the operation cannot complete
     */
    private void restoreLatest() throws IOException {
        var latest = snapshots.reload();
        Path pending = state.pendingBundle();
        if (pending != null) {
            var position = state.pendingPosition();
            if (latest == null || latest.getIndex() < position.getIndex()) {
                latest = snapshots.publish(pending, position);
            }
        }
        if (latest == null) {
            state.initialize(null, null);
        } else {
            latest = snapshots.syncReceived();
            state.initialize(latest.getFile().getPath(), latest.getTermIndex());
            setLastAppliedTermIndex(latest.getTermIndex());
        }
    }

    /**
     * GRPC successfully checked and force is currently receiving mirrors to ensure independent recovery input and operation durable storage.
     *
     * @param acknowledged acknowledged
     * @throws IOException if the operation cannot complete
     */
    void durableSnapshotReceived(final TermIndex acknowledged) throws IOException {
        if (snapshots == null) {
            throw new IOException("fixed backend node cannot acknowledge installed snapshots");
        }
        var latest = snapshots.syncReceived();
        if (latest.getIndex() < acknowledged.getIndex() || latest.getIndex() == acknowledged.getIndex() && latest.getTerm() != acknowledged.getTerm()) {
            throw new IOException("received snapshot does not cover ACK position");
        }
        RaftDurability.current(getServer().join(), getGroupId());
        state.durableBeforeSnapshotAck(latest.getFile().getPath(), latest.getTermIndex());
        durableAckIndex.accumulateAndGet(latest.getIndex(), Math::max);
    }

    /**
     * Only updates after the ACK durable sex check has actually been completed can not be impersonated as receiving durable confirmation with application position.
     *
     * @return the operation result
     */
    long durableSnapshotAckIndex() {
        return durableAckIndex.get();
    }

    /**
     * Collection is preceded by re- verify and syncs the current full image. The log cannot be deleted with an old cache index alone.
     *
     * @param required required
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    long durableSnapshotBoundary(final long required) throws IOException {
        if (snapshots == null || failed()) {
            throw new IOException("snapshot unavailable for purge");
        }
        var complete = snapshots.syncReceived();
        if (complete.getIndex() < required || !state.activatedSnapshotCovers(required)) {
            throw new IOException("activated durable snapshot does not cover purge");
        }
        return complete.getIndex();
    }

    /**
     * Maintains only cleaning complete expired files in snapshot directories without advancing or deleting the Consensus Log.
     *
     * @param work work
     * @param protectedIndices protected indices
     * @throws IOException if the operation cannot complete
     */
    void cleanupSnapshots(final RatisRetirementWork work, final java.util.Set<Long> protectedIndices) throws IOException {
        if (snapshots != null) {
            snapshots.cleanup(work, protectedIndices);
        }
    }

    /**
     * The mirror release skips file maintenance while holding the snapshot storage lock, avoiding prolonged obstruction of health detection after holding the generation lock.
     *
     * @param work work
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    boolean maintainFiles(final RatisRetirementWork work) throws IOException {
        synchronized (imageGuard) {
            if (imageJob != null && !imageJob.isDone()) {
                return false;
            }
            cleanupBuilds(work);
            if (!work.exhausted) {
                state.cleanupSnapshots(work, this::cleanupSnapshots);
            }
            if (!work.exhausted) {
                state.retire(work);
            }
            return true;
        }
    }

    /**
     * The stept installation is not yet over, the follow-up ACK must also ensure its snapshot base recovery first.
     *
     * @throws IOException if the operation cannot complete
     */
    void durableSnapshotBaseForAppend() throws IOException {
        if (state.ready()) {
            return;
        }
        if (snapshots == null) {
            throw new IOException("fixed backend not ready for append ACK");
        }
        long required = getServer().join().getDivision(getGroupId()).getRaftLog().getSnapshotIndex();
        if (required < 0) {
            return;
        }
        var latest = snapshots.syncReceived();
        if (latest.getIndex() < required) {
            throw new IOException("append ACK snapshot base missing");
        }
        state.durableBeforeSnapshotAck(latest.getFile().getPath(), latest.getTermIndex());
    }

    /**
     * A fixed applied border and back-end reading view background is exported, with a maximum of one full mirror at the same time.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    CompletableFuture<Long> requestSnapshot() throws IOException {
        if (snapshots == null) {
            throw new IOException("snapshot requires generation backend factory");
        }
        synchronized (imageGuard) {
            if (closed) {
                throw new IOException("snapshot state machine closed");
            }
            if (imageJob == null || imageJob.isDone()) {
                imageJob = new CompletableFuture<>();
                var started = imageJob;
                try {
                    images.execute(() -> {
                        try {
                            started.complete(buildImage());
                        } catch (final Throwable failure) {
                            started.completeExceptionally(failure);
                        }
                    });
                } catch (final RuntimeException rejected) {
                    started.completeExceptionally(rejected);
                }
            }
            return imageJob;
        }
    }

    /**
     * Synchronizes the access to only one background task, and timeout does not cancel the shared I/O that is being implemented.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    @Override
    public long takeSnapshot() throws IOException {
        var job = requestSnapshot();
        try {
            return job.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("snapshot wait interrupted; inspect actual progress", interrupted);
        } catch (final java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException unresolved) {
            throw new IOException("snapshot failed or unresolved", unresolved);
        }
    }

    /**
     * Large database Export, Encapsulation and Content validate Leave the application thread and recheck the current generation before posting.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    private long buildImage() throws Exception {
        // The last time the name was published and subsequent synchronous errors were reported, they were reloaded at mirror threads and no large files could be scanned at application threads.
        snapshots.reload();
        var cut = new java.util.concurrent.atomic.AtomicReference<ImageCut>();
        var existing = new java.util.concurrent.atomic.AtomicReference<ExistingCut>();
        var preparation = enqueue(1, () -> {
            if (closed || !state.ready() || failed()) {
                throw new IOException("business generation not ready for snapshot");
            }
            var position = getLastAppliedTermIndex();
            if (position == null || position.getTerm() < 1 || position.getIndex() < 0) {
                return Message.valueOf("NO_SNAPSHOT");
            }
            var current = snapshots.getLatestSnapshot();
            if (current != null && current.getIndex() >= position.getIndex()) {
                if (current.getIndex() > position.getIndex()) {
                    throw new IOException("snapshot is ahead of applied generation");
                }
                existing.set(new ExistingCut(state.core(), current.getTermIndex()));
                return Message.valueOf("EXISTING_SNAPSHOT");
            }
            var configuration = getServer().join().getDivision(getGroupId()).getRaftConf();
            var proto = RaftProtos.RaftConfigurationProto.newBuilder();
            configuration.getCurrentPeers(RaftProtos.RaftPeerRole.FOLLOWER).forEach(peer -> proto.addPeers(peer.getRaftPeerProto()));
            configuration.getPreviousPeers(RaftProtos.RaftPeerRole.FOLLOWER).forEach(peer -> proto.addOldPeers(peer.getRaftPeerProto()));
            configuration.getCurrentPeers(RaftProtos.RaftPeerRole.LISTENER).forEach(peer -> proto.addListeners(peer.getRaftPeerProto()));
            configuration.getPreviousPeers(RaftProtos.RaftPeerRole.LISTENER).forEach(peer -> proto.addOldListeners(peer.getRaftPeerProto()));
            var buildId = java.util.UUID.randomUUID();
            Path work = snapshots.getSnapshotDir().toPath().resolve(".build-" + buildId);
            Files.createDirectory(work);
            new RatisGenerationRecord(identity, buildId, -1, -1, 0, "").publish(work.resolve("BUILD"));
            RatisGenerationRecord.forceDirectory(work.getParent());
            var core = state.core();
            try {
                WalReplicaBackup pinned = null;
                if (state.supportsPinnedSnapshots()) {
                    pinned = core.prepareReplicaBackup(authority, work.resolve("backup"), limits);
                } else {
                    core.backupReplica(authority, work.resolve("backup"), limits);
                } // The old custom SPI manual mirror retains a serial contract.
                cut.set(new ImageCut(core, position, configuration.getLogEntryIndex(), proto.build(), work, pinned));
            } catch (final Throwable failure) {
                try {
                    discardBuild(work);
                } catch (final Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            return Message.valueOf("SNAPSHOT_PREPARED");
        });
        try {
            await(preparation);
        } catch (final IOException abandoned) {
            // Observation timeout does not cancel the application thread; if it is ready for subsequent completion, it completes the callback to close all fixed reading views.
            preparation.whenComplete((ignored, failure) -> {
                var orphan = cut.getAndSet(null);
                if (orphan == null) {
                    return;
                }
                try {
                    if (orphan.backup() != null) {
                        orphan.backup().close();
                    }
                } catch (final Exception cleanup) {
                    abandoned.addSuppressed(cleanup);
                }
                try {
                    discardBuild(orphan.work());
                } catch (final Exception cleanup) {
                    abandoned.addSuppressed(cleanup);
                }
            });
            throw abandoned;
        }
        var fixed = cut.get();
        if (fixed == null) {
            var previous = existing.get();
            return previous == null ? -1 : activateExisting(previous);
        }
        try (var backup = fixed.backup()) {
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_PREPARED);
            if (backup != null) {
                backup.complete();
            }
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_COMPLETE);
            Path bundle = fixed.work().resolve("snapshot.bundle");
            var header = RatisSnapshotBundle.pack(fixed.work().resolve("backup"), bundle, getGroupId(), fixed.position(), fixed.configurationIndex(), fixed.configuration(), limits);
            // Copying the full mirror does not constitute an application queue, except for short release of the active pointer, which is executed again in the application order.
            if (closed || !state.ready() || failed() || state.core() != fixed.core()) {
                throw new IOException("snapshot generation changed during export");
            }
            var published = snapshots.publish(bundle, fixed.position());
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_PUBLISHED);
            var note = state.prepareSnapshotNote(header, published.getFile().getPath());
            await(enqueue(1, () -> {
                if (closed || !state.ready() || failed() || state.core() != fixed.core()) {
                    throw new IOException("snapshot generation changed before activation");
                }
                state.lifecycleStage(RatisWalState.Stage.BEFORE_IMAGE_ACTIVE);
                state.noteSnapshot(note);
                state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_ACTIVE);
                return Message.valueOf("SNAPSHOT");
            }));
            return fixed.position().getIndex();
        } finally {
            discardBuild(fixed.work());
        }
    }

    /**
     * A mirror already exists to reconfirm durable sex and complete ACTIVE. The last activation step cannot be omitted from the location.
     *
     * @param fixed fixed
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private long activateExisting(final ExistingCut fixed) throws IOException {
        var current = snapshots.syncReceived();
        if (!current.getTermIndex().equals(fixed.position())) {
            throw new IOException("snapshot changed during activation retry");
        }
        var header = RatisSnapshotBundle.inspect(current.getFile().getPath(), getGroupId(), current.getTermIndex(), limits);
        var note = state.prepareSnapshotNote(header, current.getFile().getPath());
        await(enqueue(1, () -> {
            if (closed || !state.ready() || failed() || state.core() != fixed.core() || getLastAppliedTermIndex().getIndex() < fixed.position().getIndex()) {
                throw new IOException("generation changed during snapshot activation retry");
            }
            state.lifecycleStage(RatisWalState.Stage.BEFORE_IMAGE_ACTIVE);
            state.noteSnapshot(note);
            state.lifecycleStage(RatisWalState.Stage.AFTER_IMAGE_ACTIVE);
            return Message.valueOf("SNAPSHOT_REACTIVATED");
        }));
        return fixed.position().getIndex();
    }

    /**
     * The installation of a remote snapshot previously awaited the exit of this mirror and the application of a thread to keep it available for completion or rejection of its release.
     *
     * @throws IOException if the operation cannot complete
     */
    private void drainImage() throws IOException {
        CompletableFuture<Long> job;
        synchronized (imageGuard) {
            job = imageJob;
        }
        if (job == null || job.isDone()) {
            return;
        }
        try {
            job.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (final java.util.concurrent.ExecutionException cancelledByPause) {
            /*
             * The machine was exported and refused to release because the ready was withdrawn, and the resources were released by the mission.
             */ } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("snapshot drain interrupted", interrupted);
        } catch (final java.util.concurrent.TimeoutException running) {
            throw new IOException("snapshot export did not drain", running);
        }
    }

    /**
     * Only private temporary storage left by collection crash is allowed when no mirror task holds a construction tree.
     *
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    void cleanupBuilds(final RatisRetirementWork work) throws IOException {
        if (snapshots == null) {
            return;
        }
        synchronized (imageGuard) {
            if (imageJob != null && !imageJob.isDone()) {
                return;
            }
            try (var entries = Files.list(snapshots.getSnapshotDir().toPath())) {
                var iterator = entries.iterator();
                while (iterator.hasNext()) {
                    work.scan();
                    var root = iterator.next();
                    String name = root.getFileName().toString();
                    if (!name.matches("\\.build-[a-f0-9-]{36}")) {
                        continue;
                    }
                    if (!Files.isDirectory(root, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                        throw new IOException("snapshot build root is not private directory");
                    }
                    var id = java.util.UUID.fromString(name.substring(7));
                    Path marker = root.resolve("BUILD");
                    if (Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                        var owner = RatisGenerationRecord.read(marker);
                        if (!owner.identity().equals(identity) || !owner.generation().equals(id)) {
                            throw new IOException("snapshot build owner mismatch");
                        }
                        RatisRetirementFiles.cleanBuild(root, work);
                    } else {
                        // No data input was created before the tags were published and only empty directories and semi-written private records are accepted.
                        try (var children = Files.list(root)) {
                            var childIterator = children.iterator();
                            while (childIterator.hasNext()) {
                                work.scan();
                                var child = childIterator.next();
                                if (!child.getFileName().toString().matches("\\.record-[a-f0-9-]{36}") || !Files.isRegularFile(child, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                                    throw new IOException("unmarked snapshot build contains unknown input");
                                }
                            }
                        }
                        RatisRetirementFiles.cleanBuild(root, work);
                    }
                }
            } catch (final RatisRetirementWork.Limit exhausted) {
                work.exhausted = true;
            }
        }
    }

    /**
     * Only the private work tree generated this time has been cleared and snapshot and activity generation have been released on other paths.
     *
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    private static void discardBuild(final Path work) throws IOException {
        try (var paths = Files.walk(work)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
        RatisGenerationRecord.forceDirectory(work.getParent());
    }

    /**
     * Waiting for maintenance will not cancel the shared channel that is brushing, and timeout remains currently installed.
     *
     * @param task task
     * @throws IOException if the operation cannot complete
     */
    private void await(final CompletableFuture<?> task) throws IOException {
        try {
            task.get(limits.timeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("state machine maintenance interrupted", failure);
        } catch (final java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            throw new IOException("state machine maintenance failed or unresolved", failure);
        }
    }

    /**
     * Business proposals are prepared by local decision and original web messages cannot be directly translated into business writing.
     *
     * @param request request
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    @Override
    public TransactionContext startTransaction(final RaftClientRequest request) throws IOException {
        byte[] data = request.getMessage().getContent().toByteArray();
        if (RatisReadFence.matches(data)) {
            authority.validate(RatisReadFence.decode(data), data);
        } else {
            authority.validate(WalReplicationBatch.decode(data));
        }
        return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request).setLogData(request.getMessage().getContent()).build();
    }

    /**
     * A strict log order of the validate term and a proposal to lose leadership after preparation must be rejected.
     *
     * @param transaction transaction context
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    @Override
    public TransactionContext preAppendTransaction(final TransactionContext transaction) throws IOException {
        byte[] data = transaction.getStateMachineLogEntry().getLogData().toByteArray();
        if (RatisReadFence.matches(data)) {
            authority.validate(RatisReadFence.decode(data), data);
        } else {
            state.lifecycleStage(RatisWalState.Stage.BEFORE_COMMIT_APPEND);
            authority.validate(WalReplicationBatch.decode(data));
        }
        return transaction;
    }

    /**
     * This recall will only receive a majority commit log, which will be copied to apply not to re-judge business conflicts.
     *
     * @param transaction transaction context
     * @return the operation result
     */
    @Override
    public CompletableFuture<Message> applyTransaction(final TransactionContext transaction) {
        var entry = transaction.getLogEntry();
        byte[] payload = entry.getStateMachineLogEntry().getLogData().toByteArray();
        return enqueue(payload.length, true, () -> {
            if (snapshots != null) {
                RaftDurability.current(getServer().join(), getGroupId());
            }
            if (RatisReadFence.matches(payload)) {
                var fence = RatisReadFence.decode(payload);
                if (!fence.origin().engineId().equals(identity.engineId()) || !fence.origin().groupId().equals(identity.groupId()) || fence.epoch() != entry.getTerm()) {
                    throw new IOException("committed read fence mismatch");
                }
                updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                return Message.valueOf("FENCE_COMMITTED");
            }
            var batch = WalReplicationBatch.decode(payload);
            if (!batch.origin().engineId().equals(identity.engineId()) || !batch.origin().groupId().equals(identity.groupId()) || batch.epoch() != entry.getTerm()) {
                throw new IOException("committed group or epoch mismatch");
            }
            // This Consensus Log is commit and has passed the majority durable validate, which test in the package can be delayed before the local release.
            state.lifecycleStage(RatisWalState.Stage.BEFORE_COMMIT_APPLIED);
            state.core().applyCommitted(authority, batch.commits());
            state.lifecycleStage(RatisWalState.Stage.AFTER_COMMIT_APPLIED);
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return Message.valueOf("APPLIED");
        });
    }

    /**
     * No-op and configuration records are also updated in the order in which they are actually applied, and cannot be crossed by a walk record that has not yet been installed.
     *
     * @param term term
     * @param index index
     */
    @Override
    public void notifyTermIndexUpdated(final long term, final long index) {
        enqueue(1, true, () -> {
            updateLastAppliedTermIndex(term, index);
            return Message.valueOf("INDEX");
        });
    }

    /**
     * The Query LinearizABLE configuration also provides a majority index after the previous practical application.
     *
     * @param request request
     * @return the operation result
     */
    @Override
    public CompletableFuture<Message> query(final Message request) {
        if (!RatisReplicaStatus.matches(request.getContent().toByteArray())) {
            return enqueue(1, () -> Message.valueOf("READ_BARRIER"));
        }
        return diagnostic(-1);
    }

    /**
     * The internal diagnosis fails clearly when the application boundary is not reached, and there are no unlimited registrations of waiting persons or queues for applications.
     *
     * @param request request
     * @param minimum minimum
     * @return the operation result
     */
    @Override
    public CompletableFuture<Message> queryStale(final Message request, final long minimum) {
        if (!RatisReplicaStatus.matches(request.getContent().toByteArray())) {
            return super.queryStale(request, minimum);
        }
        return diagnostic(minimum);
    }

    /**
     * Set the order watermark, then detect JDBC in an independent bounded queue, and finally recheck the position of generation with its application.
     *
     * @param minimum minimum
     * @return the operation result
     */
    private CompletableFuture<Message> diagnostic(final long minimum) {
        var sampled = new java.util.concurrent.atomic.AtomicReference<WalMvccRuntime>();
        return enqueue(1, () -> {
            if (minimum >= 0 && (getLastAppliedTermIndex() == null || getLastAppliedTermIndex().getIndex() < minimum)) {
                throw new IOException("replica has not applied diagnostic boundary");
            }
            if (state.ready()) {
                sampled.set(state.core());
            }
            return statusResponse();
        }).thenCompose(before -> {
            try {
                var cut = RatisReplicaStatus.decode(before.getContent().toByteArray());
                if (!cut.healthy()) {
                    return CompletableFuture.completedFuture(before);
                }
                return CompletableFuture.supplyAsync(() -> state.backendHealthy(sampled.get(), Math.min(probeMillis, limits.timeoutMillis())), probes)
                        .thenCompose(backend -> enqueue(1, () -> {
                            var after = RatisReplicaStatus.decode(statusResponse().getContent().toByteArray());
                            boolean unchanged = state.ready() && state.core() == sampled.get() && after.appliedIndex() >= cut.appliedIndex() && after.publishedSequence() >= cut.publishedSequence();
                            return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
                                    new RatisReplicaStatus(identity.groupId(), identity.nodeId(), after.appliedIndex(), after.publishedSequence(), after.healthy() && backend && unchanged).encode()));
                        }));
            } catch (final Exception failure) {
                return CompletableFuture.failedFuture(failure);
            }
        });
    }

    /**
     * Quick sequence sampling only observes local state and does not execute JDBC in log application threads.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private Message statusResponse() throws IOException {
        long index = getLastAppliedTermIndex() == null ? -1 : getLastAppliedTermIndex().getIndex();
        long published = 0;
        boolean healthy = false;
        if (state.ready()) {
            var lifecycle = state.core().lifecycle();
            published = state.core().stats().publishedSequence();
            healthy = eligible.getAsBoolean() && !failed() && lifecycle.accepting() && !lifecycle.closed() && lifecycle.fatal() == null && lifecycle.materializationFailure() == null
                    && !lifecycle.proposalBlocked();
        }
        return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new RatisReplicaStatus(identity.groupId(), identity.nodeId(), index, published, healthy).encode()));
    }

    /**
     * The current callback is only the old retirement local handle, and complete control threads and failure transfer are completed in subsequent integration.
     *
     * @param pendingEntries pending entries
     */
    @Override
    public void notifyNotLeader(final Collection<TransactionContext> pendingEntries) {
        authority.lostLeadership();
    }

    /**
     * The bounded queue does not hold an unlimited log message even if the follower is slow to store.
     *
     * @param bytes bytes
     * @param task task
     * @return the operation result
     */
    private CompletableFuture<Message> enqueue(final int bytes, final Work task) {
        return enqueue(bytes, false, task);
    }

    /**
     * The commit application is frozen after the watermark application failed, and the follow-up control log and no-op cannot skip the failure record.
     *
     * @param bytes bytes
     * @param application application
     * @param task task
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private CompletableFuture<Message> enqueue(final int bytes, final boolean application, final Work task) {
        var future = new CompletableFuture<Message>();
        boolean reserved = false;
        try {
            synchronized (budget) {
                while (!closed && (queuedTasks >= 64 || queuedBytes + bytes > 32L * 1024 * 1024)) {
                    budget.wait();
                }
                if (closed) {
                    throw new IOException("state machine closed");
                }
                queuedTasks++;
                queuedBytes += bytes;
                reserved = true;
            }
            apply.execute(() -> {
                try {
                    if (application && applicationFailure != null) {
                        throw new IOException("previous committed application failed", applicationFailure);
                    }
                    future.complete(task.run());
                } catch (final Throwable failure) {
                    if (application && applicationFailure == null) {
                        applicationFailure = failure;
                    }
                    future.completeExceptionally(failure);
                } finally {
                    synchronized (budget) {
                        queuedTasks--;
                        queuedBytes -= bytes;
                        budget.notifyAll();
                    }
                }
            });
        } catch (final Exception failure) {
            if (reserved) {
                synchronized (budget) {
                    queuedTasks--;
                    queuedBytes -= bytes;
                    budget.notifyAll();
                }
            }
            future.completeExceptionally(failure);
        }
        return future;
    }

    /**
     * Target diagnostics are used for limited detection periods consistent with health management.
     *
     * @param millis millis
     */
    void configureProbeBudget(final long millis) {
        probeMillis = millis;
    }

    /**
     * Operational access and health management will allow unobstructed observation applications to fail.
     *
     * @return the operation result
     */
    boolean failed() {
        return applicationFailure != null;
    }

    /**
     * WAL I/O is in transit and the bottom engine can be shut down only if the actual installation is empty.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        closed = true;
        synchronized (budget) {
            budget.notifyAll();
        }
        images.shutdown();
        try {
            if (!images.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("snapshot images did not drain");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("snapshot image close interrupted", interrupted);
        }
        probes.shutdown();
        apply.shutdown();
        try {
            if (!probes.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("backend probes did not drain");
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("backend probe close interrupted", failure);
        }
        try {
            if (!apply.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("committed application did not drain");
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("state machine close interrupted", failure);
        }
    }

    /**
     * Prepares the output to bind the actual kernel, Raft position and configuration. The background thread cannot re-guess the boundary.
     *
     * @param core core
     * @param position position
     * @param configurationIndex configuration index
     * @param configuration configuration
     * @param work work
     * @param backup backup
     */
    private record ImageCut(WalMvccRuntime core, TermIndex position, long configurationIndex, RaftProtos.RaftConfigurationProto configuration, Path work, WalReplicaBackup backup) {
    }

    /**
     * Images published but not last completed for activation must be bound to the same kernel.
     *
     * @param core core
     * @param position position
     */
    private record ExistingCut(WalMvccRuntime core, TermIndex position) {
    }

    /**
     * Installation missions can transmit disks and protocols abnormally and cannot be swallowed up into a successful response.
     */
    @FunctionalInterface
    private interface Work {

        /**
         * Install or observe an commit state sequentially.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        Message run() throws Exception;
    }
}
