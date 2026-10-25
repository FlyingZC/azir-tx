package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** 实际持久代次边界故障验证；位置为测试给定，真实 Raft 安装另做集成验证。 */
class RatisGenerationInstallTest {
    @TempDir Path directory;
    private final RaftGroupId group = RaftGroupId.randomId();
    private final TermIndex position = TermIndex.valueOf(3, 10);

    /** 记录、恢复、指针和操作清理任一边界失败后，重开只能继续同一完整安装。 */
    @ParameterizedTest @EnumSource(value = RatisWalState.Stage.class, names = {"BEFORE_INPUT_FORCE", "AFTER_INSTALL_RECORD", "AFTER_RESTORE", "BEFORE_ACTIVE", "AFTER_ACTIVE", "AFTER_CLEARED"})
    void crashAtInstallationBoundaryResumesSameGenerationWithoutServingOldState(RatisWalState.Stage stage) throws Exception {
        Path bundle = bundle(); Path root = directory.resolve("target"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var opens = new AtomicInteger(); var closes = new AtomicInteger();
        WalReplicaBackendFactory factory = (owner, generation) -> { opens.incrementAndGet(); return new WalReplicaBackendFactory.Lease(Map.of(), () -> closes.incrementAndGet()); };
        var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> { if (hit == stage) { throw new IOException("injected " + stage); } })) {
            authority.state = state; state.initialize(null, null); assertTrue(state.ready()); assertEquals(0, state.core().stats().publishedSequence());
            assertThrows(IOException.class, () -> state.initialize(bundle, position)); assertFalse(state.ready());
        }
        int before = opens.get(); boolean pending = Files.exists(root.resolve("INSTALLING"));
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> { })) {
            authority.state = state; assertFalse(state.ready()); if (pending) { assertEquals(before, opens.get(), "unfinished install must not open old generation"); }
            state.initialize(bundle, position); assertTrue(state.ready()); assertFalse(Files.exists(root.resolve("INSTALLING"))); assertEquals(2, state.core().stats().publishedSequence());
            var tx = state.core().begin(IsolationLevel.REPEATABLE_READ); var snapshot = state.core().beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) { assertEquals(shard + ":new", new String(state.core().get(tx, shard, "k", snapshot).orElseThrow(), StandardCharsets.UTF_8)); }
            state.core().rollback(tx); var record = RatisGenerationRecord.read(root.resolve("ACTIVE")); assertEquals(position.getIndex(), record.index()); assertEquals(2, record.sequence());
        }
        assertEquals(opens.get(), closes.get(), "every owned backend lease must close");
    }

    /** 待恢复节点先登记能力要求，真正打开的后端不合格时仍必须在导入和服务前拒绝。 */
    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"GC", "SNAPSHOT"})
    void pendingLeaseRequirementIsDeferredButEnforcedBeforeRestore(String requirement) throws Exception {
        Path bundle = bundle(), root = directory.resolve("target"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> { })) {
            authority.state = state; state.initialize(null, null); state.pause(); state.durableBeforeSnapshotAck(bundle, position);
        }
        var unsupported = (WalMaterializedStore) java.lang.reflect.Proxy.newProxyInstance(WalMaterializedStore.class.getClassLoader(), new Class<?>[]{WalMaterializedStore.class}, (proxy, method, args) -> switch (method.getName()) {
            case "supportsReplicaOwnership", "supportsReplicaRestore" -> true;
            case "supportsBoundedMaintenance", "supportsPinnedSnapshot" -> false;
            default -> throw new AssertionError("backend data accessed before capability validation: " + method.getName());
        });
        WalReplicaBackendFactory invalid = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of("a", unsupported), () -> { });
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, invalid, limits(), stage -> { })) {
            authority.state = state;
            if (requirement.equals("GC")) { state.requireBoundedGc(); } else { state.requirePinnedSnapshots(); }
            var failure = assertThrows(IOException.class, () -> state.initialize(bundle, position));
            assertTrue(failure.getMessage().contains(requirement.equals("GC") ? "bounded version GC" : "automatic snapshots"));
            assertFalse(state.ready()); assertTrue(Files.exists(root.resolve("INSTALLING"))); assertEquals(-1, RatisGenerationRecord.read(root.resolve("ACTIVE")).index());
        }
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> { })) {
            authority.state = state; state.requireBoundedGc(); state.requirePinnedSnapshots(); state.initialize(bundle, position);
            assertTrue(state.ready()); assertEquals(2, state.core().stats().publishedSequence()); assertFalse(Files.exists(root.resolve("INSTALLING")));
        }
    }

    /** 初始控制记录已经消失时，重试放行仍必须重新经过目录同步，不能跳过之前失败的位置。 */
    @Test void retryCannotBecomeReadyBeforeDirectoryConfirmation() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority(); Path root = directory.resolve("target"); var attempts = new AtomicInteger();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> {
            if (stage == RatisWalState.Stage.BEFORE_READY_DIRECTORY_FORCE && attempts.getAndIncrement() < 2) { throw new IOException("injected readiness directory force failure"); }
        })) {
            authority.state = state; assertThrows(IOException.class, () -> state.initialize(null, null)); assertFalse(state.ready()); assertFalse(Files.exists(root.resolve("BOOTSTRAPPING")));
            assertThrows(IOException.class, () -> state.initialize(null, null)); assertFalse(state.ready()); assertEquals(2, attempts.get());
            state.initialize(null, null); assertTrue(state.ready()); assertEquals(3, attempts.get());
        }
    }

    /** ACTIVE 已完整替换但目录确认失败时，后续回收证明不能仅凭读取到新记录就成功。 */
    @Test void visibleActiveRecordStillRequiresDurabilityBeforePurgeProof() throws Exception {
        Path first = bundle(), root = directory.resolve("target"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority();
        var nextPosition = TermIndex.valueOf(3, 11); var oldHeader = RatisSnapshotBundle.inspect(first, group, position, limits()); Path next = directory.resolve("next.bundle");
        var nextHeader = RatisSnapshotBundle.pack(directory.resolve("backup"), next, group, nextPosition, 0, oldHeader.configuration(), limits());
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> { })) {
            authority.state = state; state.initialize(first, position); var note = state.prepareSnapshotNote(nextHeader, next);
            assertThrows(IOException.class, () -> state.noteSnapshot(note, new RatisMutableRecordIoTest.Fault(RatisMutableRecordIoTest.Failure.BEFORE_DIRECTORY_FORCE)));
            assertEquals(11, RatisGenerationRecord.read(root.resolve("ACTIVE")).index());
            var failedProof = new RatisMutableRecordIoTest.Fault(RatisMutableRecordIoTest.Failure.BEFORE_DIRECTORY_FORCE);
            assertThrows(IOException.class, () -> state.activatedSnapshotCovers(11, failedProof)); assertTrue(failedProof.fired);
            var confirmed = new RatisMutableRecordIoTest.Fault(null); assertTrue(state.activatedSnapshotCovers(11, confirmed)); assertTrue(confirmed.operations.contains("FILE_FORCE")); assertTrue(confirmed.operations.contains("DIRECTORY_FORCE")); assertTrue(state.ready());
        }
    }

    /** 损坏指针和错误节点身份不能触发空初始化，失败也应释放目录锁。 */
    @Test void corruptPointerAndWrongNodeCannotBootstrapOrRetainWriterLock() throws Exception {
        Path root = directory.resolve("target"); var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority();
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { })) { authority.state = state; state.initialize(null, null); }
        byte[] good = Files.readAllBytes(root.resolve("ACTIVE")); byte[] bad = good.clone(); bad[bad.length - 1] ^= 1; Files.write(root.resolve("ACTIVE"), bad);
        assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { })); Files.write(root.resolve("ACTIVE"), good);
        var wrong = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "other");
        assertThrows(IOException.class, () -> new RatisWalState(root, wrong, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { }));
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { })) { authority.state = state; state.initialize(null, null); assertTrue(state.ready()); }
    }

    /** 成功确认以前必须有独立持久输入，原接收文件丢失后仍可续做；force 失败不得成功返回。 */
    @Test void snapshotAckRequiresDurableIndependentRecoveryInput() throws Exception {
        Path bundle = bundle(); Path failingInput = directory.resolve("force-failure.bundle"); Files.copy(bundle, failingInput);
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority(); Path root = directory.resolve("target"); Path durable;
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { })) {
            authority.state = state; state.initialize(null, null); state.pause(); state.durableBeforeSnapshotAck(bundle, position);
            assertFalse(state.ready()); assertTrue(Files.exists(root.resolve("INSTALLING"))); durable = state.pendingBundle(); assertNotNull(durable);
            assertEquals(position, RatisSnapshotBundle.inspect(durable, group, position, limits()).position()); Files.delete(bundle);
        }
        try (var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { })) {
            authority.state = state; state.initialize(durable, position); assertTrue(state.ready()); assertEquals(2, state.core().stats().publishedSequence());
        }
        Path failureRoot = directory.resolve("failed-target");
        try (var state = new RatisWalState(failureRoot, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> {
            if (hit == RatisWalState.Stage.BEFORE_INPUT_FORCE) { throw new IOException("cannot force received input"); }
        })) {
            authority.state = state; state.initialize(null, null); state.pause(); assertThrows(IOException.class, () -> state.durableBeforeSnapshotAck(failingInput, position));
            assertFalse(Files.exists(failureRoot.resolve("INSTALLING"))); assertFalse(state.ready());
        }
    }

    /** 关闭被中断时不能释放外层目录锁和后端租约，清除中断后才能安全重试关闭。 */
    @Test void interruptedCoreCloseKeepsGenerationOwnershipUntilActualDrain() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target"); var authority = new Authority(); Path root = directory.resolve("target"); var released = new AtomicInteger();
        WalReplicaBackendFactory factory = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of(), () -> released.incrementAndGet());
        var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> { }); authority.state = state; state.initialize(null, null);
        try (var tasks = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            authority.blocked = new CompletableFuture<>(); authority.submitted = new java.util.concurrent.CountDownLatch(1);
            var tx = state.core().begin(IsolationLevel.REPEATABLE_READ); state.core().put(tx, "a", "pending", new byte[]{1});
            var committing = tasks.submit(() -> state.core().commit(tx)); assertTrue(authority.submitted.await(5, java.util.concurrent.TimeUnit.SECONDS));
            Thread.currentThread().interrupt();
            try { assertThrows(IllegalStateException.class, state::close); } finally { Thread.interrupted(); }
            assertEquals(0, released.get()); assertFalse(state.ready());
            assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> { }));
            authority.blocked.completeExceptionally(new IOException("release test proposal")); assertThrows(java.util.concurrent.ExecutionException.class, () -> committing.get(5, java.util.concurrent.TimeUnit.SECONDS));
        } finally { Thread.interrupted(); if (authority.blocked != null) { authority.blocked.completeExceptionally(new IOException("test cleanup")); } state.close(); }
        assertEquals(1, released.get());
    }

    /** 后端租约释放失败时保留目录所有权，成功重试后才清空句柄并释放锁。 */
    @Test void failedBackendReleaseKeepsLeaseAndDirectoryUntilRetry() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        var authority = new Authority(); var attempts = new AtomicInteger(); Path root = directory.resolve("release-failure");
        WalReplicaBackendFactory factory = (owner, generation) -> new WalReplicaBackendFactory.Lease(Map.of(), () -> {
            if (attempts.incrementAndGet() == 1) { throw new IOException("lease release failed"); }
        });
        var state = new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, factory, limits(), hit -> { });
        try {
            assertThrows(IOException.class, state::close);
            assertThrows(IOException.class, () -> new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), hit -> { }));
            state.close(); state.close(); assertEquals(2, attempts.get());
        } finally { state.close(); }
    }

    /** 原始节点尚未返回时也能保留后端清理句柄，不必启动 Raft 或数据库验证目录锁责任。 */
    @Test void rawNodeStartupFailureRetainsCleanupForAnUnreturnedGeneration() throws Exception {
        var peers = List.of(RaftPeer.newBuilder().setId("target").setAddress("127.0.0.1:19001").build(),
                RaftPeer.newBuilder().setId("peer-b").setAddress("127.0.0.1:19002").build(),
                RaftPeer.newBuilder().setId("peer-c").setAddress("127.0.0.1:19003").build());
        var configuration = RaftGroup.valueOf(group, peers); var allowed = new java.util.concurrent.atomic.AtomicBoolean();
        Path root = directory.resolve("partial-node");
        WalReplicaBackendFactory factory = (owner, generation) -> {
            throw new RatisStartupException(new IOException("backend open failed"), () -> {
                if (!allowed.get()) { throw new IOException("backend cleanup pending"); }
            });
        };
        var pending = assertThrows(RatisStartupException.class, () -> RatisWalNode.openGenerationalLoopback(root, peers.getFirst().getId(), configuration,
                Set.of("a", "b"), factory, WalOptions.defaults(), limits()));
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "target");
        try {
            assertTrue(pending.cleanupPending()); assertThrows(IOException.class, pending::retryCleanup);
            assertThrows(IOException.class, () -> new RatisWalState(root.resolve("business"), identity, Set.of("a", "b"), WalOptions.defaults(), new Authority(), WalReplicaBackendFactory.localOnly(), limits(), hit -> { }));
        } finally { allowed.set(true); pending.retryCleanup(); }
        assertFalse(pending.cleanupPending());
        try (var reopened = new RatisWalState(root.resolve("business"), identity, Set.of("a", "b"), WalOptions.defaults(), new Authority(), WalReplicaBackendFactory.localOnly(), limits(), hit -> { })) {
            assertNotNull(reopened.core());
        }
    }

    /** 构造已完成的业务镜像与封装，代次测试不伪造 Raft 多数派证明。 */
    private Path bundle() throws Exception {
        var identity = new WalReplicaIdentity(group.getUuid(), group.getUuid(), "source"); var authority = new Authority(); Path backup = directory.resolve("backup");
        try (var core = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), identity, authority)) {
            authority.source = core;
            for (String value : List.of("old", "new")) { var tx = core.begin(IsolationLevel.REPEATABLE_READ); for (String shard : Set.of("a", "b")) { core.put(tx, shard, "k", (shard + ":" + value).getBytes(StandardCharsets.UTF_8)); } core.commit(tx); }
            core.backupReplica(authority, backup, limits());
        }
        var config = RaftConfigurationProto.newBuilder(); for (int i = 0; i < 3; i++) { config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto()); }
        Path bundle = directory.resolve("snapshot.bundle"); RatisSnapshotBundle.pack(backup, bundle, group, position, 0, config.build(), limits()); return bundle;
    }

    /** 每次操作有有限 I/O 时间与总字节。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(16_000_000, 2, 64_000, 60_000); }

    /** 受控端口只用于代次协议，安装期间返回零任期。 */
    private static final class Authority implements WalCommitAuthority {
        RatisWalState state; WalMvccRuntime source;
        CompletableFuture<Void> blocked; java.util.concurrent.CountDownLatch submitted;
        /** 未就绪代次不能建立业务句柄。 */
        @Override public long epoch() { return state == null || state.ready() ? 1 : 0; }
        /** 镜像单测不访问网络。 */
        @Override public long readBarrier(long timeoutMillis) { return epoch(); }
        /** 本地已提交回调不代表生产共识。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) {
            if (blocked != null) { submitted.countDown(); return blocked; }
            (state == null ? source : state.core()).applyCommitted(this, commits); return CompletableFuture.completedFuture(null);
        }
    }
}
