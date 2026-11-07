package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** 当前代次保护、文件预算、持久退休续做及指针回退拒绝专项。 */
class RatisStateRetirementTest {
    @TempDir Path directory;
    private final UUID group = UUID.randomUUID(); private final WalReplicaIdentity identity = new WalReplicaIdentity(group, group, "target");
    private final TermIndex position = TermIndex.valueOf(3, 10);
    private final List<WalReceipt> receipts = new ArrayList<>();

    /** 每轮一个文件也能完成，当前代次数据与所有凭据保留，旧指针不能复活退休对象。 */
    @Test void boundedFilesProtectCurrentAndRetiredPointerCannotReopen() throws Exception {
        Path bundle = bundle(); Path root = directory.resolve("target"); var authority = new Authority(); RatisGenerationRecord old;
        try (var state = state(root, authority, stage -> { })) {
            authority.state = state; state.initialize(null, null); old = RatisGenerationRecord.read(root.resolve("ACTIVE")); state.initialize(bundle, position);
            var current = RatisGenerationRecord.read(root.resolve("ACTIVE")); assertNotEquals(old.generation(), current.generation());
            Path scratch = root.resolve(".record-" + UUID.randomUUID()); Files.write(scratch, new byte[]{1}); Path note = root.resolve("user-note"); Files.writeString(note, "keep");
            finish(state, 1); assertFalse(Files.exists(root.resolve("generation-" + old.generation()))); assertTrue(Files.exists(root.resolve("generation-" + current.generation())));
            assertFalse(Files.exists(scratch)); assertEquals("keep", Files.readString(note));
            assertTrue(Files.exists(root.resolve("retired").resolve(old.generation().toString()))); verify(state);
            old.publish(root.resolve("ACTIVE"));
        }
        assertThrows(IOException.class, () -> state(root, authority, stage -> { })); assertFalse(Files.exists(root.resolve("generation-" + old.generation())));
    }

    /** 日志、后端进度、实际删除和墓碑后失败都能重开续做，不能改删另一代次。 */
    @ParameterizedTest @EnumSource(RatisWalState.RetirementStage.class)
    void interruptionResumesSameRetirement(RatisWalState.RetirementStage wanted) throws Exception {
        Path bundle = bundle(); Path root = directory.resolve("target"); var authority = new Authority(); var armed = new AtomicBoolean(true); UUID old;
        try (var state = state(root, authority, stage -> { if (stage == wanted && armed.compareAndSet(true, false)) { throw new IOException("injected retirement " + wanted); } })) {
            authority.state = state; state.initialize(null, null); old = RatisGenerationRecord.read(root.resolve("ACTIVE")).generation(); state.initialize(bundle, position);
            assertThrows(IOException.class, () -> state.retire(new RatisRetirementWork(options(128)))); assertTrue(state.retirementPending());
        }
        try (var state = state(root, authority, stage -> { })) {
            authority.state = state; state.initialize(bundle, position); finish(state, 2); verify(state); assertFalse(state.retirementPending()); assertFalse(Files.exists(root.resolve("generation-" + old)));
        }
    }

    /** 无旧代次时不删除当前数据；未知文件也不能被递归清理掩盖。 */
    @Test void activeGenerationAndUnknownFilesAreProtected() throws Exception {
        Path bundle = bundle(); Path root = directory.resolve("target"); var authority = new Authority();
        try (var state = state(root, authority, stage -> { })) {
            authority.state = state; state.initialize(null, null); UUID old = RatisGenerationRecord.read(root.resolve("ACTIVE")).generation();
            var empty = new RatisRetirementWork(options(128)); state.retire(empty); assertEquals(0, empty.completed); assertTrue(Files.exists(root.resolve("generation-" + old)));
            state.initialize(bundle, position); Path unknown = root.resolve("generation-" + old + "/wal/user-file"); Files.writeString(unknown, "keep");
            assertThrows(IOException.class, () -> state.retire(new RatisRetirementWork(options(128)))); assertEquals("keep", Files.readString(unknown)); verify(state);
        }
    }

    /** 取保护集合以后，安装必须等实际删除阶段退出，不能在维护中途换代次。 */
    @Test void snapshotProtectionRemainsLockedThroughCleanup() throws Exception {
        Path bundle = bundle(); var authority = new Authority();
        try (var state = state(directory.resolve("target"), authority, stage -> { }); var executor = Executors.newSingleThreadExecutor()) {
            authority.state = state; state.initialize(bundle, position); var entered = new CountDownLatch(1); var task = new java.util.concurrent.atomic.AtomicReference<Future<?>>();
            state.cleanupSnapshots(new RatisRetirementWork(options(128)), (work, protectedIndices) -> {
                assertEquals(Set.of(position.getIndex()), protectedIndices);
                task.set(executor.submit(() -> { entered.countDown(); state.initialize(bundle, position); return null; }));
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    assertThrows(TimeoutException.class, () -> task.get().get(100, TimeUnit.MILLISECONDS));
                    assertTrue(state.ready());
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
            });
            task.get().get(30, TimeUnit.SECONDS); verify(state);
        }
    }

    /** 重复维护使用真实返回计数，预算不耗尽下一轮可继续。 */
    private static void finish(RatisWalState state, int files) throws Exception {
        for (int i = 0; i < 100; i++) { var work = new RatisRetirementWork(options(files)); state.retire(work); assertTrue(work.removed <= files); if (!state.retirementPending() && work.completed > 0) { return; } }
        throw new AssertionError("retirement did not make bounded progress");
    }
    /** 当前内核仍可读取两片数据及两份永久凭据。 */
    private void verify(RatisWalState state) { var tx = state.core().begin(IsolationLevel.REPEATABLE_READ); var view = state.core().beginReadStatement(tx); for (String shard : List.of("a", "b")) { assertArrayEquals(new byte[]{2}, state.core().get(tx, shard, "k", view).orElseThrow()); } state.core().rollback(tx); assertEquals(2, state.core().stats().publishedSequence()); for (var receipt : receipts) { assertEquals(receipt, state.core().findCommitted(receipt.transactionId()).orElseThrow()); } }
    /** 有限扫描和单轮文件数量，时间留给测试容器之外的桌面任务。 */
    private static RatisRetirementOptions options(int files) { return new RatisRetirementOptions(1, files, 512, 5000, 2); }
    /** 包内验证退休边界，正式装配构造无注入。 */
    private RatisWalState state(Path root, Authority authority, RatisWalState.RetirementHook hook) throws IOException { return new RatisWalState(root, identity, Set.of("a", "b"), WalOptions.defaults(), authority, WalReplicaBackendFactory.localOnly(), limits(), stage -> { }, hook); }
    /** 构造正式 WAL 备份，受控端口不冒充生产共识。 */
    private Path bundle() throws Exception {
        var authority = new Authority(); Path backup = directory.resolve("backup");
        try (var source = WalMvccRuntime.openReplica(directory.resolve("source"), Set.of("a", "b"), Map.of(), WalOptions.defaults(), WalRetentionOptions.preserveAll(), new WalReplicaIdentity(group, group, "source"), authority)) {
            authority.source = source; for (int value = 1; value <= 2; value++) { var tx = source.begin(IsolationLevel.REPEATABLE_READ); for (String shard : List.of("a", "b")) { source.put(tx, shard, "k", new byte[]{(byte) value}); } receipts.add(source.commit(tx)); } source.backupReplica(authority, backup, limits());
        }
        var config = RaftConfigurationProto.newBuilder(); for (int i = 0; i < 3; i++) { config.addPeers(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build().getRaftPeerProto()); }
        Path bundle = directory.resolve("snapshot.bundle"); RatisSnapshotBundle.pack(backup, bundle, RaftGroupId.valueOf(group), position, 0, config.build(), limits()); return bundle;
    }
    /** 每轮镜像操作有有限字节和时间预算。 */
    private static WalBackupOptions limits() { return new WalBackupOptions(16_000_000, 2, 64_000, 60_000); }
    /** 只提供已提交本地记录，用于退休协议而非真实选举测试。 */
    private static final class Authority implements WalCommitAuthority {
        RatisWalState state; WalMvccRuntime source;
        /** 安装未就绪时不提供业务任期。 */
        @Override public long epoch() { return state == null || state.ready() ? 1 : 0; }
        /** 单测不发网络请求。 */
        @Override public long readBarrier(long timeout) { return epoch(); }
        /** 安装真实 WAL 决议之后才确认。 */
        @Override public CompletionStage<Void> submit(long epoch, List<WalCommit> commits) { (state == null ? source : state.core()).applyCommitted(this, commits); return CompletableFuture.completedFuture(null); }
    }
}
