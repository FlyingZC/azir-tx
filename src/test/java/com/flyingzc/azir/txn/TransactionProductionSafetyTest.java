package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MutationType;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.GcManager;
import com.flyingzc.azir.gc.InMemorySafePointStore;
import com.flyingzc.azir.metadata.InMemoryTransactionMetadataStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.recovery.RecoveryManager;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.tso.AtomicTimestampOracle;
import com.flyingzc.azir.tso.TimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 生产化边界的确定性测试源码，本轮按用户要求不执行。 */
class TransactionProductionSafetyTest {
    /** 快照发放必须等待已取提交时间戳但尚未发布的提交决议。 */
    @Test
    void snapshotMustWaitForCommitPublication() throws Exception {
        var metadata = new InMemoryTransactionMetadataStore();
        var allocated = new CountDownLatch(1);
        var publish = new CountDownLatch(1);
        var sequence = new AtomicTimestampOracle(100);
        var calls = new AtomicInteger();
        TimestampOracle tso = () -> {
            long timestamp = sequence.nextTimestamp();
            if (calls.incrementAndGet() == 2) {
                allocated.countDown();
                await(publish);
            }
            return timestamp;
        };
        var shard = new MemoryParticipant("s1");
        try (var manager = new TransactionManager(tso, metadata, Map.of("s1", shard));
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Transaction writer = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(writer, "s1", "k", new byte[]{7});
            var commit = executor.submit(() -> manager.commit(writer));
            assertTrue(allocated.await(5, TimeUnit.SECONDS));
            var started = new CountDownLatch(1);
            var reader = executor.submit(() -> {
                started.countDown();
                return manager.begin(IsolationLevel.REPEATABLE_READ);
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            try {
                assertThrows(TimeoutException.class, () -> reader.get(100, TimeUnit.MILLISECONDS));
            } finally {
                publish.countDown();
            }
            long committedTs = commit.get(5, TimeUnit.SECONDS).commitTs();
            Transaction tx = reader.get(5, TimeUnit.SECONDS);
            assertTrue(tx.startTs() > committedTs);
            assertArrayEquals(new byte[]{7}, manager.get(tx, "s1", "k").orElseThrow());
            assertArrayEquals(new byte[]{7}, manager.get(tx, "s1", "k").orElseThrow());
            manager.commit(tx);
        } finally {
            publish.countDown();
        }
    }

    /** 已持久化提交决议后的失败不能由 execute 模板回滚其余参与者。 */
    @Test
    void committedDecisionMustSurviveExecuteFailure() {
        var metadata = new InMemoryTransactionMetadataStore();
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        second.failCommit.set(true);
        Map<String, StorageAdapter> shards = Map.of("a", first, "b", second);
        var txId = new AtomicReference<Long>();
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, shards)) {
            MvccException failure = assertThrows(MvccException.class, () -> manager.execute(IsolationLevel.REPEATABLE_READ, tx -> {
                txId.set(tx.txId());
                manager.put(tx, "a", "left", new byte[]{1});
                manager.put(tx, "b", "right", new byte[]{2});
                return null;
            }));
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
            assertFalse(failure.retryable());
            assertEquals(TxStatus.COMMITTED, metadata.get(txId.get()).orElseThrow().status());
            assertEquals(0, first.aborts.get() + second.aborts.get());
            assertTrue(second.intents.containsKey(txId.get()));
            new RecoveryManager(metadata, shards).recover(txId.get());
            assertFalse(second.intents.containsKey(txId.get()));
        }
    }

    /** 恢复不能接管仍拥有有效快照租约的正常写事务。 */
    @Test
    void recoveryMustNotAbortLiveTransaction() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata,
                TransactionOptions.failFastDefaults(), Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            new RecoveryManager(metadata, Map.of("s1", shard)).recover(tx.txId());
            assertEquals(TxStatus.ACTIVE, metadata.get(tx.txId()).orElseThrow().status());
            assertEquals(0, shard.aborts.get());
            manager.rollback(tx);
        }
    }

    /** lazy 只读事务仍需登记快照并阻止 GC 越过其可见边界。 */
    @Test
    void lazyReaderMustPinSafePoint() {
        var metadata = new InMemoryTransactionMetadataStore();
        var tso = new AtomicTimestampOracle(100);
        var safePoint = new InMemorySafePointStore(0);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false);
        try (var manager = new TransactionManager(tso, metadata, safePoint, options, Map.of())) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertTrue(metadata.get(tx.txId()).isEmpty());
            assertTrue(metadata.snapshotExpiry(tx.txId()).isPresent());
            assertTrue(new GcManager(metadata, tso, safePoint, Map.of()).advanceSafePoint() < tx.startTs());
            manager.commit(tx);
            assertTrue(metadata.snapshotExpiry(tx.txId()).isEmpty());
        }
    }

    /** 超出写集合预算时不得把被拒绝的写入放入缓冲。 */
    @Test
    void writeBudgetMustRejectBeforeBufferMutation() {
        var limits = new TransactionLimits(60_000, 1, 1024, 8, 2, 2, 2, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false, limits);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                options, Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "a", new byte[]{1});
            MvccException failure = assertThrows(MvccException.class, () -> manager.put(tx, "s1", "b", new byte[]{2}));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, failure.errorCode());
            assertTrue(tx.bufferedMutation("s1", "b").isEmpty());
            manager.rollback(tx);
        }
    }

    /** 后台队列满时采用同步回退，并保持固定队列容量。 */
    @Test
    void asyncQueueMustBeBoundedAndFallbackSynchronously() throws Exception {
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        first.blockNext.set(true);
        var limits = new TransactionLimits(60_000, 10, 1024, 8, 2, 1, 2, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.asyncParallel(1), true, false, false, limits);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                options, Map.of("a", first, "b", second))) {
            try {
                writePair(manager, "1");
                assertTrue(first.entered.await(5, TimeUnit.SECONDS));
                writePair(manager, "2");
                writePair(manager, "3");
                assertTrue(manager.runtimeStats().asyncQueued() <= 1);
                assertEquals(1L, manager.runtimeStats().synchronousFallbacks());
            } finally {
                first.proceed.countDown();
            }
        }
    }

    /** 本地提交成功但中心未决时，rollback 必须采用本地 COMMITTED 结果。 */
    @Test
    void rollbackMustResolveLocallyCommittedSingleShard() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.atomicDirect = true;
        shard.failAfterLocalCommit.set(true);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{3});
            MvccException failure = assertThrows(MvccException.class, () -> manager.commit(tx));
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
            assertThrows(MvccException.class, metadata::assertNoPendingPublications);
            manager.rollback(tx);
            assertEquals(TxStatus.COMMITTED, tx.status());
            assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
            assertEquals(0, shard.aborts.get());
            assertDoesNotThrow(metadata::assertNoPendingPublications);
            Transaction reader = manager.begin(IsolationLevel.REPEATABLE_READ);
            assertArrayEquals(new byte[]{3}, manager.get(reader, "s1", "k").orElseThrow());
            manager.commit(reader);
        }
    }

    /** 相同 txId 在不同事务域中不能绕过管理器归属校验。 */
    @Test
    void rollbackMustRejectTransactionFromAnotherManager() {
        var firstMetadata = new InMemoryTransactionMetadataStore();
        var secondMetadata = new InMemoryTransactionMetadataStore();
        try (var first = new TransactionManager(new AtomicTimestampOracle(100), firstMetadata, Map.of());
             var second = new TransactionManager(new AtomicTimestampOracle(100), secondMetadata, Map.of())) {
            Transaction a = first.begin(IsolationLevel.REPEATABLE_READ);
            Transaction b = second.begin(IsolationLevel.REPEATABLE_READ);
            assertEquals(a.txId(), b.txId());
            MvccException error = assertThrows(MvccException.class, () -> second.rollback(a));
            assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
            assertEquals(TxStatus.ACTIVE, secondMetadata.get(b.txId()).orElseThrow().status());
            assertTrue(secondMetadata.snapshotExpiry(b.txId()).isPresent());
        }
    }

    /** eager 预写失败后不允许提交缺少持久意向的参与者集合。 */
    @Test
    void failedEagerPrewriteMustInvalidateTransaction() {
        var metadata = new InMemoryTransactionMetadataStore();
        var first = new MemoryParticipant("a");
        var second = new MemoryParticipant("b");
        second.failPrewrite.set(true);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata,
                TransactionOptions.failFastDefaults(), Map.of("a", first, "b", second))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "a", "k", new byte[]{1});
            assertThrows(MvccException.class, () -> manager.put(tx, "b", "k", new byte[]{2}));
            assertTrue(tx.rollbackOnly());
            MvccException error = assertThrows(MvccException.class, () -> manager.commit(tx));
            assertEquals(ErrorCode.TRANSACTION_ABORTED, error.errorCode());
            assertEquals(TxStatus.ACTIVE, metadata.get(tx.txId()).orElseThrow().status());
            manager.rollback(tx);
            assertEquals(TxStatus.ABORTED, metadata.get(tx.txId()).orElseThrow().status());
        }
    }

    /** 返回的分页数组不能成为修改事务写缓冲的入口。 */
    @Test
    void pageValuesMustNotAliasBufferedWrites() {
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            var snapshot = manager.beginReadStatement(tx);
            var page = manager.scanPage(tx, "s1", "a", "z", snapshot, null, 1);
            page.values().getFirst().value()[0] = 9;
            assertArrayEquals(new byte[]{1}, manager.get(tx, "s1", "k", snapshot).orElseThrow());
            manager.rollback(tx);
        }
    }

    /** 发布屏障退出失败不得留下占用许可的幽灵事务或额外释放许可。 */
    @Test
    void failedBeginPublicationMustReleaseExactlyOnePermit() {
        var delegate = new InMemoryTransactionMetadataStore();
        var failExit = new AtomicBoolean(true);
        TransactionMetadataStore metadata = (TransactionMetadataStore) java.lang.reflect.Proxy.newProxyInstance(
                TransactionMetadataStore.class.getClassLoader(), new Class<?>[]{TransactionMetadataStore.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if (method.getName().equals("withPublicationLock") && failExit.getAndSet(false)) {
                            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected publication exit failure", false, null);
                        }
                        return result;
                    } catch (java.lang.reflect.InvocationTargetException ex) {
                        throw ex.getCause();
                    }
                });
        var limits = new TransactionLimits(60_000, 10, 1024, 1, 1, 1, 1, 1000);
        var options = new TransactionOptions(LockWaitPolicy.failFast(), TransactionRetryPolicy.noRetry(),
                CrossShardCommitPolicy.serial(), true, true, false, limits);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, options, Map.of())) {
            assertThrows(MvccException.class, () -> manager.begin(IsolationLevel.REPEATABLE_READ));
            assertEquals(0, manager.runtimeStats().activeTransactions());
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            MvccException error = assertThrows(MvccException.class, () -> manager.begin(IsolationLevel.REPEATABLE_READ));
            assertEquals(ErrorCode.RESOURCE_EXHAUSTED, error.errorCode());
            manager.rollback(tx);
        }
    }

    /** 非原子单分片适配器缺少提交意向解析时必须在发布决议前拒绝提交。 */
    @Test
    void nonAtomicSingleShardMustRequireIntentResolution() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.resolutionEnabled = false;
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            MvccException error = assertThrows(MvccException.class, () -> manager.commit(tx));
            assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
            assertNotEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
            assertTrue(shard.intents.isEmpty());
            manager.rollback(tx);
        }
    }

    /** 参与者无法提供终态证据时不能把未知本地提交变成中心回滚。 */
    @Test
    void unknownLocalFencingMustNotPublishAbort() {
        var metadata = new InMemoryTransactionMetadataStore();
        var shard = new MemoryParticipant("s1");
        shard.atomicDirect = true;
        shard.failAfterLocalCommit.set(true);
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), metadata, Map.of("s1", shard))) {
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.put(tx, "s1", "k", new byte[]{1});
            assertThrows(MvccException.class, () -> manager.commit(tx));
            shard.hideLocalDecision = true;
            MvccException error = assertThrows(MvccException.class, () -> manager.rollback(tx));
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
            assertEquals(TxStatus.PREPARING, metadata.get(tx.txId()).orElseThrow().status());
            shard.hideLocalDecision = false;
            for (TxStatus status : List.of(TxStatus.ACTIVE, TxStatus.PREPARING)) {
                shard.reportedLocalStatus = status;
                assertThrows(MvccException.class, () -> manager.rollback(tx));
                assertEquals(TxStatus.PREPARING, metadata.get(tx.txId()).orElseThrow().status());
            }
            shard.reportedLocalStatus = null;
            manager.rollback(tx);
            assertEquals(TxStatus.COMMITTED, metadata.get(tx.txId()).orElseThrow().status());
        }
    }

    /** 小页合并自身新增、覆盖和删除时保持游标前进且不遗漏后续存储键。 */
    @Test
    void bufferedPaginationMustMergeDeletesAndNewKeys() {
        try (var manager = new TransactionManager(new AtomicTimestampOracle(100), new InMemoryTransactionMetadataStore(),
                Map.of("s1", new MemoryParticipant("s1")))) {
            Transaction seed = manager.begin(IsolationLevel.REPEATABLE_READ);
            for (String key : List.of("a", "b", "c")) { manager.put(seed, "s1", key, new byte[]{1}); }
            manager.commit(seed);
            Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
            manager.delete(tx, "s1", "a");
            manager.put(tx, "s1", "b", new byte[]{2});
            manager.put(tx, "s1", "d", new byte[]{4});
            manager.delete(tx, "s1", "e");
            manager.put(tx, "s1", "z", new byte[]{9});
            var snapshot = manager.beginReadStatement(tx);
            var keys = new ArrayList<String>();
            String after = null;
            int pages = 0;
            do {
                var page = manager.scanPage(tx, "s1", "a", "m", snapshot, after, 1);
                page.values().forEach(value -> keys.add(value.key()));
                after = page.nextKey();
                assertTrue(++pages < 10, "cursor must make progress through empty pages");
            } while (after != null);
            assertEquals(List.of("b", "c", "d"), keys);
            assertArrayEquals(new byte[]{2}, manager.get(tx, "s1", "b", snapshot).orElseThrow());
            manager.rollback(tx);
        }
    }

    /** 提交一个无冲突双分片事务。 */
    private static void writePair(TransactionManager manager, String suffix) {
        Transaction tx = manager.begin(IsolationLevel.REPEATABLE_READ);
        manager.put(tx, "a", "a" + suffix, new byte[]{1});
        manager.put(tx, "b", "b" + suffix, new byte[]{2});
        manager.commit(tx);
    }

    /** 有界等待确定性交错控制点。 */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timeout");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }

    /** 用真实版本及意向集合模拟持久化协议，故障开关仅控制物化时机。 */
    private static final class MemoryParticipant implements StorageAdapter {
        private final String shardId;
        private final Map<Long, List<Mutation>> intents = new ConcurrentHashMap<>();
        private final Map<String, Map<Long, VersionedValue>> versions = new ConcurrentHashMap<>();
        private final AtomicInteger aborts = new AtomicInteger();
        private final AtomicBoolean failCommit = new AtomicBoolean();
        private final AtomicBoolean failPrewrite = new AtomicBoolean();
        private final AtomicBoolean failAfterLocalCommit = new AtomicBoolean();
        private final Map<Long, ParticipantRecord> localRecords = new ConcurrentHashMap<>();
        private boolean atomicDirect;
        private boolean resolutionEnabled = true;
        private boolean hideLocalDecision;
        private TxStatus reportedLocalStatus;
        private final AtomicBoolean blockNext = new AtomicBoolean();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private TransactionMetadataStore metadata;

        /** 创建指定分片的模拟参与者。 */
        private MemoryParticipant(String shardId) { this.shardId = shardId; }
        /** 返回分片编号。 */
        @Override public String shardId() { return shardId; }
        /** 内存存储不需要建表。 */
        @Override public void initialize() { }
        /** 绑定读取提交决议的权威。 */
        @Override public void configureMetadataStore(TransactionMetadataStore store) { metadata = store; }
        /** 只有绑定元数据后才具备逻辑提交意向读取能力。 */
        @Override public boolean supportsCommittedIntentResolution() { return metadata != null && resolutionEnabled; }
        /** 按测试场景启用本地原子提交。 */
        @Override public boolean supportsAtomicDirectCommit() { return atomicDirect; }
        /** 返回真实模拟持久化本地终态。 */
        @Override public Optional<ParticipantRecord> participantRecord(long txId) {
            if (hideLocalDecision) { return Optional.empty(); }
            return reportedLocalStatus == null ? Optional.ofNullable(localRecords.get(txId))
                    : Optional.of(new ParticipantRecord(txId, txId, reportedLocalStatus, null));
        }
        /** 根据固定时间戳读取最新版本或逻辑提交意向。 */
        @Override public Optional<VersionedValue> get(ReadContext context, String key) {
            VersionedValue result = versions.getOrDefault(key, Map.of()).values().stream()
                    .filter(value -> value.versionTs() <= context.readTs())
                    .max(java.util.Comparator.comparingLong(VersionedValue::versionTs)).orElse(null);
            for (var entry : intents.entrySet()) {
                var record = metadata.get(entry.getKey());
                long timestamp = entry.getKey() == context.txId() ? context.readTs()
                        : record.filter(value -> value.status() == TxStatus.COMMITTED).map(value -> value.commitTs()).orElse(Long.MAX_VALUE);
                if (timestamp > context.readTs()) { continue; }
                for (Mutation mutation : entry.getValue()) {
                    if (mutation.key().equals(key) && (result == null || timestamp >= result.versionTs())) {
                        result = new VersionedValue(key, mutation.value(), mutation.type() == MutationType.DELETE,
                                timestamp, entry.getKey() == context.txId());
                    }
                }
            }
            return Optional.ofNullable(result);
        }
        /** 扫描符合范围的快照版本。 */
        @Override public List<VersionedValue> scan(ReadContext context, KeyRange range) {
            var keys = new TreeMap<String, Boolean>();
            versions.keySet().forEach(key -> keys.put(key, true));
            intents.values().forEach(values -> values.forEach(value -> keys.put(value.key(), true)));
            var result = new ArrayList<VersionedValue>();
            keys.subMap(range.startKeyInclusive(), true, range.endKeyExclusive(), false).keySet().forEach(key ->
                    get(context, key).filter(value -> !value.deleted()).ifPresent(result::add));
            return result;
        }
        /** 保存事务意向集合。 */
        @Override public void prewrite(long txId, long startTs, List<Mutation> mutations) {
            if (failPrewrite.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected prewrite failure", false, txId);
            }
            intents.put(txId, List.copyOf(mutations));
            localRecords.putIfAbsent(txId, new ParticipantRecord(txId, startTs, TxStatus.ACTIVE, null));
        }
        /** 内存意向保存后即已准备完成。 */
        @Override public void prepare(long txId) { }
        /** 按相同时间戳幂等物化意向。 */
        @Override public void commit(long txId, long commitTs) {
            if (failCommit.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected materialization failure", true, txId);
            }
            if (blockNext.getAndSet(false)) { entered.countDown(); await(proceed); }
            for (Mutation mutation : intents.getOrDefault(txId, List.of())) {
                versions.computeIfAbsent(mutation.key(), ignored -> new ConcurrentHashMap<>()).put(commitTs,
                        new VersionedValue(mutation.key(), mutation.value(), mutation.type() == MutationType.DELETE, commitTs, false));
            }
            intents.remove(txId);
            localRecords.put(txId, new ParticipantRecord(txId, txId, TxStatus.COMMITTED, commitTs));
        }
        /** 模拟版本和本地决议原子提交，再注入响应丢失。 */
        @Override public void directCommit(long txId, long startTs, long commitTs, List<Mutation> mutations) {
            prewrite(txId, startTs, mutations);
            commit(txId, commitTs);
            if (failAfterLocalCommit.getAndSet(false)) {
                throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "injected local commit response loss", false, txId);
            }
        }
        /** 清理回滚事务并记录实际调用次数。 */
        @Override public void abort(long txId) {
            ParticipantRecord record = localRecords.get(txId);
            if (record != null && record.status() == TxStatus.COMMITTED) { return; }
            aborts.incrementAndGet();
            intents.remove(txId);
            localRecords.put(txId, new ParticipantRecord(txId, txId, TxStatus.ABORTED, null));
        }
        /** 返回仍有意向的事务。 */
        @Override public Set<Long> findPendingTransactionIds(int limit) { return Set.copyOf(intents.keySet()); }
        /** 本测试存储不执行版本回收。 */
        @Override public int gc(long safePoint, int limit) { return 0; }
    }
}
