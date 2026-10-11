package mvcc.gc;

import mvcc.common.IsolationLevel;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** 验证 lazy 租约、未物化事务及 begin/GC 的发布屏障竞争。 */
class PublicationGcTest {
    /** GET_LOCK 失效后遗留的持久化未知槽必须阻止 GC 发号和安全点推进。 */
    @Test
    void pendingLocalPublicationBlocksSafePointAdvancement() {
        var metadata = new InMemoryTransactionMetadataStore();
        var safePoint = new InMemorySafePointStore(0);
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), safePoint, Map.of());
        metadata.create(1, 1, IsolationLevel.REPEATABLE_READ);
        metadata.registerParticipants(1, Set.of("a"));
        metadata.markLocalCommitPending(1, 10);
        assertThrows(mvcc.common.MvccException.class, gc::advanceSafePoint);
        assertEquals(0, safePoint.currentSafePoint());
    }

    /** 未决本地提交和未完成中心提交均保护恢复下界，完成后旧元数据不再阻挡。 */
    @Test
    void unfinalizedDecisionPinsGcUntilMaterialization() {
        var metadata = new InMemoryTransactionMetadataStore();
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), new InMemorySafePointStore(0), Map.of());
        metadata.createCommitted(10, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), 20);
        assertEquals(9, gc.advanceSafePoint());
        metadata.markFinalized(10);
        assertTrue(gc.advanceSafePoint() >= 100);
        assertTrue(metadata.get(10).isPresent());
    }

    /** 有界租约清理后剩余过期记录仍保守阻挡，不会超出单轮批次。 */
    @Test
    void expiredLazySnapshotsAreRemovedInBoundedBatches() {
        var metadata = new InMemoryTransactionMetadataStore();
        metadata.registerSnapshot(1, 1, 1);
        metadata.registerSnapshot(2, 2, 1);
        var gc = new GcManager(metadata, new AtomicTimestampOracle(100), new InMemorySafePointStore(0), Map.of(), 1, 0, 1000);
        assertEquals(1, gc.advanceSafePoint());
        assertEquals(2L, metadata.oldestActiveStartTs().orElseThrow());
        assertTrue(gc.advanceSafePoint() >= 100);
    }

    /** 分配 startTs 后即使登记暂时暂停，GC 也只能在登记完成后看到保护下界。 */
    @Test
    void gcCannotPassSnapshotAllocatedInsidePublicationLock() throws Exception {
        var metadata = new InMemoryTransactionMetadataStore();
        var tso = new AtomicTimestampOracle(100);
        var gc = new GcManager(metadata, tso, new InMemorySafePointStore(0), Map.of());
        var allocated = new CountDownLatch(1);
        var gcStarted = new CountDownLatch(1);
        var register = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var begin = executor.submit(() -> metadata.withPublicationLock(() -> {
                long startTs = tso.nextTimestamp();
                allocated.countDown();
                await(register);
                metadata.registerSnapshot(startTs, startTs, Long.MAX_VALUE);
                return startTs;
            }));
            try {
                assertTrue(allocated.await(5, TimeUnit.SECONDS));
                var advance = executor.submit(() -> {
                    gcStarted.countDown();
                    return gc.advanceSafePoint();
                });
                assertTrue(gcStarted.await(5, TimeUnit.SECONDS));
                assertFalse(advance.isDone());
                register.countDown();
                assertEquals(begin.get(5, TimeUnit.SECONDS) - 1, advance.get(5, TimeUnit.SECONDS));
            } finally {
                register.countDown();
            }
        }
    }

    /** 用有界等待构造确定性竞争并保留中断语义。 */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("latch timed out");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ex);
        }
    }
}
