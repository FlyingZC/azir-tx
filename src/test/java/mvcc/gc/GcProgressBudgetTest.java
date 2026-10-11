package mvcc.gc;

import mvcc.common.IsolationLevel;
import mvcc.metadata.InMemoryTransactionMetadataStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.storage.StorageAdapter;
import mvcc.tso.AtomicTimestampOracle;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** 使用可控单调时钟验证小预算下分片和元数据回收的最低进展。 */
class GcProgressBudgetTest {
    /** 控制阶段和每个工作单元均超过一毫秒，轮转仍覆盖所有分片及元数据。 */
    @Test
    void oneMillisecondBudgetStillRotatesThroughAllWork() {
        var fixture = new Fixture();
        fixture.records.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), 2);
        fixture.records.markFinalized(1);
        var gc = fixture.manager(Map.of("a", fixture.adapter("a", false), "b", fixture.adapter("b", false)));
        assertEquals(1, gc.gcOnce(1));
        assertEquals(1, gc.gcOnce(1));
        assertEquals(0, gc.gcOnce(1));
        assertEquals(List.of("a", "b", "metadata"), fixture.work);
        assertTrue(fixture.records.get(1).isEmpty());
        assertEquals(1, gc.gcOnce(1));
        assertEquals(List.of("a", "b", "metadata", "a"), fixture.work);
    }

    /** 某分片持续失败时游标也必须前移，不能饿死其他分片或元数据清理。 */
    @Test
    void failingShardDoesNotKeepTheNextRoundOnTheSameWork() {
        var fixture = new Fixture();
        var gc = fixture.manager(Map.of("a", fixture.adapter("a", true), "b", fixture.adapter("b", false)));
        assertThrows(IllegalStateException.class, () -> gc.gcOnce(1));
        assertEquals(1, gc.gcOnce(1));
        assertEquals(0, gc.gcOnce(1));
        assertEquals(List.of("a", "b", "metadata"), fixture.work);
    }

    /** 没有参与分片时，长控制阶段后仍必须执行一个元数据清理批次。 */
    @Test
    void metadataOnlyGcAlwaysAttemptsItsWorkUnit() {
        var fixture = new Fixture();
        var gc = fixture.manager(Map.of());
        assertEquals(0, gc.gcOnce(1));
        assertEquals(List.of("metadata"), fixture.work);
    }

    /** 通过调用行为推进虚拟时间，不依赖休眠、线程调度或真实数据库。 */
    private static final class Fixture {
        private final AtomicLong nanos = new AtomicLong();
        private final InMemoryTransactionMetadataStore records = new InMemoryTransactionMetadataStore();
        private final List<String> work = new ArrayList<>();

        /** 构造一毫秒预算的 GC，控制阶段固定消耗十毫秒。 */
        private GcManager manager(Map<String, StorageAdapter> adapters) {
            var watermark = new InMemorySafePointStore(0);
            SafePointStore slowControl = new SafePointStore() {
                /** 返回真实测试水位。 */
                @Override
                public long currentSafePoint() { return watermark.currentSafePoint(); }

                /** 模拟慢控制面，但不消耗真实时间。 */
                @Override
                public long advanceTo(long candidate) {
                    nanos.addAndGet(10_000_000);
                    return watermark.advanceTo(candidate);
                }
            };
            return new GcManager(metadata(), new AtomicTimestampOracle(100), slowControl, adapters, 1, 0, 1, nanos::get);
        }

        /** 保留真实元数据行为，只记录清理工作并提供固定未来墙钟。 */
        private TransactionMetadataStore metadata() {
            return (TransactionMetadataStore) Proxy.newProxyInstance(TransactionMetadataStore.class.getClassLoader(),
                    new Class<?>[]{TransactionMetadataStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("currentTimeMillis")) {
                            return Long.MAX_VALUE;
                        }
                        if (method.getName().equals("purgeFinalized")) {
                            work.add("metadata");
                            nanos.addAndGet(2_000_000);
                        }
                        try {
                            return method.invoke(records, args);
                        } catch (InvocationTargetException ex) {
                            throw ex.getCause();
                        }
                    });
        }

        /** 模拟一个必定耗尽本轮剩余预算的分片工作单元。 */
        private StorageAdapter adapter(String shardId, boolean fail) {
            return (StorageAdapter) Proxy.newProxyInstance(StorageAdapter.class.getClassLoader(), new Class<?>[]{StorageAdapter.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("shardId")) {
                            return shardId;
                        }
                        if (method.getName().equals("gc")) {
                            work.add(shardId);
                            nanos.addAndGet(2_000_000);
                            if (fail) {
                                throw new IllegalStateException("shard unavailable");
                            }
                            return 1;
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
