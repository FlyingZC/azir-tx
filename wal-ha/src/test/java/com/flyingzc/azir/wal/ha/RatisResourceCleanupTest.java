package com.flyingzc.azir.wal.ha;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** 核对失败关闭的重试、依赖保护与原始异常，避免仅以关闭标志推断释放完成。 */
class RatisResourceCleanupTest {
    /** 首次释放失败后仍须执行第二次真实释放，成功后重复关闭不再执行。 */
    @Test void leaseReleaseFailureRemainsRetryable() throws Exception {
        var calls = new AtomicInteger();
        var lease = new WalReplicaBackendFactory.Lease(Map.of(), () -> {
            if (calls.incrementAndGet() == 1) { throw new IOException("first close failed"); }
        });
        assertThrows(IOException.class, lease::close);
        lease.close(); lease.close();
        assertEquals(2, calls.get());
    }

    /** 前置释放失败时保留命名空间保护，池释放成功后才允许退休。 */
    @Test void failedPoolCloseCannotReleaseNamespaceProtection() throws Exception {
        var keys = List.of("cleanup-test-" + UUID.randomUUID());
        var guard = MysqlNamespaceLeases.open(keys);
        var allowed = new AtomicBoolean();
        var sequence = new RatisCloseSequence(() -> {
            if (!allowed.get()) { throw new IOException("pool still running"); }
        }, guard::close);
        try {
            assertThrows(IOException.class, sequence::close);
            assertThrows(IOException.class, () -> MysqlNamespaceLeases.retire(keys));
        } finally { allowed.set(true); sequence.close(); }
        try (var retired = MysqlNamespaceLeases.retire(keys)) { assertNotNull(retired); }
    }

    /** 顺序清理保留失败阶段，不跳过依赖，也不重复执行已经完成的阶段。 */
    @Test void cleanupRetriesOnlyTheIncompleteSuffix() throws Exception {
        var events = new ArrayList<String>();
        var calls = new AtomicInteger();
        var sequence = new RatisCloseSequence(() -> events.add("producer-stopped"), () -> {
            if (calls.incrementAndGet() == 1) { events.add("consumer-busy"); throw new IOException("busy"); }
            events.add("consumer-drained");
        }, () -> events.add("storage-closed"));
        assertThrows(IOException.class, sequence::close);
        assertEquals(List.of("producer-stopped", "consumer-busy"), events);
        sequence.close(); sequence.close();
        assertEquals(List.of("producer-stopped", "consumer-busy", "consumer-drained", "storage-closed"), events);
    }

    /** 内层未释放时外层资源不能提前关闭，重试完成后仍保留最初启动原因。 */
    @Test void nestedStartupCleanupPreservesCauseAndDependencyOrder() throws Exception {
        var primary = new IOException("startup failed");
        var allowed = new AtomicBoolean();
        var outerClosed = new AtomicBoolean();
        var nested = new RatisStartupException(primary, () -> {
            if (!allowed.get()) { throw new IOException("inner resource busy"); }
        });
        var failure = RatisStartupException.afterFailure(nested, () -> outerClosed.set(true));
        var pending = assertInstanceOf(RatisStartupException.class, failure);
        assertSame(primary, pending.getCause().getCause());
        assertTrue(pending.cleanupPending()); assertFalse(outerClosed.get());
        allowed.set(true); pending.retryCleanup(); pending.retryCleanup();
        assertFalse(pending.cleanupPending()); assertFalse(nested.cleanupPending()); assertTrue(outerClosed.get());
    }
}
