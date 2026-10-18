package mvcc.maintenance;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** 验证后台失败可恢复、资源关闭可等待以及预算边界。 */
class MaintenanceServiceTest {
    /** 异常不能永久取消调度，成功后应清除最近错误并保留累计失败。 */
    @Test void failureIsObservableAndNextRoundRetries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var service = new MaintenanceService("test-retry", new MaintenanceOptions(10, 3, 1000), limit -> {
            assertEquals(3, limit);
            if (calls.incrementAndGet() == 1) { throw new IllegalStateException("injected maintenance failure"); }
            return new MaintenanceResult(2, 3, 10);
        })) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (service.health().successes() == 0 && System.nanoTime() < deadline) { Thread.sleep(5); }
            assertTrue(service.health().successes() > 0); assertEquals(1, service.health().failures());
            assertNull(service.health().lastError()); assertEquals(10, service.health().lastResult().safePoint());
        }
    }
    /** 阻塞 JDBC 没有响应中断时，关闭必须报超时而不能假称已经结束。 */
    @Test void closeTimeoutCanBeRetriedAfterTaskExits() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var service = new MaintenanceService("test-close", new MaintenanceOptions(10, 1, 20), limit -> {
            entered.countDown();
            boolean interrupted = false;
            while (release.getCount() != 0) {
                try { release.await(); } catch (InterruptedException failure) { interrupted = true; }
            }
            if (interrupted) { Thread.currentThread().interrupt(); }
            return new MaintenanceResult(0, 0, 0);
        });
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, service::close);
        } finally { release.countDown(); }
        service.close(); service.close();
    }
    /** 配置错误在后台线程创建前就应失败。 */
    @Test void invalidBudgetsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceOptions(0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceOptions(10, 4097, 1));
        assertThrows(IllegalArgumentException.class, () -> new MaintenanceResult(-1, 0, 0));
    }
}
