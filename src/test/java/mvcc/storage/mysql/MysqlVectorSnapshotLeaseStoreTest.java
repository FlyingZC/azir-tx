package mvcc.storage.mysql;

import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 真实 MySQL 控制库验证进程级向量租约与 GC 下界的先后关系。 */
@Testcontainers
class MysqlVectorSnapshotLeaseStoreTest {
    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("vector_reader").withUsername("test").withPassword("test");
    private static HikariDataSource center;
    private MysqlVectorSnapshotLeaseStore store;

    /** 测试只连接本容器的隔离控制数据库。 */
    @BeforeAll
    static void open() {
        center = (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
    }

    /** 关闭本类创建的连接池。 */
    @AfterAll
    static void close() {
        if (center != null) {
            center.close();
        }
    }

    /** 每例显式重建测试专属租约表，生产初始化本身不得清空旧行。 */
    @BeforeEach
    void reset() throws Exception {
        try (Connection connection = center.getConnection(); Statement sql = connection.createStatement()) {
            sql.execute("drop table if exists mvcc_vector_reader_lease_v1");
            sql.execute("drop table if exists mvcc_vector_gc_floor_v1");
        }
        store = new MysqlVectorSnapshotLeaseStore(center, List.of("s1", "s2"), 4);
        store.initializeProduct();
    }

    /** 活跃进程保护旧切面，单调续租后下界可前进，释放后可到当前切面。 */
    @Test
    void activeReaderMustPinAndReleaseVectorGcFloor() {
        try (var guard = store.acquire("reader-a", flat(0), 5_000)) {
            assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            guard.renew(flat(5));
            assertEquals(flat(5), store.advanceSafeFloor(flat(10)));
            assertEquals(flat(5), store.currentSafeFloor());
        }
        assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        MvccException stale = assertThrows(MvccException.class,
                () -> store.acquire("reader-b", flat(5), 5_000));
        assertEquals(ErrorCode.SNAPSHOT_TOO_OLD, stale.errorCode());
    }

    /** 每个分条和中心决议分别取最老有效组件，不能退化成标量界限。 */
    @Test
    void differentStripesMustKeepIndependentFloors() {
        var active = cut(7, List.of(2L, 8L, 3L, 9L), List.of(4L, 6L, 5L, 7L));
        var published = flat(10);
        try (var guard = store.acquire("striped-reader", active, 5_000)) {
            assertEquals(active, store.advanceSafeFloor(published));
        }
        assertEquals(published, store.advanceSafeFloor(published));
    }

    /** 旧进程保守截止后不能续租，GC 可在数据库租约过期后推进。 */
    @Test
    void expiredGuardCannotKeepAnOldSnapshotAlive() throws Exception {
        var guard = store.acquire("expiring-reader", flat(0), 200);
        Thread.sleep(260);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, guard::requireActive).errorCode());
        assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, () -> guard.renew(flat(0))).errorCode());
        guard.close();
    }

    /** 同一 owner 未过期不能被接任，关闭后新代数不能复用旧守卫。 */
    @Test
    void ownerGenerationMustFenceDuplicateAcquisition() {
        var original = store.acquire("same-owner", flat(0), 5_000);
        assertEquals(1, original.generation());
        assertEquals(ErrorCode.ILLEGAL_STATE,
                assertThrows(MvccException.class,
                        () -> store.acquire("same-owner", flat(0), 5_000)).errorCode());
        original.close();
        try (var next = store.acquire("same-owner", flat(0), 5_000)) {
            assertEquals(2, next.generation());
            assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    assertThrows(MvccException.class, original::requireActive).errorCode());
        }
    }

    /** 重启只保留同一路由布局，错误路由集合不能解释已有 GC 下界。 */
    @Test
    void restartPreservesFloorAndRejectsDifferentLayout() {
        assertEquals(flat(3), store.advanceSafeFloor(flat(3)));
        var reopened = new MysqlVectorSnapshotLeaseStore(center, List.of("s2", "s1"), 4);
        reopened.initializeProduct();
        assertEquals(flat(3), reopened.currentSafeFloor());
        var mismatched = new MysqlVectorSnapshotLeaseStore(center, List.of("s1", "s3"), 4);
        assertEquals(ErrorCode.STORAGE_UNAVAILABLE,
                assertThrows(MvccException.class, mismatched::initializeProduct).errorCode());
    }

    /** 注册与 GC 同时争用下界行时，只能保护旧读者或拒绝过旧租约。 */
    @Test
    void leaseRegistrationAndGcAdvanceMustSerialize() throws Exception {
        try (Connection blocker = center.getConnection();
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try (Statement lock = blocker.createStatement();
                 var ignored = lock.executeQuery("""
                         select generation from mvcc_vector_gc_floor_v1
                         where singleton_id = 1 for update
                         """)) {
                assertTrue(ignored.next());
            }
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            var lease = workers.submit(() -> {
                ready.countDown();
                start.await();
                try {
                    return (Object) store.acquire("racing-reader", flat(0), 5_000);
                } catch (MvccException ex) {
                    return ex;
                }
            });
            var advance = workers.submit(() -> {
                ready.countDown();
                start.await();
                return store.advanceSafeFloor(flat(10));
            });
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            blocker.commit();
            Object outcome = lease.get(10, TimeUnit.SECONDS);
            var floor = advance.get(10, TimeUnit.SECONDS);
            if (outcome instanceof MysqlVectorSnapshotLeaseStore.Guard guard) {
                try (guard) {
                    assertEquals(flat(0), floor);
                }
            } else {
                assertEquals(ErrorCode.SNAPSHOT_TOO_OLD,
                        ((MvccException) outcome).errorCode());
                assertEquals(flat(10), floor);
            }
        }
    }

    /** 多笔 RR 与在途 RC 共用一条进程租约，最后旧读者退出后下界才前进。 */
    @Test
    void processRuntimeMustProtectAllActiveReadsWithOneLease() throws Exception {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        try (var runtime = new MysqlVectorReaderProtectionRuntime(
                store, published::get, "one-process", 5_000, 1_000, 100)) {
            runtime.start();
            var rr = runtime.beginRead();
            var rc = runtime.beginRead();
            assertEquals(flat(0), rr.snapshot());
            published.set(flat(10));
            runtime.refresh();
            assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            assertEquals(1, durableLeaseRows());
            rc.close();
            runtime.refresh();
            assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            rr.close();
            runtime.refresh();
            assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
        }
    }

    /** 进程关闭时在途读先失败关闭，但持久租约必须保留到其句柄退出。 */
    @Test
    void closingProcessMustNotReleaseInFlightReadTooEarly() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        var runtime = new MysqlVectorReaderProtectionRuntime(
                store, published::get, "closing-process", 5_000, 1_000, 100);
        runtime.start();
        var reader = runtime.beginRead();
        published.set(flat(10));
        runtime.close();
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, reader::requireActive).errorCode());
        assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
        reader.close();
        assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
    }

    /** 读者数量有界且过旧的发布切面不能重新打开已回收版本。 */
    @Test
    void runtimeMustRejectReaderOverflowAndOldPublishedCut() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        try (var runtime = new MysqlVectorReaderProtectionRuntime(
                store, published::get, "bounded-process", 5_000, 1_000, 1)) {
            runtime.start();
            try (var reader = runtime.beginRead()) {
                assertEquals(ErrorCode.RESOURCE_EXHAUSTED,
                        assertThrows(MvccException.class, runtime::beginRead).errorCode());
            }
            published.set(flat(10));
            runtime.refresh();
            store.advanceSafeFloor(flat(10));
            published.set(flat(5));
            assertEquals(ErrorCode.SNAPSHOT_TOO_OLD,
                    assertThrows(MvccException.class, runtime::beginRead).errorCode());
        }
    }

    /** 中心故障使运行时撤销全部读句柄，而旧租约自然到期前仍保护版本。 */
    @Test
    void failedRefreshMustRevokeLocalReadersWithoutLosingProtection() {
        AtomicReference<MysqlStripedSnapshotOracle.Snapshot> published = new AtomicReference<>(flat(0));
        AtomicReference<Boolean> fail = new AtomicReference<>(false);
        try (var runtime = new MysqlVectorReaderProtectionRuntime(
                store, () -> {
                    if (fail.get()) {
                        throw new IllegalStateException("publisher unavailable");
                    }
                    return published.get();
                }, "failing-process", 5_000, 1_000, 100)) {
            runtime.start();
            try (var reader = runtime.beginRead()) {
                published.set(flat(10));
                fail.set(true);
                assertThrows(IllegalStateException.class, runtime::refresh);
                assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        assertThrows(MvccException.class, reader::requireActive).errorCode());
                assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
            }
        }
    }

    /** 取新切面失败时旧读句柄也必须立即失败关闭，租约继续保留到释放。 */
    @Test
    void failedCutAcquisitionMustRevokeExistingReaders() {
        AtomicReference<Boolean> unavailable = new AtomicReference<>(false);
        var runtime = new MysqlVectorReaderProtectionRuntime(
                store, () -> {
                    if (unavailable.get()) {
                        throw new IllegalStateException("publisher disappeared");
                    }
                    return flat(0);
                }, "cut-failure", 5_000, 1_000, 100);
        runtime.start();
        var old = runtime.beginRead();
        unavailable.set(true);
        assertThrows(IllegalStateException.class, runtime::beginRead);
        assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                assertThrows(MvccException.class, old::requireActive).errorCode());
        assertEquals(flat(0), store.advanceSafeFloor(flat(10)));
        runtime.close();
        old.close();
        assertEquals(flat(10), store.advanceSafeFloor(flat(10)));
    }

    /** 持久租约行数按进程而非事务数量计费。 */
    private long durableLeaseRows() throws Exception {
        try (Connection connection = center.getConnection(); Statement query = connection.createStatement();
             var row = query.executeQuery("select count(*) from mvcc_vector_reader_lease_v1")) {
            row.next();
            return row.getLong(1);
        }
    }

    /** 用相同分量构造最小可重复的两分片向量。 */
    private static MysqlStripedSnapshotOracle.Snapshot flat(long value) {
        return cut(value, List.of(value, value, value, value),
                List.of(value, value, value, value));
    }

    /** 固定路由名构造每条可独立验证的切面。 */
    private static MysqlStripedSnapshotOracle.Snapshot cut(long decision,
                                                            List<Long> first, List<Long> second) {
        return new MysqlStripedSnapshotOracle.Snapshot(
                decision, Map.of("s1", first, "s2", second));
    }
}
