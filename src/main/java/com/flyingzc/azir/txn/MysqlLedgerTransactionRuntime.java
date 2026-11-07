package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.gc.NoOpSafePointStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metrics.TransactionMetrics;
import com.flyingzc.azir.publication.PublicationGcManager;
import com.flyingzc.azir.publication.PublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.PublicationRecoveryManager;
import com.flyingzc.azir.publication.PublicationRecoveryRunner;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** 显式装配账本事务入口与独立连接预算的后台恢复、GC 生命周期。 */
public final class MysqlLedgerTransactionRuntime implements AutoCloseable {
    private final TransactionManager transactionManager;
    private final PublicationMaintenanceScheduler maintenanceScheduler;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 保存已启动的事务入口和后台任务。 */
    private MysqlLedgerTransactionRuntime(TransactionManager transactionManager,
                                          PublicationMaintenanceScheduler maintenanceScheduler) {
        this.transactionManager = transactionManager;
        this.maintenanceScheduler = maintenanceScheduler;
    }

    /**
     * 在已激活账本的数据库上装配运行时；数据源由调用方持有，后台池必须独立且指向相同库。
     */
    public static MysqlLedgerTransactionRuntime open(DataSource foregroundControl,
                                                     DataSource backgroundControl,
                                                     Map<String, DataSource> foregroundShards,
                                                     Map<String, DataSource> backgroundShards,
                                                     TransactionOptions transactionOptions,
                                                     LedgerRuntimeOptions runtimeOptions,
                                                     TransactionMetrics metrics) {
        Objects.requireNonNull(foregroundControl, "foregroundControl");
        Objects.requireNonNull(backgroundControl, "backgroundControl");
        Objects.requireNonNull(transactionOptions, "transactionOptions");
        Objects.requireNonNull(runtimeOptions, "runtimeOptions");
        Objects.requireNonNull(metrics, "metrics");
        Map<String, DataSource> front = Map.copyOf(foregroundShards);
        Map<String, DataSource> back = Map.copyOf(backgroundShards);
        if (foregroundControl == backgroundControl || front.isEmpty() || !front.keySet().equals(back.keySet())) {
            throw new IllegalArgumentException("ledger runtime requires matching routes and independent pools");
        }
        requireSameAuthority(foregroundControl, backgroundControl, "control");
        for (String shardId : front.keySet()) {
            if (front.get(shardId) == back.get(shardId)) {
                throw new IllegalArgumentException("background shard pool must be independent: " + shardId);
            }
            requireSameAuthority(front.get(shardId), back.get(shardId), shardId);
        }
        var foregroundMetadata = new MysqlTransactionMetadataStore(foregroundControl);
        var backgroundMetadata = new MysqlTransactionMetadataStore(backgroundControl);
        var foregroundLedger = new MysqlPublicationLedger(foregroundControl);
        var backgroundLedger = new MysqlPublicationLedger(backgroundControl);
        foregroundLedger.assertActivated();
        backgroundLedger.assertActivated();
        Map<String, StorageAdapter> frontAdapters = new HashMap<>();
        Map<String, StorageAdapter> backAdapters = new HashMap<>();
        front.forEach((shardId, dataSource) -> {
            var adapter = new MysqlStorageAdapter(shardId, dataSource, foregroundMetadata);
            adapter.requireInitializedFastPath();
            frontAdapters.put(shardId, adapter);
        });
        back.forEach((shardId, dataSource) -> backAdapters.put(shardId,
                new MysqlStorageAdapter(shardId, dataSource, backgroundMetadata)));
        TransactionManager manager = null;
        PublicationMaintenanceScheduler scheduler = null;
        CoordinatorSnapshotRegistry registry = null;
        try {
            if (runtimeOptions.coordinatorSnapshots()) {
                registry = new CoordinatorSnapshotRegistry(foregroundLedger, backgroundLedger,
                        transactionOptions.limits().timeoutMillis(), runtimeOptions.coordinatorLeaseMillis(),
                        runtimeOptions.coordinatorRefreshIntervalMillis());
            }
            manager = new TransactionManager(new MysqlTimestampOracle(foregroundControl), foregroundMetadata,
                    NoOpSafePointStore.INSTANCE, transactionOptions, frontAdapters, metrics,
                    foregroundLedger, runtimeOptions.batchControlCommit(), runtimeOptions.batchWorkers(),
                    runtimeOptions.directSingleShard(), runtimeOptions.batchBegin(), runtimeOptions.beginWorkers(),
                    registry);
            var recovery = new PublicationRecoveryManager(backgroundLedger, backgroundMetadata, backAdapters);
            var runner = new PublicationRecoveryRunner(backgroundLedger, recovery, backgroundMetadata);
            var gc = new PublicationGcManager(backgroundLedger, backgroundMetadata, backAdapters);
            scheduler = new PublicationMaintenanceScheduler(runner, gc,
                    runtimeOptions.recoveryBatchSize(), runtimeOptions.gcBatchSize(),
                    runtimeOptions.recoveryIntervalMillis(), runtimeOptions.gcIntervalMillis());
            scheduler.start();
            return new MysqlLedgerTransactionRuntime(manager, scheduler);
        } catch (RuntimeException | Error ex) {
            if (scheduler != null) {
                try {
                    scheduler.close();
                } catch (RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            if (manager != null) {
                try {
                    manager.close();
                } catch (RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            if (manager == null && registry != null) {
                try {
                    registry.close();
                } catch (RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            throw ex;
        }
    }

    /** 返回已启用 RC/RR 账本协议的事务管理器。 */
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    /** 返回恢复和 GC 的周期统计与最近失败原因。 */
    public PublicationMaintenanceScheduler maintenanceScheduler() {
        return maintenanceScheduler;
    }

    /** 先停止后台任务，再关闭前台事务执行器；数据源由创建者关闭。 */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                maintenanceScheduler.close();
            } finally {
                transactionManager.close();
            }
        }
    }

    /** 比较服务器 UUID 和所选数据库，拒绝后台池误接到不同权威。 */
    private static void requireSameAuthority(DataSource foreground, DataSource background, String route) {
        if (!authority(foreground).equals(authority(background))) {
            throw new IllegalArgumentException("background database authority differs from foreground: " + route);
        }
    }

    /** 读取一条连接实际所属的 MySQL 服务器和数据库。 */
    private static String authority(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement query = connection.createStatement();
             ResultSet row = query.executeQuery("select @@server_uuid, database()")) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "ledger runtime datasource requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect ledger runtime database authority failed", false, null, ex);
        }
    }
}
