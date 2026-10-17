package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.BackgroundTaskStats;
import mvcc.common.MvccException;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.metrics.TransactionMetrics;
import mvcc.publication.SegmentPublicationGcManager;
import mvcc.publication.SegmentPublicationMaintenanceScheduler;
import mvcc.publication.SegmentPublicationRecoveryManager;
import mvcc.publication.SegmentPublicationRecoveryRunner;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;
import mvcc.storage.StorageAdapter;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.tso.mysql.MysqlTimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** 已激活分段协议的显式事务入口及独立后台 owner、恢复、发布和 GC 生命周期。 */
public final class MysqlSegmentTransactionRuntime implements AutoCloseable {
    private final TransactionManager transactionManager;
    private final SegmentPublicationMaintenanceScheduler scheduler;
    private final PublicationSegmentOwnerRegistry owner;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 保存已启动的前台管理器和后台任务。 */
    private MysqlSegmentTransactionRuntime(TransactionManager transactionManager,
                                           SegmentPublicationMaintenanceScheduler scheduler,
                                           PublicationSegmentOwnerRegistry owner) {
        this.transactionManager = transactionManager;
        this.scheduler = scheduler;
        this.owner = owner;
    }

    /** 仅在持久分段模式已切换且前后台连接池独立同权威时启动。 */
    public static MysqlSegmentTransactionRuntime open(DataSource foregroundControl,
                                                      DataSource backgroundControl,
                                                      Map<String, DataSource> foregroundShards,
                                                      Map<String, DataSource> backgroundShards,
                                                      TransactionOptions transactionOptions,
                                                      SegmentRuntimeOptions runtimeOptions,
                                                      TransactionMetrics metrics) {
        Objects.requireNonNull(foregroundControl, "foregroundControl");
        Objects.requireNonNull(backgroundControl, "backgroundControl");
        Objects.requireNonNull(transactionOptions, "transactionOptions");
        Objects.requireNonNull(runtimeOptions, "runtimeOptions");
        Objects.requireNonNull(metrics, "metrics");
        Map<String, DataSource> front = Map.copyOf(foregroundShards);
        Map<String, DataSource> back = Map.copyOf(backgroundShards);
        if (foregroundControl == backgroundControl || front.isEmpty()
                || !front.keySet().equals(back.keySet())
                || !transactionOptions.bufferWrites() || !transactionOptions.lazyBeginMetadata()) {
            throw new IllegalArgumentException("segmented runtime requires buffered lazy transactions and independent pools");
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
        var foregroundLedger = new MysqlSegmentPublicationLedger(foregroundControl);
        var backgroundLedger = new MysqlSegmentPublicationLedger(backgroundControl);
        foregroundLedger.assertActivated();
        backgroundLedger.assertActivated();
        Map<String, StorageAdapter> frontAdapters = new HashMap<>();
        Map<String, StorageAdapter> backAdapters = new HashMap<>();
        front.forEach((shardId, dataSource) -> {
            var adapter = new MysqlStorageAdapter(shardId, dataSource, foregroundMetadata);
            adapter.requireInitializedFastPath();
            if (runtimeOptions.fastCommitProcedure()) {
                adapter.enableSingleKeyCommitV2();
            }
            frontAdapters.put(shardId, adapter);
        });
        back.forEach((shardId, dataSource) -> backAdapters.put(shardId,
                new MysqlStorageAdapter(shardId, dataSource, backgroundMetadata)));
        PublicationSegmentOwnerRegistry owner = null;
        PublicationSegmentCommitBatcher batcher = null;
        SegmentPublicationMaintenanceScheduler scheduler = null;
        TransactionManager manager = null;
        try {
            var recovery = new SegmentPublicationRecoveryManager(backgroundLedger,
                    backgroundMetadata, backAdapters);
            var runner = new SegmentPublicationRecoveryRunner(backgroundLedger,
                    recovery, backgroundMetadata);
            var gc = new SegmentPublicationGcManager(backgroundLedger,
                    backgroundMetadata, backAdapters);
            scheduler = new SegmentPublicationMaintenanceScheduler(runner, backgroundLedger, gc,
                    runtimeOptions.recoveryBatchSize(), runtimeOptions.publicationBatchSize(),
                    runtimeOptions.gcBatchSize(), runtimeOptions.recoveryIntervalMillis(),
                    runtimeOptions.publicationIntervalMillis(), runtimeOptions.gcIntervalMillis(),
                    runtimeOptions.eventDrivenPublication());
            owner = new PublicationSegmentOwnerRegistry(foregroundLedger, backgroundLedger,
                    runtimeOptions.segmentSize(), runtimeOptions.ownerLeaseMillis(),
                    runtimeOptions.ownerRefreshMillis(), runtimeOptions.idleSealMillis());
            if (runtimeOptions.batchDecisions()) {
                batcher = new PublicationSegmentCommitBatcher(owner,
                        transactionOptions.limits().maxActiveTransactions(), 32,
                        runtimeOptions.batchWorkers());
            }
            manager = TransactionManager.forSegment(new MysqlTimestampOracle(foregroundControl),
                    foregroundMetadata, transactionOptions, frontAdapters, metrics,
                    foregroundLedger, owner, scheduler, batcher,
                    runtimeOptions.directSingleShard(), runtimeOptions.overlapMaterialization(),
                    runtimeOptions.fastMaterialization());
            scheduler.start();
            return new MysqlSegmentTransactionRuntime(manager, scheduler, owner);
        } catch (RuntimeException | Error ex) {
            if (manager != null) {
                try {
                    manager.close();
                } catch (RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            } else {
                if (batcher != null) {
                    try {
                        batcher.close(transactionOptions.limits().shutdownTimeoutMillis());
                    } catch (RuntimeException closeFailure) {
                        ex.addSuppressed(closeFailure);
                    }
                }
                if (owner != null) {
                    try {
                        owner.close();
                    } catch (RuntimeException closeFailure) {
                        ex.addSuppressed(closeFailure);
                    }
                }
            }
            if (scheduler != null) {
                try {
                    scheduler.close();
                } catch (RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            throw ex;
        }
    }

    /** 返回显式分段模式的 RC/RR 事务管理器。 */
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    /** 返回后台恢复、发布与 GC 的运行统计。 */
    public SegmentPublicationMaintenanceScheduler maintenanceScheduler() {
        return scheduler;
    }

    /** 返回分段 owner 续租与主动封存轮次的统计。 */
    public BackgroundTaskStats ownerRefreshStats() {
        return owner.refreshStats();
    }

    /** 返回最近一次 owner 续租故障。 */
    public RuntimeException lastOwnerRefreshFailure() {
        return owner.lastRefreshFailure();
    }

    /** 先停止前台事务与分段 owner，再停止发布和恢复；数据源由调用方关闭。 */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                transactionManager.close();
            } finally {
                try {
                    owner.close();
                } finally {
                    scheduler.close();
                }
            }
        }
    }

    /** 校验两套连接池实际落在相同 MySQL 服务器 UUID 和数据库。 */
    private static void requireSameAuthority(DataSource foreground, DataSource background, String route) {
        if (!authority(foreground).equals(authority(background))) {
            throw new IllegalArgumentException("segmented background authority differs from foreground: " + route);
        }
    }

    /** 查询连接实际数据库身份而不相信仅由 JDBC URL 提供的路由名。 */
    private static String authority(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement query = connection.createStatement();
             ResultSet row = query.executeQuery("select @@server_uuid, database()")) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented runtime requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        } catch (SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect segmented runtime database authority failed", false, null, ex);
        }
    }
}
