package mvcc.txn;

import mvcc.api.Transaction;
import mvcc.api.TransactionCommitMode;
import mvcc.api.TransactionCommitResult;
import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.common.TxStatus;
import mvcc.gc.NoOpSafePointStore;
import mvcc.gc.SafePointStore;
import mvcc.metadata.TransactionMetadataStore;
import mvcc.metadata.mysql.MysqlTransactionMetadataStore;
import mvcc.metadata.BeginReservation;
import mvcc.metadata.FastTransactionBegin;
import mvcc.metadata.FastTransactionCommit;
import mvcc.metadata.TxRecord;
import mvcc.metrics.NoOpTransactionMetrics;
import mvcc.metrics.TransactionMetrics;
import mvcc.metrics.TransactionStage;
import mvcc.publication.mysql.MysqlPublicationLedger;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger;
import mvcc.publication.mysql.MysqlSegmentPublicationLedger.CommittedProof;
import mvcc.publication.SegmentPublicationMaintenanceScheduler;
import mvcc.publication.PublicationEntry;
import mvcc.publication.PublicationEntryStatus;
import mvcc.publication.PublicationCommitResult;
import mvcc.storage.KeyRange;
import mvcc.storage.Mutation;
import mvcc.storage.ReadContext;
import mvcc.storage.StorageAdapter;
import mvcc.storage.mysql.MysqlStorageAdapter;
import mvcc.storage.VersionedValue;
import mvcc.storage.ParticipantRecord;
import mvcc.storage.ScanPage;
import mvcc.tso.TimestampOracle;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 事务管理器，协调 TSO、元数据和分片存储完成 MVCC 事务。
 */
public final class TransactionManager implements AutoCloseable {
    private final TimestampOracle timestampOracle;
    private final TransactionMetadataStore metadataStore;
    private final SafePointStore safePointStore;
    private final Map<String, StorageAdapter> adapters;
    private final TransactionOptions options;
    private final TransactionClassifier transactionClassifier;
    private final TransactionMetrics metrics;
    private final MysqlPublicationLedger publicationLedger;
    private final MysqlSegmentPublicationLedger segmentLedger;
    private final PublicationSegmentOwnerRegistry segmentOwner;
    private final SegmentPublicationMaintenanceScheduler segmentScheduler;
    private final PublicationSegmentCommitBatcher segmentCommitBatcher;
    private final boolean segmentDirectSingleShard;
    private final boolean segmentOverlapMaterialization;
    private final boolean segmentFastMaterialization;
    private final CoordinatorSnapshotRegistry coordinatorRegistry;
    private final boolean ledgerDirectSingleShard;
    private final PublicationCommitBatcher publicationCommitBatcher;
    private final PublicationLocalReserveBatcher localReserveBatcher;
    private final PublicationLocalCompletionBatcher localCompletionBatcher;
    private final PublicationBeginBatcher beginBatcher;
    private final ExecutorService participantExecutor;
    private final ExecutorService asyncCommitExecutor;
    private final ExecutorService intentCleanupExecutor;
    private final Map<String, Semaphore> shardPermits = new HashMap<>();
    private final Semaphore transactionPermits;
    private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder synchronousFallbacks = new LongAdder();
    private final LongAdder backgroundFailures = new LongAdder();
    private final Object transactionOwner = new Object();

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(), adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, TransactionOptions.defaults(), adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters,
                              TransactionMetrics metrics) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters, metrics);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, NoOpTransactionMetrics.INSTANCE);
    }

    /**
     * 构造 TransactionManager 实例。
     */
    public TransactionManager(TimestampOracle timestampOracle,
                              TransactionMetadataStore metadataStore,
                              SafePointStore safePointStore,
                              TransactionOptions options,
                              Map<String, StorageAdapter> adapters,
                              TransactionMetrics metrics) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics, null);
    }

    /** 新账本模式仅在控制面权威一致且写入可解析已提交 intent 时构造。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics, publicationLedger, false);
    }

    /** 压测和隔离验证可选择启用有界账本批量决议，默认路径待验收后再切换。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger,
                       boolean batchLedgerCommits) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, 2);
    }

    /** 隔离压测可调整批量决议工作线程数，生产入口仍待长稳验收。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger,
                       boolean batchLedgerCommits,
                       int batchWorkers) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, false);
    }

    /** 仅显式启用已验证的单分片本地直接提交，默认保持吞吐较高的 intent 路径。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger,
                       boolean batchLedgerCommits,
                       int batchWorkers,
                       boolean ledgerDirectSingleShard) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard, false, 1);
    }

    /** 批量快照开始仅由显式实验选项启用，默认保持已验证的单笔 begin。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger,
                       boolean batchLedgerCommits,
                       int batchWorkers,
                       boolean ledgerDirectSingleShard,
                       boolean batchLedgerBegins,
                       int beginWorkers) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard,
                batchLedgerBegins, beginWorkers, null);
    }

    /** 协调者快照租约仅由显式运行时装配，不能与批量开始同时启用。 */
    TransactionManager(TimestampOracle timestampOracle,
                       TransactionMetadataStore metadataStore,
                       SafePointStore safePointStore,
                       TransactionOptions options,
                       Map<String, StorageAdapter> adapters,
                       TransactionMetrics metrics,
                       MysqlPublicationLedger publicationLedger,
                       boolean batchLedgerCommits,
                       int batchWorkers,
                       boolean ledgerDirectSingleShard,
                       boolean batchLedgerBegins,
                       int beginWorkers,
                       CoordinatorSnapshotRegistry coordinatorRegistry) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard,
                batchLedgerBegins, beginWorkers, coordinatorRegistry, null, null, null, null,
                false, false, false);
    }

    /** 显式分段模式仅与已激活的新时间线和同库元数据权威装配。 */
    static TransactionManager forSegment(TimestampOracle timestampOracle,
                                         TransactionMetadataStore metadataStore,
                                         TransactionOptions options,
                                         Map<String, StorageAdapter> adapters,
                                         TransactionMetrics metrics,
                                         MysqlSegmentPublicationLedger segmentLedger,
                                         PublicationSegmentOwnerRegistry segmentOwner,
                                         SegmentPublicationMaintenanceScheduler segmentScheduler) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, null);
    }

    /** 允许显式实验运行时提供有界同分段批量决议队列。 */
    static TransactionManager forSegment(TimestampOracle timestampOracle,
                                         TransactionMetadataStore metadataStore,
                                         TransactionOptions options,
                                         Map<String, StorageAdapter> adapters,
                                         TransactionMetrics metrics,
                                         MysqlSegmentPublicationLedger segmentLedger,
                                         PublicationSegmentOwnerRegistry segmentOwner,
                                         SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         PublicationSegmentCommitBatcher segmentCommitBatcher) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher, false);
    }

    /** 分段单分片物理 1PC 仅由已具备本地终态恢复的显式运行时启用。 */
    static TransactionManager forSegment(TimestampOracle timestampOracle,
                                         TransactionMetadataStore metadataStore,
                                         TransactionOptions options,
                                         Map<String, StorageAdapter> adapters,
                                         TransactionMetrics metrics,
                                         MysqlSegmentPublicationLedger segmentLedger,
                                         PublicationSegmentOwnerRegistry segmentOwner,
                                         SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         boolean segmentDirectSingleShard) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, false);
    }

    /** 分片物化与全局发布等待仅在并行提交策略下显式重叠。 */
    static TransactionManager forSegment(TimestampOracle timestampOracle,
                                         TransactionMetadataStore metadataStore,
                                         TransactionOptions options,
                                         Map<String, StorageAdapter> adapters,
                                         TransactionMetrics metrics,
                                         MysqlSegmentPublicationLedger segmentLedger,
                                         PublicationSegmentOwnerRegistry segmentOwner,
                                         SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         boolean segmentDirectSingleShard,
                                         boolean segmentOverlapMaterialization) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, segmentOverlapMaterialization, false);
    }

    /** 分段中心决议凭据只向同步、非批量的 MySQL 参与者开放。 */
    static TransactionManager forSegment(TimestampOracle timestampOracle,
                                         TransactionMetadataStore metadataStore,
                                         TransactionOptions options,
                                         Map<String, StorageAdapter> adapters,
                                         TransactionMetrics metrics,
                                         MysqlSegmentPublicationLedger segmentLedger,
                                         PublicationSegmentOwnerRegistry segmentOwner,
                                         SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         boolean segmentDirectSingleShard,
                                         boolean segmentOverlapMaterialization,
                                         boolean segmentFastMaterialization) {
        return new TransactionManager(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE,
                options, adapters, metrics, null, false, 1, false, false, 1,
                null, segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, segmentOverlapMaterialization, segmentFastMaterialization);
    }

    /** 旧账本与分段账本只允许其中一个承担全局读写时间线。 */
    private TransactionManager(TimestampOracle timestampOracle,
                               TransactionMetadataStore metadataStore,
                               SafePointStore safePointStore,
                               TransactionOptions options,
                               Map<String, StorageAdapter> adapters,
                               TransactionMetrics metrics,
                               MysqlPublicationLedger publicationLedger,
                               boolean batchLedgerCommits,
                               int batchWorkers,
                               boolean ledgerDirectSingleShard,
                               boolean batchLedgerBegins,
                               int beginWorkers,
                               CoordinatorSnapshotRegistry coordinatorRegistry,
                               MysqlSegmentPublicationLedger segmentLedger,
                               PublicationSegmentOwnerRegistry segmentOwner,
                               SegmentPublicationMaintenanceScheduler segmentScheduler,
                               PublicationSegmentCommitBatcher segmentCommitBatcher,
                               boolean segmentDirectSingleShard,
                               boolean segmentOverlapMaterialization,
                               boolean segmentFastMaterialization) {
        if (coordinatorRegistry != null && (publicationLedger == null || batchLedgerBegins)) {
            throw new IllegalArgumentException("coordinator snapshots require ledger mode without batch begin");
        }
        if (segmentLedger != null && (publicationLedger != null || segmentOwner == null
                || segmentScheduler == null || !options.lazyBeginMetadata() || !options.bufferWrites()
                || !segmentLedger.sharesMetadataAuthority(metadataStore))
                || segmentLedger == null && (segmentOwner != null || segmentScheduler != null
                || segmentCommitBatcher != null || segmentDirectSingleShard
                || segmentOverlapMaterialization || segmentFastMaterialization)
                || segmentOverlapMaterialization && !options.crossShardCommitPolicy().parallelCommit()) {
            throw new IllegalArgumentException("segmented mode requires one buffered MySQL control authority");
        }
        if (segmentFastMaterialization && (segmentCommitBatcher != null || segmentOverlapMaterialization
                || options.crossShardCommitPolicy().asyncCommit()
                || adapters.values().stream().anyMatch(adapter -> !(adapter instanceof MysqlStorageAdapter)))) {
            throw new IllegalArgumentException("fast materialization requires synchronous MySQL segment decisions");
        }
        this.timestampOracle = timestampOracle;
        this.metadataStore = metadataStore;
        this.safePointStore = safePointStore;
        this.options = options;
        this.transactionClassifier = new TransactionClassifier();
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.publicationLedger = publicationLedger;
        this.segmentLedger = segmentLedger;
        this.segmentOwner = segmentOwner;
        this.segmentScheduler = segmentScheduler;
        this.segmentCommitBatcher = segmentCommitBatcher;
        this.segmentDirectSingleShard = segmentDirectSingleShard;
        this.segmentOverlapMaterialization = segmentOverlapMaterialization;
        this.segmentFastMaterialization = segmentFastMaterialization;
        this.coordinatorRegistry = coordinatorRegistry;
        this.ledgerDirectSingleShard = ledgerDirectSingleShard;
        this.adapters = Map.copyOf(adapters);
        if (publicationLedger != null) {
            if (!options.lazyBeginMetadata() || !options.bufferWrites()
                    || !publicationLedger.sharesMetadataAuthority(metadataStore)
                    || !publicationLedger.sharesTimestampAuthority(timestampOracle)) {
                throw new IllegalArgumentException("ledger mode requires buffered lazy transactions and one MySQL authority");
            }
            publicationLedger.assertActivated();
            publicationLedger.currentPublished();
            this.adapters.forEach((shardId, adapter) -> {
                if (!adapter.supportsCommittedIntentResolution()) {
                    throw new IllegalArgumentException("ledger participant cannot resolve committed intents: " + shardId);
                }
            });
        }
        if (segmentLedger != null) {
            segmentLedger.assertActivated();
            this.adapters.forEach((shardId, adapter) -> {
                if (!adapter.supportsCommittedIntentResolution()) {
                    throw new IllegalArgumentException("segment participant cannot resolve committed intents: " + shardId);
                }
            });
        }
        if (publicationLedger == null && segmentLedger == null
                && metadataStore instanceof MysqlTransactionMetadataStore mysqlMetadata) {
            // 旧管理器在构造期拒绝已切换的持久模式，避免每笔 begin 才碰到租约外键栅栏。
            mysqlMetadata.assertLegacyProtocolActive();
        }
        this.transactionPermits = new Semaphore(options.limits().maxActiveTransactions());
        this.adapters.forEach((shardId, adapter) -> {
            if (shardId.isBlank() || shardId.contains(",") || !shardId.equals(adapter.shardId())) {
                throw new IllegalArgumentException("shard id must match its adapter and cannot be blank or contain commas");
            }
            adapter.configureMetadataStore(metadataStore);
            shardPermits.put(shardId, new Semaphore(options.limits().shardConcurrency(), true));
        });
        this.participantExecutor = requiresParallelExecutor(options.crossShardCommitPolicy())
                ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                        .name("mvcc-participant-", 0)
                        .factory())
                : null;
        this.asyncCommitExecutor = options.crossShardCommitPolicy().asyncCommit()
                ? boundedExecutor("mvcc-async-commit-")
                : null;
        this.intentCleanupExecutor = options.crossShardCommitPolicy().asyncCommit() && options.deferIntentCleanup()
                ? boundedExecutor("mvcc-intent-cleanup-")
                : null;
        this.publicationCommitBatcher = publicationLedger != null && batchLedgerCommits
                ? new PublicationCommitBatcher(publicationLedger, options.limits().maxActiveTransactions(),
                        32, batchWorkers)
                : null;
        this.localReserveBatcher = publicationLedger != null && ledgerDirectSingleShard && batchLedgerCommits
                ? new PublicationLocalReserveBatcher(publicationLedger, options.limits().maxActiveTransactions(),
                        32, batchWorkers)
                : null;
        this.localCompletionBatcher = publicationLedger != null && ledgerDirectSingleShard && batchLedgerCommits
                ? new PublicationLocalCompletionBatcher(publicationLedger, options.limits().maxActiveTransactions(),
                        32, batchWorkers)
                : null;
        this.beginBatcher = publicationLedger != null && batchLedgerBegins
                ? new PublicationBeginBatcher(publicationLedger, options.limits().timeoutMillis(),
                        options.limits().maxActiveTransactions(), 32, beginWorkers)
                : null;
    }

    /**
     * 开启一个新事务。
     */
    public Transaction begin(IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel");
        ensureOpen();
        expireLocalTransactions();
        if (!transactionPermits.tryAcquire()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "active transaction limit reached", true, null);
        }
        Transaction registered = null;
        try {
            Transaction transaction = segmentLedger != null ? beginFromSegment(isolationLevel)
                    : publicationLedger == null
                    ? acceleratedBegin(isolationLevel).orElseGet(() -> legacyBegin(isolationLevel))
                    : beginFromLedger(isolationLevel);
            activeTransactions.put(transaction.txId(), transaction);
            registered = transaction;
            ensureOpen();
            return transaction;
        } catch (RuntimeException | Error ex) {
            if (registered == null || activeTransactions.remove(registered.txId(), registered)) {
                transactionPermits.release();
            }
            if (registered != null && coordinatorRegistry != null && registered.txId() < 0) {
                coordinatorRegistry.release(registered.txId());
            }
            throw ex;
        }
    }

    /** 共享水位快照和新租约必须在同一控制事务中持久化后才创建事务对象。 */
    private Transaction beginFromLedger(IsolationLevel isolationLevel) {
        BeginReservation reservation = ledgerBeginStage(isolationLevel);
        return new Transaction(reservation.txId(), reservation.startTs(), isolationLevel,
                reservation.deadlineMillis(), transactionOwner);
    }

    /** 分段模式的开始快照与逐事务租约同事务持久化后才暴露事务对象。 */
    private Transaction beginFromSegment(IsolationLevel isolationLevel) {
        BeginReservation reservation = segmentBeginStage(isolationLevel);
        return new Transaction(reservation.txId(), reservation.startTs(), isolationLevel,
                reservation.deadlineMillis(), transactionOwner);
    }

    /** 仅在时间戳和元数据确定共享一个控制面权威时启用合并发布。 */
    private Optional<Transaction> acceleratedBegin(IsolationLevel isolationLevel) {
        if (!(metadataStore instanceof FastTransactionBegin fastBegin)) {
            return Optional.empty();
        }
        var action = (Supplier<Optional<BeginReservation>>) () -> fastBegin.tryBegin(timestampOracle,
                isolationLevel, options.limits().timeoutMillis(), !options.lazyBeginMetadata(), this::ensureOpen);
        Optional<BeginReservation> reservation = acceleratedBeginStage(action);
        return reservation.map(value -> {
            Transaction transaction = new Transaction(value.txId(), value.startTs(), isolationLevel,
                    value.deadlineMillis(), transactionOwner);
            transaction.metadataPersisted(value.metadataPersisted());
            return transaction;
        });
    }

    /** 非 MySQL 或不同控制面权威继续使用原有发布协议。 */
    private Transaction legacyBegin(IsolationLevel isolationLevel) {
        return metadataStore.withPublicationLock(() -> {
            ensureOpen();
            metadataStore.assertNoPendingPublications();
            long txId = nextTimestamp(TransactionStage.BEGIN_TSO);
            Transaction created = new Transaction(txId, txId, isolationLevel,
                    metadataStore.currentTimeMillis() + options.limits().timeoutMillis(), transactionOwner);
            // 独立快照登记保护 lazy 只读事务，不能依赖是否写入事务元数据。
            metadataStore.registerSnapshot(txId, txId, created.deadlineMillis());
            if (!options.lazyBeginMetadata()) {
                createBeginMetadata(txId, txId, isolationLevel);
                created.metadataPersisted(true);
            }
            return created;
        });
    }

    /**
     * 读取指定键或事务的当前可见数据。
     */
    public Optional<byte[]> get(Transaction tx, String shardId, String key) {
        return get(tx, shardId, key, beginReadStatement(tx));
    }

    /** 在同一条 SQL 的固定快照下执行点读。 */
    public Optional<byte[]> get(Transaction tx, String shardId, String key, StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
        StorageAdapter adapter = adapter(shardId);
        Optional<byte[]> bufferedValue = bufferedRead(tx, shardId, key);
        if (bufferedValue != null) {
            return bufferedValue;
        }
        Optional<VersionedValue> result = withShardPermit(shardId, () -> adapter.get(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                key
        ));
        ensureLeaseValid(tx);
        if (result.isEmpty() || result.get().deleted()) {
            return Optional.empty();
        }
        return Optional.of(result.get().value().clone());
    }

    /**
     * 按键范围扫描当前快照可见的数据。
     */
    public List<VersionedValue> scan(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive) {
        return scan(tx, shardId, startKeyInclusive, endKeyExclusive, beginReadStatement(tx));
    }

    /**
     * 创建绑定当前事务的语句级读取快照。
     */
    public StatementReadSnapshot beginReadStatement(Transaction tx) {
        ensureActive(tx);
        if (publicationLedger != null || segmentLedger != null) {
            ensureLeaseValid(tx);
            long readTs = readTs(tx);
            ensureSnapshotVisible(readTs, tx.txId());
            return new StatementReadSnapshot(tx.txId(), readTs);
        }
        return metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            metadataStore.assertNoPendingPublications();
            long readTs = readTs(tx);
            ensureSnapshotVisible(readTs, tx.txId());
            return new StatementReadSnapshot(tx.txId(), readTs);
        });
    }

    /** SQL 完成排序或聚合后验证原语句仍在租约内，不分配新的 RC 时间戳。 */
    public void validateReadStatement(Transaction tx, StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
    }

    /**
     * 使用指定语句级快照扫描键范围，供一次 SQL 内的多表或多分片扫描复用同一可见边界。
     */
    public List<VersionedValue> scan(Transaction tx,
                                     String shardId,
                                     String startKeyInclusive,
                                     String endKeyExclusive,
                                     StatementReadSnapshot snapshot) {
        List<VersionedValue> result = new ArrayList<>();
        String after = null;
        long bytes = 0;
        do {
            ScanPage page = scanPage(tx, shardId, startKeyInclusive, endKeyExclusive,
                    snapshot, after, options.limits().scanPageSize());
            for (VersionedValue value : page.values()) {
                bytes += value.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + (value.value() == null ? 0 : value.value().length);
                if (result.size() >= options.limits().maxMaterializedRows() || bytes > options.limits().maxMaterializedBytes()) {
                    throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                            "materialized scan budget exceeded; use scanPage", false, tx.txId());
                }
                result.add(value);
            }
            after = page.nextKey();
        } while (after != null);
        return List.copyOf(result);
    }

    /** 按二进制键序合并一页存储结果和自身缓冲变更，空页也可带继续位置。 */
    public ScanPage scanPage(Transaction tx, String shardId, String startKeyInclusive, String endKeyExclusive,
                             StatementReadSnapshot snapshot, String afterKeyExclusive, int limit) {
        validateStatement(tx, snapshot);
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("page limit must be between 1 and 4096");
        }
        if (startKeyInclusive != null && endKeyExclusive != null && compareKeys(startKeyInclusive, endKeyExclusive) > 0) {
            throw new IllegalArgumentException("scan start must not exceed end");
        }
        StorageAdapter adapter = adapter(shardId);
        ScanPage base = withShardPermit(shardId, () -> adapter.scanPage(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                new KeyRange(startKeyInclusive, endKeyExclusive), afterKeyExclusive, limit));
        if (base.nextKey() != null && afterKeyExclusive != null && compareKeys(base.nextKey(), afterKeyExclusive) <= 0) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "storage page cursor did not advance", false, tx.txId());
        }
        List<Mutation> buffered = options.bufferWrites()
                ? tx.bufferedMutationsPage(shardId, startKeyInclusive, endKeyExclusive, afterKeyExclusive, limit + 1)
                : List.of();
        String pageEnd = base.nextKey();
        if (buffered.size() > limit) {
            String bufferedEnd = buffered.get(limit - 1).key();
            if (pageEnd == null || compareKeys(bufferedEnd, pageEnd) < 0) {
                pageEnd = bufferedEnd;
            }
        }
        NavigableMap<String, VersionedValue> merged = new TreeMap<>(TransactionManager::compareKeys);
        for (VersionedValue value : base.values()) {
            if (pageEnd == null || compareKeys(value.key(), pageEnd) <= 0) {
                merged.put(value.key(), value);
            }
        }
        if (!buffered.isEmpty()) {
            for (Mutation mutation : buffered) {
                String key = mutation.key();
                if ((startKeyInclusive != null && compareKeys(key, startKeyInclusive) < 0)
                        || (endKeyExclusive != null && compareKeys(key, endKeyExclusive) >= 0)
                        || (afterKeyExclusive != null && compareKeys(key, afterKeyExclusive) <= 0)
                        || (pageEnd != null && compareKeys(key, pageEnd) > 0)) {
                    continue;
                }
                if (mutation.type() == mvcc.common.MutationType.DELETE) {
                    merged.remove(key);
                } else {
                    merged.put(key, new VersionedValue(key, mutation.value(), false, tx.startTs(), true));
                }
            }
        }
        List<VersionedValue> values = new ArrayList<>(Math.min(limit, merged.size()));
        for (VersionedValue value : merged.values()) {
            if (values.size() == limit) {
                ensureLeaseValid(tx);
                return new ScanPage(List.copyOf(values), values.getLast().key());
            }
            values.add(new VersionedValue(value.key(), value.value() == null ? null : value.value().clone(),
                    value.deleted(), value.versionTs(), value.ownIntent()));
        }
        ensureLeaseValid(tx);
        return new ScanPage(List.copyOf(values), pageEnd);
    }

    /**
     * 在事务中写入键值数据。
     */
    public void put(Transaction tx, String shardId, String key, byte[] value) {
        Objects.requireNonNull(value, "value");
        write(tx, shardId, Mutation.put(key, value));
    }

    /**
     * 在事务中删除键值数据。
     */
    public void delete(Transaction tx, String shardId, String key) {
        write(tx, shardId, Mutation.delete(key));
    }

    /**
     * 在事务模板中执行回调并按策略重试。
     */
    public <T> T execute(IsolationLevel isolationLevel, TransactionCallback<T> callback) {
        MvccException lastRetryable = null;
        for (int attempt = 1; attempt <= options.retryPolicy().maxAttempts(); attempt++) {
            Transaction tx = begin(isolationLevel);
            try {
                T result = callback.doInTransaction(tx);
                commit(tx);
                return result;
            } catch (MvccException ex) {
                safeRollback(tx);
                if (tx.decisionStarted() || tx.status() == TxStatus.COMMITTED) {
                    throw ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN ? ex : unknownCommit(tx, ex);
                }
                if (!options.retryPolicy().shouldRetry(ex, attempt)) {
                    throw ex;
                }
                lastRetryable = ex;
                sleepQuietly(options.retryPolicy().nextDelayMillis(attempt - 1));
            } catch (RuntimeException ex) {
                safeRollback(tx);
                throw ex;
            }
        }
        throw lastRetryable == null
                ? new MvccException(ErrorCode.ILLEGAL_STATE, "retry loop exhausted without terminal exception", false, null)
                : lastRetryable;
    }

    /**
     * 提交事务意向并生成已提交版本。
     */
    public TransactionCommitResult commit(Transaction tx) {
        ensureActive(tx);
        TransactionClassification classification = transactionClassifier.classifyForCommit(tx, options);
        try {
            if (segmentLedger != null) {
                return switch (classification.protocolType()) {
                    case READ_ONLY_SNAPSHOT -> commitReadOnly(tx);
                    case SINGLE_SHARD_1PC -> segmentDirectSingleShard
                            && adapter(classification.participants().iterator().next())
                            .supportsAtomicDirectCommit()
                            ? commitSegmentSingleShardDirect(tx, classification.participants().iterator().next())
                            : commitSegmentWrite(tx);
                    case CROSS_SHARD_ASYNC_COMMIT, CROSS_SHARD_SYNC_2PC -> commitSegmentWrite(tx);
                    default -> throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "unsupported segmented transaction protocol: " + classification.protocolType(),
                            false, tx.txId());
                };
            }
            if (publicationLedger != null) {
                return switch (classification.protocolType()) {
                    case READ_ONLY_SNAPSHOT -> commitReadOnly(tx);
                    case SINGLE_SHARD_1PC -> ledgerDirectSingleShard
                            && adapter(classification.participants().iterator().next())
                            .supportsAtomicDirectCommit()
                            ? commitLedgerSingleShardDirect(tx, classification.participants().iterator().next())
                            : commitLedgerWrite(tx);
                    case CROSS_SHARD_ASYNC_COMMIT, CROSS_SHARD_SYNC_2PC -> commitLedgerWrite(tx);
                    default -> throw new MvccException(ErrorCode.ILLEGAL_STATE,
                            "unsupported ledger transaction protocol: " + classification.protocolType(),
                            false, tx.txId());
                };
            }
            return switch (classification.protocolType()) {
                case READ_ONLY_SNAPSHOT -> commitReadOnly(tx);
                case SINGLE_SHARD_1PC -> commitSingleShard(tx, classification.participants().iterator().next());
                case CROSS_SHARD_ASYNC_COMMIT, CROSS_SHARD_SYNC_2PC -> commitCrossShard(tx);
                default -> throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "unsupported transaction protocol: " + classification.protocolType(), false, tx.txId());
            };
        } catch (RuntimeException ex) {
            if (tx.decisionStarted()) {
                if (tx.status() == TxStatus.COMMITTED) {
                    releaseTransaction(tx);
                }
                if (ex instanceof MvccException mvcc && mvcc.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                    throw mvcc;
                }
                throw unknownCommit(tx, ex);
            }
            throw ex;
        }
    }

    /**
     * 提交只读事务。
     */
    private TransactionCommitResult commitReadOnly(Transaction tx) {
        if (publicationLedger != null || segmentLedger != null) {
            ensureLeaseValid(tx);
            tx.status(TxStatus.COMMITTED);
            releaseTransaction(tx);
            return new TransactionCommitResult(tx.txId(), tx.startTs(), TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
        }
        long commitTs = metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            if (!tx.metadataPersisted()) {
                return tx.startTs();
            }
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                metadataStore.markCommitted(tx.txId(), timestamp);
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
            return timestamp;
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /** 全部写事务先落参与者意向，再用持久分段槽提交中心决议并等待发布。 */
    private TransactionCommitResult commitSegmentWrite(Transaction tx) {
        registerWriteParticipants(tx);
        prewriteBufferedMutationsStage(tx);
        boolean explicitPrepareRequired = requiresExplicitPreparePhase(tx.participants());
        if (explicitPrepareRequired) {
            persistPreparingMetadata(tx);
            prepareParticipantsStage(tx);
        }
        ensureLeaseValid(tx);
        PublicationSegmentOwnerRegistry.Assignment slot = segmentReserveStage(tx);
        tx.decisionStarted(true);
        CommittedProof proof = segmentFastMaterialization ? segmentDecisionProofStage(tx, slot) : null;
        long commitTs = proof == null ? segmentDecisionStage(tx, slot) : proof.commitTs();
        tx.status(TxStatus.COMMITTED);
        if (segmentOverlapMaterialization && !options.crossShardCommitPolicy().asyncCommit()) {
            return commitSegmentOverlapped(tx, commitTs);
        }
        segmentAwaitStage(tx, commitTs);
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                releaseLocalTransaction(tx);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            runAcrossParticipants(tx.participants(), participant -> {
                if (proof == null) {
                    commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT,
                            tx, participant, commitTs);
                } else {
                    commitDecidedParticipantStage(tx, participant, proof);
                }
            },
                    options.crossShardCommitPolicy().parallelCommit());
        } catch (RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /** 中心决议已持久化后并行物化分片与发布前缀，返回前仍等待两者成功。 */
    private TransactionCommitResult commitSegmentOverlapped(Transaction tx, long commitTs) {
        CompletableFuture<Void> participants;
        try {
            // 发布前版本仍受 commitTs 水位约束；发布后未物化分片由读路径解析已提交意向。
            participants = CompletableFuture.runAsync(() -> runAcrossParticipants(tx.participants(),
                    participant -> commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT,
                            tx, participant, commitTs), true), participantExecutor);
        } catch (RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        RuntimeException publicationFailure = null;
        try {
            segmentAwaitStage(tx, commitTs);
        } catch (RuntimeException ex) {
            publicationFailure = ex;
        }
        Throwable participantFailure = null;
        try {
            participants.join();
        } catch (CompletionException ex) {
            participantFailure = ex.getCause();
        }
        if (publicationFailure != null) {
            if (participantFailure != null) {
                publicationFailure.addSuppressed(participantFailure);
            }
            throw publicationFailure;
        }
        if (participantFailure != null) {
            completeTransaction(tx, false);
            if (participantFailure instanceof Error error) {
                throw error;
            }
            throw unknownCommit(tx, participantFailure instanceof RuntimeException runtime
                    ? runtime : new CompletionException(participantFailure));
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /** 单分片先持久化候选槽，再由本地一次事务直接落版本与参与者终态。 */
    private TransactionCommitResult commitSegmentSingleShardDirect(Transaction tx, String participant) {
        registerWriteParticipants(tx);
        ensureLeaseValid(tx);
        PublicationSegmentOwnerRegistry.Assignment slot = segmentReserveStage(tx);
        tx.decisionStarted(true);
        segmentClaimLocalStage(tx, slot, participant);
        try {
            directCommitSingleShardParticipantStage(tx, participant, slot.commitTs());
        } catch (RuntimeException ex) {
            return resolveSegmentDirectOutcome(tx, participant, slot, ex);
        }
        // directCommit 正常返回意味着本地事务已提交；仅响应未知时再读持久证明。
        return finishSegmentDirectCommit(tx, participant, slot,
                new ParticipantRecord(tx.txId(), tx.startTs(), TxStatus.COMMITTED, slot.commitTs()));
    }

    /** 本地响应未知时先 fencing 分片，再按持久终态补中心提交或中止。 */
    private TransactionCommitResult resolveSegmentDirectOutcome(Transaction tx, String participant,
                                                                PublicationSegmentOwnerRegistry.Assignment slot,
                                                                RuntimeException original) {
        ParticipantRecord local;
        try {
            withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            });
            local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() ->
                    new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "segmented direct participant lacks durable terminal proof", false, tx.txId()));
        } catch (RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        if (local.status() == TxStatus.COMMITTED) {
            return finishSegmentDirectCommit(tx, participant, slot, local);
        }
        if (local.status() != TxStatus.ABORTED || local.startTs() != tx.startTs()) {
            throw unknownCommit(tx, new IllegalStateException(
                    "segmented direct participant terminal proof differs from transaction"));
        }
        try {
            segmentCompleteLocalStage(slot, local, participant, false);
        } catch (RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        tx.status(TxStatus.ABORTED);
        tx.decisionStarted(false);
        throw original;
    }

    /** 真实本地 COMMITTED 证明补齐中心决议并等待全局已发布水位。 */
    private TransactionCommitResult finishSegmentDirectCommit(Transaction tx, String participant,
                                                              PublicationSegmentOwnerRegistry.Assignment slot,
                                                              ParticipantRecord local) {
        if (local.status() != TxStatus.COMMITTED || local.startTs() != tx.startTs()
                || local.commitTs() == null || local.commitTs() != slot.commitTs()) {
            throw unknownCommit(tx, new IllegalStateException(
                    "segmented direct proof differs from reserved time slot"));
        }
        segmentCompleteLocalStage(slot, local, participant, true);
        tx.status(TxStatus.COMMITTED);
        segmentAwaitStage(tx, slot.commitTs());
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), slot.commitTs(),
                TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /** 只把已由独立后台发布器持久推进的水位作为提交确认边界。 */
    private void awaitSegmentPublication(Transaction tx, long commitTs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.limits().timeoutMillis());
        if (segmentScheduler.publishedWatermark() < commitTs) {
            segmentScheduler.requestPublication();
        }
        while (segmentScheduler.publishedWatermark() < commitTs) {
            if (System.nanoTime() >= deadline) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "segmented decision persisted but publication confirmation timed out", false, tx.txId());
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "interrupted while awaiting segmented publication", false, tx.txId());
            }
            java.util.concurrent.locks.LockSupport.parkNanos(100_000);
        }
    }

    /** 新协议所有写事务先持久化参与者，再发布中心决议与连续水位。 */
    private TransactionCommitResult commitLedgerWrite(Transaction tx) {
        registerWriteParticipants(tx);
        prewriteBufferedMutationsStage(tx);
        boolean explicitPrepareRequired = requiresExplicitPreparePhase(tx.participants());
        if (explicitPrepareRequired) {
            persistPreparingMetadata(tx);
            prepareParticipantsStage(tx);
        }
        ensureLeaseValid(tx);
        tx.decisionStarted(true);
        PublicationCommitResult publication = ledgerAtomicCommitStage(tx);
        long commitTs = publication.commitTs();
        tx.status(TxStatus.COMMITTED);
        if (!publication.publishedImmediately()) {
            ledgerPublishStage();
        }
        ledgerAwaitStage(tx, commitTs);
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                releaseLocalTransaction(tx);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            runAcrossParticipants(tx.participants(), participant -> commitParticipantStage(
                    TransactionStage.PRIMARY_PARTICIPANT_COMMIT, tx, participant, commitTs),
                    options.crossShardCommitPolicy().parallelCommit());
        } catch (RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /** 单分片先留下可恢复的 PENDING 候选，再一次本地事务直接落版本与终态。 */
    private TransactionCommitResult commitLedgerSingleShardDirect(Transaction tx, String participant) {
        registerWriteParticipants(tx);
        ensureLeaseValid(tx);
        tx.decisionStarted(true);
        long commitTs = ledgerLocalReserveStage(tx, participant);
        try {
            directCommitSingleShardParticipantStage(tx, participant, commitTs);
        } catch (RuntimeException ex) {
            return resolveLedgerDirectOutcome(tx, participant, commitTs, ex);
        }
        return finishLedgerDirectCommit(tx, participant, commitTs);
    }

    /** 本地响应丢失时先 fencing，再按真实终态补中心决议或安全回滚。 */
    private TransactionCommitResult resolveLedgerDirectOutcome(Transaction tx, String participant,
                                                               long commitTs, RuntimeException original) {
        ParticipantRecord local;
        try {
            withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            });
            local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() ->
                    new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                            "direct participant has no durable terminal record", false, tx.txId()));
        } catch (RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        if (local.status() == TxStatus.COMMITTED) {
            return finishLedgerDirectCommit(tx, participant, commitTs);
        }
        if (local.status() != TxStatus.ABORTED || local.startTs() != tx.startTs()) {
            throw unknownCommit(tx, new IllegalStateException("direct participant terminal proof differs from transaction"));
        }
        try {
            if (metadataStore.markAborted(tx.txId()).status() != TxStatus.ABORTED) {
                throw new IllegalStateException("local abort raced a central commit decision");
            }
            publicationLedger.decideAbortedAfterFencing(tx.txId(), commitTs);
            try {
                ledgerPublishStage();
            } catch (RuntimeException publicationFailure) {
                backgroundFailures.increment();
            }
        } catch (RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        tx.status(TxStatus.ABORTED);
        tx.decisionStarted(false);
        throw original;
    }

    /** 核对本地提交证明后补齐中心和账本决议，再等待全局发布。 */
    private TransactionCommitResult finishLedgerDirectCommit(Transaction tx, String participant, long commitTs) {
        ParticipantRecord local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() ->
                new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "direct participant committed without durable local proof", false, tx.txId()));
        if (local.status() != TxStatus.COMMITTED || local.startTs() != tx.startTs()
                || local.commitTs() == null || local.commitTs() != commitTs) {
            throw unknownCommit(tx, new IllegalStateException("direct participant proof differs from reserved commit"));
        }
        boolean publishedImmediately = ledgerLocalDecisionStage(local, commitTs, participant);
        tx.status(TxStatus.COMMITTED);
        if (!publishedImmediately && localCompletionBatcher == null) {
            ledgerPublishStage();
        }
        ledgerAwaitStage(tx, commitTs);
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交单分片事务。
     */
    private TransactionCommitResult commitSingleShard(Transaction tx, String participant) {
        registerWriteParticipants(tx);
        boolean atomicDirect = options.bufferWrites() && adapter(participant).supportsAtomicDirectCommit();
        if (!atomicDirect && !adapter(participant).supportsCommittedIntentResolution()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "intent-based single-shard commit requires committed intent resolution", false, tx.txId());
        }
        if (!atomicDirect && options.bufferWrites()) {
            prewriteBufferedMutationsStage(tx);
        }
        long commitTs = metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                if (atomicDirect) {
                    // 本地版本和事务记录先原子提交，中心决议丢失时由参与者记录恢复。
                    metadataStore.markLocalCommitPending(tx.txId(), timestamp);
                    directCommitSingleShardParticipantStage(tx, participant, timestamp);
                }
                markSingleShardCommittedStage(tx, participant, timestamp);
                tx.status(TxStatus.COMMITTED);
                if (!atomicDirect) {
                    commitParticipantStage(TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT,
                            tx, participant, timestamp);
                }
                return timestamp;
            } catch (MvccException ex) {
                if (atomicDirect && !tx.status().equals(TxStatus.COMMITTED)
                        && (ex.errorCode() == ErrorCode.WRITE_CONFLICT || ex.errorCode() == ErrorCode.LOCK_CONFLICT)) {
                    tx.decisionStarted(false);
                    metadataStore.markAborted(tx.txId());
                    tx.status(TxStatus.ABORTED);
                    throw ex;
                }
                throw unknownCommit(tx, ex);
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * 提交跨分片事务。
     */
    private TransactionCommitResult commitCrossShard(Transaction tx) {
        for (String participant : tx.participants()) {
            if (!adapter(participant).supportsCommittedIntentResolution()) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "cross-shard snapshot visibility requires committed intent resolution: " + participant,
                        false, tx.txId());
            }
        }
        registerWriteParticipants(tx);
        if (options.bufferWrites()) {
            prewriteBufferedMutationsStage(tx);
        } else {
            prewriteBufferedMutations(tx);
        }

        // 多分片事务先持久化可恢复状态，再提交各参与分片，保证异常后可以恢复。
        boolean explicitPrepareRequired = requiresExplicitPreparePhase(tx.participants());
        if (explicitPrepareRequired) {
            persistPreparingMetadata(tx);
            prepareParticipantsStage(tx);
        }

        long commitTs = publishCrossShardDecision(tx, explicitPrepareRequired);
        String primaryParticipant = primaryParticipant(tx.participants());
        try {
            if (options.crossShardCommitPolicy().asyncCommit()) {
                scheduleAsyncParticipantCommit(tx, commitTs, tx.participants());
                // 元数据决议已持久化；租约由异步物化线程在完成标记之后释放。
                releaseLocalTransaction(tx);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            // 同步提交模式先提交主参与者，再提交其余参与者，保留状态未知异常边界。
            commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT, tx, primaryParticipant, commitTs);
            Set<String> secondaryParticipants = secondaryParticipants(tx.participants(), primaryParticipant);
            runAcrossParticipants(
                    secondaryParticipants,
                    participant -> commitParticipantStage(
                            TransactionStage.SECONDARY_PARTICIPANT_COMMIT,
                            tx,
                            participant,
                            commitTs
                    ),
                    options.crossShardCommitPolicy().parallelCommit()
            );
        } catch (RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }

        completeTransaction(tx, true);
        return new TransactionCommitResult(
                tx.txId(),
                commitTs,
                options.crossShardCommitPolicy().asyncCommit()
                        ? TransactionCommitMode.ASYNC_PRIMARY_COMMIT
                        : TransactionCommitMode.TWO_PHASE_COMMIT
        );
    }

    /** 优先在同一 MySQL 权威内合并发号和决议；其余实现保留原协议。 */
    private long publishCrossShardDecision(Transaction tx, boolean explicitPrepareRequired) {
        if (metadataStore instanceof FastTransactionCommit fastCommit) {
            Supplier<OptionalLong> fastAction = () -> fastCommit.tryCommit(timestampOracle, tx.txId(),
                    tx.participants(), () -> {
                        ensureLeaseValid(tx);
                        tx.decisionStarted(true);
                    }, timestamp -> tx.status(TxStatus.COMMITTED));
            OptionalLong accelerated = acceleratedCommitStage(fastAction);
            if (accelerated.isPresent()) {
                return accelerated.getAsLong();
            }
        }
        return metadataStore.withPublicationLock(() -> {
            ensureLeaseValid(tx);
            long timestamp = nextTimestamp(TransactionStage.COMMIT_TSO);
            tx.decisionStarted(true);
            try {
                persistCommittedMetadataStage(tx, timestamp, explicitPrepareRequired);
                tx.status(TxStatus.COMMITTED);
                return timestamp;
            } catch (RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
    }

    /**
     * 关闭资源或后台执行器。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.limits().shutdownTimeoutMillis());
        ExecutorService rollbackExecutor = Executors.newVirtualThreadPerTaskExecutor();
        for (Transaction tx : List.copyOf(activeTransactions.values())) {
            rollbackExecutor.submit(() -> safeRollback(tx));
        }
        shutdownExecutor(rollbackExecutor, deadline);
        if (publicationCommitBatcher != null) {
            publicationCommitBatcher.close(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
        }
        if (localReserveBatcher != null) {
            localReserveBatcher.close(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
        }
        if (localCompletionBatcher != null) {
            localCompletionBatcher.close(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
        }
        if (beginBatcher != null) {
            beginBatcher.close(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
        }
        if (segmentCommitBatcher != null) {
            segmentCommitBatcher.close(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
        }
        shutdownExecutor(asyncCommitExecutor, deadline);
        // async commit 可能继续提交 intent cleanup 任务，必须先 drain async 再关闭 cleanup。
        shutdownExecutor(intentCleanupExecutor, deadline);
        shutdownExecutor(participantExecutor, deadline);
        // 未完成的回滚保留持久化租约及恢复记录，仅释放关闭管理器的本地写缓冲。
        activeTransactions.forEach((txId, tx) -> {
            if (activeTransactions.remove(txId, tx)) {
                transactionPermits.release();
            }
        });
        if (coordinatorRegistry != null) {
            coordinatorRegistry.close();
        }
        if (segmentOwner != null) {
            segmentOwner.close();
        }
    }

    /**
     * 调度异步参与者提交任务。
     */
    private void scheduleAsyncParticipantCommit(Transaction tx, long commitTs, Set<String> participants) {
        if (participants.isEmpty()) {
            return;
        }
        Set<String> stableParticipants = Set.copyOf(participants);
        submitOrRun(asyncCommitExecutor, () -> {
            boolean succeeded = true;
            for (String participant : stableParticipants) {
                try {
                    commitParticipantStage(TransactionStage.ASYNC_PARTICIPANT_COMMIT, tx, participant, commitTs);
                } catch (RuntimeException ex) {
                    backgroundFailures.increment();
                    succeeded = false;
                }
            }
            finishAsyncCommit(tx, succeeded);
        });
    }

    /** 异步物化结束后记录完成结果并释放快照；失败决议仍由恢复任务补齐。 */
    private void finishAsyncCommit(Transaction tx, boolean finalized) {
        try {
            if (finalized && tx.metadataPersisted() && !options.deferIntentCleanup()) {
                metadataStore.markFinalized(tx.txId());
            }
            if (segmentLedger != null) {
                segmentLedger.releaseSnapshot(tx.txId());
            } else if (publicationLedger == null) {
                metadataStore.releaseSnapshot(tx.txId());
            } else {
                publicationLedger.releaseSnapshot(tx.txId());
            }
        } catch (RuntimeException ex) {
            backgroundFailures.increment();
        }
    }

    /**
     * 持久化跨分片事务准备中元数据。
     */
    private void persistPreparingMetadata(Transaction tx) {
        if (tx.metadataPersisted()) {
            metadataStore.persistPreparing(tx.txId(), tx.participants());
        } else if (options.lazyBeginMetadata()) {
            metadataStore.createPreparing(tx.txId(), tx.startTs(), tx.isolationLevel(), tx.participants());
            tx.metadataPersisted(true);
        } else {
            metadataStore.persistPreparing(tx.txId(), tx.participants());
            tx.metadataPersisted(true);
        }
    }

    /**
     * 持久化跨分片事务已提交元数据。
     */
    private void persistCommittedMetadata(Transaction tx, long commitTs, boolean explicitPrepareRequired) {
        if (explicitPrepareRequired) {
            metadataStore.persistCommitted(tx.txId(), commitTs);
        } else if (tx.metadataPersisted()) {
            metadataStore.persistCommitted(tx.txId(), tx.participants(), commitTs);
        } else if (options.lazyBeginMetadata()) {
            metadataStore.createCommitted(tx.txId(), tx.startTs(), tx.isolationLevel(), tx.participants(), commitTs);
            tx.metadataPersisted(true);
        } else {
            metadataStore.persistCommitted(tx.txId(), tx.participants(), commitTs);
            tx.metadataPersisted(true);
        }
    }

    /**
     * 选择跨分片事务的主参与者。
     */
    private String primaryParticipant(Set<String> participants) {
        return participants.stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "cross-shard tx has no participants", false, null));
    }

    /**
     * 计算除主参与者之外的从参与者集合。
     */
    private Set<String> secondaryParticipants(Set<String> participants, String primaryParticipant) {
        return participants.stream()
                .filter(participant -> !participant.equals(primaryParticipant))
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    /**
     * 回滚事务。
     */
    public void rollback(Transaction tx) {
        ensureOwned(tx);
        if (segmentLedger != null) {
            rollbackSegment(tx);
            return;
        }
        if (publicationLedger != null) {
            rollbackLedger(tx);
            return;
        }
        if (tx.status() == TxStatus.COMMITTED) {
            releaseTransaction(tx);
            return;
        }
        rollbackStage(tx);
        if (tx.status() != TxStatus.COMMITTED) {
            tx.status(TxStatus.ABORTED);
        }
        releaseTransaction(tx);
    }

    /** 分段决议已开始但缺少终态槽时保持结果未知，避免盲目覆盖在途提交。 */
    private void rollbackSegment(Transaction tx) {
        PublicationEntry entry = segmentLedger.findByTxId(tx.txId()).orElse(null);
        if (tx.status() == TxStatus.COMMITTED
                || entry != null && entry.status() == PublicationEntryStatus.COMMITTED) {
            tx.status(TxStatus.COMMITTED);
            releaseTransaction(tx);
            return;
        }
        if (tx.decisionStarted() && entry == null) {
            throw unknownCommit(tx, new IllegalStateException("segmented decision has no durable terminal identity"));
        }
        if (entry != null && entry.status() == PublicationEntryStatus.PENDING) {
            TxRecord current = metadataStore.get(tx.txId()).orElseThrow(() ->
                    unknownCommit(tx, new IllegalStateException("claimed segment slot has no metadata")));
            if (current.commitTs() != null) {
                throw unknownCommit(tx, new IllegalStateException(
                        "segmented local candidate requires durable participant proof"));
            }
        }
        if (tx.metadataPersisted()) {
            TxRecord decided = metadataStore.markAborted(tx.txId());
            if (decided.status() == TxStatus.COMMITTED) {
                PublicationEntry committed = segmentLedger.findByTxId(tx.txId()).orElseThrow(() ->
                        unknownCommit(tx, new IllegalStateException("committed metadata lost segmented slot")));
                if (committed.status() != PublicationEntryStatus.COMMITTED) {
                    throw unknownCommit(tx, new IllegalStateException("segmented rollback raced different terminal slot"));
                }
                tx.status(TxStatus.COMMITTED);
                releaseTransaction(tx);
                return;
            }
            runAcrossParticipants(tx.participants(), participant -> withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            }), options.crossShardCommitPolicy().parallelRollback());
        }
        tx.status(TxStatus.ABORTED);
        markFinalizedQuietly(tx);
        releaseTransaction(tx);
    }

    /** 新协议先确认中心终态，再幂等 fencing 参与者并解除账本水位阻塞。 */
    private void rollbackLedger(Transaction tx) {
        PublicationEntry entry = publicationLedger.findByTxId(tx.txId()).orElse(null);
        if (tx.status() == TxStatus.COMMITTED || entry != null && entry.status() == PublicationEntryStatus.COMMITTED) {
            tx.status(TxStatus.COMMITTED);
            releaseTransaction(tx);
            return;
        }
        if (entry != null && entry.status() == PublicationEntryStatus.PENDING) {
            TxRecord current = metadataStore.get(tx.txId()).orElseThrow(() ->
                    unknownCommit(tx, new IllegalStateException("pending publication has no metadata")));
            if (current.commitTs() != null) {
                throw unknownCommit(tx, new IllegalStateException("local pending publication needs participant proof"));
            }
        }
        if (tx.metadataPersisted()) {
            TxRecord decided = metadataStore.markAborted(tx.txId());
            if (decided.status() == TxStatus.COMMITTED) {
                tx.status(TxStatus.COMMITTED);
                releaseTransaction(tx);
                return;
            }
        }
        if (tx.metadataPersisted()) {
            runAcrossParticipants(tx.participants(), participant -> withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            }), options.crossShardCommitPolicy().parallelRollback());
        }
        // 预留提交的响应可能丢失；中心 ABORTED 行锁会等待在途预留后，再读取真实账本。
        PublicationEntry afterFence = publicationLedger.findByTxId(tx.txId()).orElse(null);
        if (afterFence != null && afterFence.status() == PublicationEntryStatus.PENDING) {
            publicationLedger.decideAbortedAfterFencing(tx.txId(), afterFence.commitTs());
            publicationLedger.advancePublished(256);
        } else if (afterFence != null && afterFence.status() == PublicationEntryStatus.COMMITTED) {
            throw unknownCommit(tx, new IllegalStateException("commit decision appeared after rollback fencing"));
        }
        tx.status(TxStatus.ABORTED);
        markFinalizedQuietly(tx);
        releaseTransaction(tx);
    }

    /**
     * 读取事务元数据记录。
     */
    public Optional<TxRecord> getRecord(long txId) {
        return metadataStore.get(txId);
    }

    /**
     * 向事务写集合追加一次变更。
     */
    private void write(Transaction tx, String shardId, Mutation mutation) {
        ensureActive(tx);
        ensureLeaseValid(tx);
        adapter(shardId);
        mvcc.storage.Utf8KeyOrder.encode(mutation.key());
        tx.reserveWrite(shardId, mutation, options.limits().maxWriteKeys(), options.limits().maxWriteBytes());
        // 复制值避免调用方修改数组影响已计费、已预写或后台提交的写集合。
        mutation = new Mutation(mutation.key(), mutation.type(), mutation.value() == null ? null : mutation.value().clone());
        if (options.bufferWrites()) {
            tx.bufferMutation(shardId, mutation);
            return;
        }
        StorageAdapter adapter = adapter(shardId);
        tx.addParticipant(shardId);
        try {
            registerWriteParticipants(tx);
            prewriteEagerMutationStage(tx, shardId, mutation, adapter);
        } catch (RuntimeException | Error ex) {
            tx.markRollbackOnly();
            throw ex;
        }
    }

    /**
     * 带冲突等待策略地预写入非缓冲单条变更。
     */
    private void prewriteEagerMutation(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter) {
        LockWaitPolicy lockWaitPolicy = options.lockWaitPolicy();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitPolicy.lockWaitTimeoutMs());
        int attempt = 0;
        while (true) {
            try {
                withShardPermit(shardId, () -> {
                    ensureLeaseValid(tx);
                    adapter.prewrite(tx.txId(), tx.startTs(), java.util.List.of(mutation));
                    return null;
                });
                return;
            } catch (MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on key " + mutation.key() + ", tx=" + tx.txId() + ", shard=" + shardId,
                            true,
                            tx.txId(),
                            ex
                    );
                }
                long delayMillis = Math.min(
                        lockWaitPolicy.nextDelayMillis(attempt++),
                        remainingMillis(deadlineNanos)
                );
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * 确认事务仍处于可操作状态。
     */
    private void ensureActive(Transaction tx) {
        ensureOpen();
        ensureOwned(tx);
        if (activeTransactions.get(tx.txId()) != tx) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction belongs to another or closed manager", false, tx.txId());
        }
        if (tx.status() != TxStatus.ACTIVE || tx.rollbackOnly()) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "transaction not active: " + tx.txId(), false, tx.txId());
        }
    }

    /**
     * 计算事务读取使用的快照时间戳。
     */
    private long readTs(Transaction tx) {
        return tx.isolationLevel() == IsolationLevel.REPEATABLE_READ
                ? tx.startTs()
                : segmentLedger != null ? segmentLedger.currentPublished()
                : publicationLedger == null ? nextTimestamp(TransactionStage.READ_TSO)
                        : publicationLedger.currentPublished();
    }

    /**
     * 校验读取快照尚未被安全点回收。
     */
    private void ensureSnapshotVisible(long readTs, long txId) {
        long safePoint = segmentLedger != null ? segmentLedger.currentSafePoint()
                : publicationLedger == null
                ? safePointStore.currentSafePoint() : publicationLedger.currentSafePoint();
        if (readTs < safePoint) {
            throw new MvccException(
                    ErrorCode.SNAPSHOT_TOO_OLD,
                    "snapshot too old, readTs=" + readTs + ", safePoint=" + safePoint,
                    false,
                    txId
            );
        }
    }

    /**
     * 获取指定分片的存储适配器。
     */
    private StorageAdapter adapter(String shardId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "unknown shard: " + shardId, false, null);
        }
        return adapter;
    }

    /**
     * 判断参与分片是否需要显式 prepare 阶段。
     */
    private boolean requiresExplicitPreparePhase(Set<String> participants) {
        for (String participant : participants) {
            if (adapter(participant).requiresExplicitPreparePhase()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断遇到冲突后是否需要等待重试。
     */
    private boolean shouldWaitOnConflict(MvccException ex, LockWaitPolicy lockWaitPolicy) {
        return lockWaitPolicy.conflictMode() == ConflictMode.WAIT
                && TransientStorageConflictDetector.isTransientConflict(ex);
    }

    /**
     * 计算距离截止时间的剩余毫秒数。
     */
    private long remainingMillis(long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    /**
     * 安全回滚事务并吞掉回滚异常。
     */
    private void safeRollback(Transaction tx) {
        try {
            rollback(tx);
        } catch (RuntimeException ignored) {
        }
    }

    /**
     * 在多个参与分片上执行操作。
     */
    private void runAcrossParticipants(Set<String> participants, Consumer<String> operation, boolean parallel) {
        if (!parallel || participants.size() <= 1) {
            for (String participant : participants) {
                operation.accept(participant);
            }
            return;
        }

        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CompletableFuture<?>[] futures = participants.stream()
                .map(participant -> CompletableFuture.runAsync(() -> {
                    try {
                        operation.accept(participant);
                    } catch (Throwable ex) {
                        // 并行参与者抛出的 Error 也必须回传给调用方，避免提交路径误判成功。
                        failures.add(ex);
                        throw ex;
                    }
                }, participantExecutor))
                .toArray(CompletableFuture[]::new);

        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException ignored) {
        }

        Throwable failure = failures.peek();
        if (failure != null) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new CompletionException(failure);
        }
    }

    /**
     * 安静等待指定毫秒数。
     */
    private void sleepQuietly(long delayMillis) {
        if (delayMillis <= 0L) {
            return;
        }
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for retry", true, null, ex);
        }
    }

    /**
     * 将事务缓冲写集合预写入各分片。
     */
    private void prewriteBufferedMutations(Transaction tx) {
        if (!options.bufferWrites()) {
            return;
        }
        runAcrossParticipants(
                tx.participants(),
                shardId -> {
                    List<Mutation> mutations = tx.bufferedMutations(shardId);
                    if (!mutations.isEmpty()) {
                        prewriteMutations(tx, shardId, mutations);
                    }
                },
                options.crossShardCommitPolicy().parallelPrewrite()
        );
    }

    /**
     * 带冲突等待策略地预写入指定分片变更。
     */
    private void prewriteMutations(Transaction tx, String shardId, List<Mutation> mutations) {
        LockWaitPolicy lockWaitPolicy = options.lockWaitPolicy();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(lockWaitPolicy.lockWaitTimeoutMs());
        int attempt = 0;
        while (true) {
            try {
                withShardPermit(shardId, () -> {
                    ensureLeaseValid(tx);
                    adapter(shardId).prewrite(tx.txId(), tx.startTs(), mutations);
                    return null;
                });
                return;
            } catch (MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on shard " + shardId + ", tx=" + tx.txId(),
                            true,
                            tx.txId(),
                            ex
                    );
                }
                // 等待型锁策略只对瞬时冲突退避重试，避免对永久错误做无意义重试。
                long delayMillis = Math.min(lockWaitPolicy.nextDelayMillis(attempt++), remainingMillis(deadlineNanos));
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * 从事务本地写缓冲读取最新变更。
     */
    private Optional<byte[]> bufferedRead(Transaction tx, String shardId, String key) {
        if (!options.bufferWrites()) {
            return null;
        }
        Optional<Mutation> mutation = tx.bufferedMutation(shardId, key);
        if (mutation.isEmpty()) {
            return null;
        }
        return mutation.get().type() == mvcc.common.MutationType.DELETE
                ? Optional.empty()
                : Optional.of(mutation.get().value().clone());
    }

    /**
     * 判断提交策略是否需要并行执行器。
     */
    private boolean requiresParallelExecutor(CrossShardCommitPolicy policy) {
        return policy.parallelPrewrite()
                || policy.parallelPrepare()
                || policy.parallelCommit()
                || policy.parallelRollback()
                || policy.asyncCommit();
    }

    /**
     * 关闭并行执行器。
     */
    private void shutdownExecutor(ExecutorService executor, long deadlineNanos) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                // 未完成物化由持久化决议恢复，不能阻止关闭无限等待。
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    /**
     * 创建带名称前缀的后台线程工厂。
     */
    private ThreadFactory namedFactory(String prefix) {
        return Thread.ofPlatform()
                .daemon(true)
                .name(prefix, 0)
                .factory();
    }

    /**
     * 提交指定参与分片。
     */
    private void commitParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            boolean cleanupIntents = !options.deferIntentCleanup() || asyncCommitExecutor == null;
            withShardPermit(shardId, () -> {
                adapter(shardId).commit(tx.txId(), commitTs, bufferedMutations, cleanupIntents);
                return null;
            });
            if (!cleanupIntents) {
                submitOrRun(intentCleanupExecutor, () -> {
                    try {
                        cleanupIntentsStage(tx, shardId);
                    } catch (RuntimeException ex) {
                        backgroundFailures.increment();
                    }
                });
            }
            return;
        }
        withShardPermit(shardId, () -> {
            adapter(shardId).commit(tx.txId(), commitTs);
            return null;
        });
    }

    /**
     * 直接提交单分片参与者。
     */
    private void directCommitSingleShardParticipant(Transaction tx, String shardId, long commitTs) {
        List<Mutation> bufferedMutations = tx.bufferedMutations(shardId);
        if (options.bufferWrites() && !bufferedMutations.isEmpty()) {
            withShardPermit(shardId, () -> {
                adapter(shardId).directCommit(tx.txId(), tx.startTs(), commitTs, bufferedMutations);
                return null;
            });
            return;
        }
        commitParticipant(tx, shardId, commitTs);
    }

    /** 返回调用方可导出到监控系统的有界执行统计。 */
    public TransactionRuntimeStats runtimeStats() {
        return new TransactionRuntimeStats(activeTransactions.size(), queuedTasks(asyncCommitExecutor),
                queuedTasks(intentCleanupExecutor), synchronousFallbacks.sum(), backgroundFailures.sum());
    }

    /** 创建具备固定线程数和有界等待队列的后台执行器。 */
    private ThreadPoolExecutor boundedExecutor(String prefix) {
        int workers = options.crossShardCommitPolicy().asyncCommitWorkers();
        return new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(options.limits().asyncQueueCapacity()), namedFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** 队列满或正在关闭时由当前线程完成已决议工作，不丢弃补提交。 */
    private void submitOrRun(ExecutorService executor, Runnable action) {
        try {
            executor.execute(action);
        } catch (RejectedExecutionException ex) {
            synchronousFallbacks.increment();
            action.run();
        }
    }

    /** 返回后台执行器等待队列长度。 */
    private int queuedTasks(ExecutorService executor) {
        return executor instanceof ThreadPoolExecutor pool ? pool.getQueue().size() : 0;
    }

    /** 限制每个分片的在途操作，等待预算耗尽时明确失败。 */
    private <T> T withShardPermit(String shardId, Supplier<T> action) {
        Semaphore permits = shardPermits.get(shardId);
        if (permits == null) {
            adapter(shardId);
            throw new IllegalStateException("missing shard budget");
        }
        boolean acquired = false;
        try {
            acquired = permits.tryAcquire(options.limits().timeoutMillis(), TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "shard concurrency budget exhausted: " + shardId,
                        true, null);
            }
            return action.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for shard budget",
                    false, null, ex);
        } finally {
            if (acquired) {
                permits.release();
            }
        }
    }

    /** 在首个持久化写入之前登记完整参与者，保证进程崩溃后可以定位所有 intent。 */
    private void registerWriteParticipants(Transaction tx) {
        ensureLeaseValid(tx);
        TxRecord record;
        if (!tx.metadataPersisted()) {
            // 租约先于此记录持久化；预写仍在登记完成后执行。
            if (coordinatorRegistry != null && tx.txId() < 0) {
                record = coordinatorRegistry.promoteAndRegister(tx, tx.participants());
            } else {
                record = metadataStore.createWithParticipants(tx.txId(), tx.startTs(),
                        tx.isolationLevel(), tx.participants());
            }
            tx.metadataPersisted(true);
            if (coordinatorRegistry != null && tx.txId() < 0) {
                coordinatorRegistry.promotionPersisted(tx.txId());
            }
        } else {
            record = metadataStore.registerParticipants(tx.txId(), tx.participants());
        }
        if (record.status() != TxStatus.ACTIVE && record.status() != TxStatus.PREPARING) {
            throw new MvccException(ErrorCode.TRANSACTION_ABORTED, "write registration rejected for terminal transaction",
                    false, tx.txId());
        }
    }

    /** 校验事务与语句归属，保持 RR 和跨分片 SQL 的固定快照。 */
    private void validateStatement(Transaction tx, StatementReadSnapshot snapshot) {
        ensureActive(tx);
        ensureLeaseValid(tx);
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.txId() != tx.txId()
                || (tx.isolationLevel() == IsolationLevel.REPEATABLE_READ && snapshot.readTs() != tx.startTs())) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "statement snapshot does not belong to transaction",
                    false, tx.txId());
        }
        ensureSnapshotVisible(snapshot.readTs(), tx.txId());
    }

    /** 拒绝租约过期的事务；查询完成后也必须检查，避免返回过期 GC 期间读取的结果。 */
    private void ensureLeaseValid(Transaction tx) {
        if (segmentLedger != null) {
            OptionalLong expiry = segmentLedger.snapshotExpiry(tx.txId());
            if (expiry.isEmpty() || expiry.getAsLong() != tx.deadlineMillis()
                    || metadataStore.currentTimeMillis() >= tx.deadlineMillis()) {
                throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT,
                        "segmented transaction lease expired", false, tx.txId());
            }
            return;
        }
        if (coordinatorRegistry != null && tx.txId() < 0 && coordinatorRegistry.protects(tx.txId())) {
            coordinatorRegistry.assertProtected(tx);
            return;
        }
        if (publicationLedger == null) {
            if (!metadataStore.isSnapshotLeaseValid(tx.txId(), tx.deadlineMillis())) {
                throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT, "transaction lease expired", false, tx.txId());
            }
            return;
        }
        OptionalLong ledgerExpiry = publicationLedger.snapshotExpiry(tx.txId());
        if (ledgerExpiry.isEmpty() || ledgerExpiry.getAsLong() != tx.deadlineMillis()
                || metadataStore.currentTimeMillis() >= tx.deadlineMillis()) {
            throw new MvccException(ErrorCode.TRANSACTION_TIMEOUT, "transaction lease expired", false, tx.txId());
        }
    }

    /** 管理器关闭后拒绝接收新事务及新业务操作。 */
    private void ensureOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction manager closed", false, null);
        }
    }

    /** 包括回滚在内的事务操作都必须来自创建该事务的管理器。 */
    private void ensureOwned(Transaction tx) {
        Objects.requireNonNull(tx, "tx");
        if (!tx.ownedBy(transactionOwner)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction belongs to another manager", false, tx.txId());
        }
    }

    /** 释放到期事务的本地名额，持久化租约及事务由恢复任务处理。 */
    private void expireLocalTransactions() {
        long now = metadataStore.currentTimeMillis();
        activeTransactions.forEach((txId, tx) -> {
            if (tx.deadlineMillis() <= now && activeTransactions.remove(txId, tx)) {
                if (coordinatorRegistry != null && txId < 0) {
                    coordinatorRegistry.release(txId);
                }
                transactionPermits.release();
            }
        });
    }

    /** 将已决议事务标记为完成，并按物化结果记录恢复进度。 */
    private void completeTransaction(Transaction tx, boolean finalized) {
        tx.status(TxStatus.COMMITTED);
        if (finalized) {
            markFinalizedQuietly(tx);
        }
        releaseTransaction(tx);
    }

    /** 释放快照和本地资源；释放失败时租约到期仍可回收。 */
    private void releaseTransaction(Transaction tx) {
        try {
            if (segmentLedger != null) {
                segmentLedger.releaseSnapshot(tx.txId());
            } else if (publicationLedger == null) {
                metadataStore.releaseSnapshot(tx.txId());
            } else if (coordinatorRegistry == null || tx.txId() >= 0
                    || !coordinatorRegistry.release(tx.txId())) {
                publicationLedger.releaseSnapshot(tx.txId());
            }
        } catch (RuntimeException ex) {
            backgroundFailures.increment();
        } finally {
            releaseLocalTransaction(tx);
        }
    }

    /** 只释放本实例的事务名额，不修改持久化租约或决议。 */
    private void releaseLocalTransaction(Transaction tx) {
        if (activeTransactions.remove(tx.txId(), tx)) {
            transactionPermits.release();
        }
    }

    /** 记录已完成物化；失败时留给恢复重放，不能将成功提交改判为业务失败。 */
    private void markFinalizedQuietly(Transaction tx) {
        if (!tx.metadataPersisted() || (options.deferIntentCleanup() && tx.status() == TxStatus.COMMITTED)) {
            return;
        }
        try {
            metadataStore.markFinalized(tx.txId());
        } catch (RuntimeException ex) {
            backgroundFailures.increment();
        }
    }

    /** 构造不可直接重跑业务的提交结果未知异常。 */
    private MvccException unknownCommit(Transaction tx, RuntimeException cause) {
        if (tx.status() != TxStatus.COMMITTED) {
            tx.status(TxStatus.PREPARING);
        }
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "resolve transaction decision before retrying business: " + tx.txId(), false, tx.txId(), cause);
    }

    /** 按 MySQL VARBINARY 使用的 UTF-8 无符号字节顺序比较键。 */
    private static int compareKeys(String left, String right) {
        return mvcc.storage.Utf8KeyOrder.compare(left, right);
    }

    /** 在分片预算内执行已提交 intent 清理。 */
    private void cleanupParticipantIntents(Transaction tx, String shardId) {
        withShardPermit(shardId, () -> {
            adapter(shardId).cleanupIntents(tx.txId());
            return null;
        });
    }

    /** 显式准备也使用同一分片并发预算，不能绕过前台流量限制。 */
    private void prepareParticipant(Transaction tx, String shardId) {
        withShardPermit(shardId, () -> {
            ensureLeaseValid(tx);
            adapter(shardId).prepare(tx.txId());
            return null;
        });
    }

    /** 判断当前是否启用事务阶段指标。 */
    private boolean metricsEnabled() {
        return metrics != NoOpTransactionMetrics.INSTANCE;
    }

    /** 默认指标关闭时直接取得分段快照，避免业务入口分配计量包装。 */
    private BeginReservation segmentBeginStage(IsolationLevel isolationLevel) {
        if (!metricsEnabled()) {
            return segmentLedger.begin(isolationLevel, options.limits().timeoutMillis(), false);
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_BEGIN,
                () -> segmentLedger.begin(isolationLevel, options.limits().timeoutMillis(), false));
    }

    /** 默认指标关闭时直接从本地持久分段取得提交槽。 */
    private PublicationSegmentOwnerRegistry.Assignment segmentReserveStage(Transaction tx) {
        if (!metricsEnabled()) {
            return segmentOwner.allocate(tx.startTs());
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_RESERVE,
                () -> segmentOwner.allocate(tx.startTs()));
    }

    /** 分别计量单分片物理提交的中心候选认领与终态补齐。 */
    private void segmentClaimLocalStage(Transaction tx, PublicationSegmentOwnerRegistry.Assignment slot,
                                        String participant) {
        if (!metricsEnabled()) {
            segmentOwner.claimLocal(slot, tx.txId(), tx.deadlineMillis(), participant);
            return;
        }
        recordMeasuredStage(TransactionStage.SEGMENT_LOCAL_CLAIM, () -> {
            segmentOwner.claimLocal(slot, tx.txId(), tx.deadlineMillis(), participant);
            return null;
        });
    }

    /** 成功及异常恢复都记录本地终态证明对应的中心事务耗时。 */
    private void segmentCompleteLocalStage(PublicationSegmentOwnerRegistry.Assignment slot,
                                           ParticipantRecord local, String participant, boolean committed) {
        if (!metricsEnabled()) {
            if (committed) {
                segmentOwner.completeLocalCommit(slot, local, participant);
            } else {
                segmentOwner.completeLocalAbort(slot, local, participant);
            }
            return;
        }
        recordMeasuredStage(TransactionStage.SEGMENT_LOCAL_COMPLETE, () -> {
            if (committed) {
                segmentOwner.completeLocalCommit(slot, local, participant);
            } else {
                segmentOwner.completeLocalAbort(slot, local, participant);
            }
            return null;
        });
    }

    /** 默认指标关闭时直接把中心与分段槽决议原子落盘。 */
    private long segmentDecisionStage(Transaction tx, PublicationSegmentOwnerRegistry.Assignment slot) {
        Supplier<Long> action = segmentCommitBatcher == null
                ? () -> segmentOwner.decide(slot, tx.txId(), tx.deadlineMillis(), tx.participants())
                : () -> segmentCommitBatcher.commit(slot, tx.txId(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_DECISION, action);
    }

    /** 单槽中心决议成功后把不可伪造的已提交凭据传给同步物化阶段。 */
    private CommittedProof segmentDecisionProofStage(Transaction tx,
                                                     PublicationSegmentOwnerRegistry.Assignment slot) {
        Supplier<CommittedProof> action = () -> segmentOwner.decideWithProof(slot, tx.txId(),
                tx.startTs(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_DECISION, action);
    }

    /** 返回可选分段批量决议的累计分组形态。 */
    PublicationSegmentCommitBatcher.Stats segmentBatchStats() {
        return segmentCommitBatcher == null ? null : segmentCommitBatcher.stats();
    }

    /** 默认指标关闭时直接等待已持久发布水位覆盖本事务时间戳。 */
    private void segmentAwaitStage(Transaction tx, long commitTs) {
        if (!metricsEnabled()) {
            awaitSegmentPublication(tx, commitTs);
            return;
        }
        recordMeasuredStage(TransactionStage.SEGMENT_AWAIT,
                () -> awaitSegmentPublication(tx, commitTs));
    }

    /** no-op 指标下直接获取新协议的持久化开始快照。 */
    private BeginReservation ledgerBeginStage(IsolationLevel isolationLevel) {
        Supplier<BeginReservation> action = coordinatorRegistry != null
                ? () -> coordinatorRegistry.begin(isolationLevel)
                : beginBatcher == null
                ? () -> publicationLedger.begin(isolationLevel, options.limits().timeoutMillis(), false)
                : () -> beginBatcher.begin(isolationLevel);
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_BEGIN, action);
    }

    /** 返回实验性批量开始的累计批次规模。 */
    PublicationBeginBatcher.Stats ledgerBeginBatchStats() {
        return beginBatcher == null ? null : beginBatcher.stats();
    }

    /** no-op 指标下直接预留持久化提交时间戳。 */
    private long ledgerReserveStage(Transaction tx) {
        if (!metricsEnabled()) {
            return publicationLedger.reserveCommit(tx.txId(), tx.deadlineMillis());
        }
        return recordMeasuredStage(TransactionStage.LEDGER_RESERVE,
                () -> publicationLedger.reserveCommit(tx.txId(), tx.deadlineMillis()));
    }

    /** 单分片候选时间与中心 PREPARING 状态同账本槽原子预留。 */
    private long ledgerLocalReserveStage(Transaction tx, String participant) {
        Supplier<Long> action = localReserveBatcher == null
                ? () -> publicationLedger.reserveLocalCommit(tx.txId(), tx.deadlineMillis(), participant)
                : () -> localReserveBatcher.reserve(tx.txId(), tx.deadlineMillis(), participant);
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_RESERVE, action);
    }

    /** 返回实验性本地预留批次的累计聚合形态。 */
    PublicationLocalReserveBatcher.Stats ledgerLocalReserveStats() {
        return localReserveBatcher == null ? null : localReserveBatcher.stats();
    }

    /** 返回实验性本地补决议批次的累计聚合形态。 */
    PublicationLocalCompletionBatcher.Stats ledgerLocalCompletionStats() {
        return localCompletionBatcher == null ? null : localCompletionBatcher.stats();
    }

    /** 本地已提交证明满足候选时间后补齐中心与账本的相同决议。 */
    private boolean ledgerLocalDecisionStage(ParticipantRecord local, long commitTs, String participant) {
        Supplier<Boolean> action = localCompletionBatcher == null
                ? () -> publicationLedger.completeLocalCommitAfterFencing(local, commitTs, participant)
                : () -> localCompletionBatcher.complete(local, commitTs, participant);
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_DECISION, action);
    }

    /** no-op 指标下直接持久化中心与账本的原子决议。 */
    private void ledgerDecisionStage(Transaction tx, long commitTs) {
        if (!metricsEnabled()) {
            publicationLedger.decideCommitted(tx.txId(), commitTs, tx.participants());
            return;
        }
        recordMeasuredStage(TransactionStage.LEDGER_DECISION,
                () -> publicationLedger.decideCommitted(tx.txId(), commitTs, tx.participants()));
    }

    /** intent 写集合已持久化时在一个控制事务中分配时间戳并提交决议。 */
    private PublicationCommitResult ledgerAtomicCommitStage(Transaction tx) {
        Supplier<PublicationCommitResult> action = publicationCommitBatcher == null
                ? () -> publicationLedger.commitPreparedIntents(tx.txId(), tx.deadlineMillis(), tx.participants())
                : () -> publicationCommitBatcher.commit(tx.txId(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_ATOMIC_COMMIT, action);
    }

    /** 返回可选账本批量决议的累计形态，非批量模式没有统计。 */
    PublicationCommitBatcher.Stats ledgerBatchStats() {
        return publicationCommitBatcher == null ? null : publicationCommitBatcher.stats();
    }

    /** 返回当前事务管理器所用账本实例的批量控制事务阶段耗时。 */
    MysqlPublicationLedger.BatchTiming ledgerBatchTiming() {
        return publicationLedger == null ? null : publicationLedger.batchTiming();
    }

    /** no-op 指标下直接推进已决议账本的连续水位。 */
    private long ledgerPublishStage() {
        if (!metricsEnabled()) {
            return publicationLedger.advancePublished(256);
        }
        return recordMeasuredStage(TransactionStage.LEDGER_PUBLISH,
                () -> publicationLedger.advancePublished(256));
    }

    /** no-op 指标下直接等待自身提交被全局水位覆盖。 */
    private void ledgerAwaitStage(Transaction tx, long commitTs) {
        if (!metricsEnabled()) {
            publicationLedger.awaitPublished(tx.txId(), commitTs, options.limits().timeoutMillis());
            return;
        }
        recordMeasuredStage(TransactionStage.LEDGER_AWAIT,
                () -> publicationLedger.awaitPublished(tx.txId(), commitTs, options.limits().timeoutMillis()));
    }

    /** no-op 指标下直接执行服务器端 begin，避免计量包装分配。 */
    private Optional<BeginReservation> acceleratedBeginStage(Supplier<Optional<BeginReservation>> action) {
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.BEGIN_PUBLICATION, action);
    }

    /** no-op 指标下直接执行服务器端提交决议。 */
    private OptionalLong acceleratedCommitStage(Supplier<OptionalLong> action) {
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.COMMIT_PUBLICATION, action);
    }

    /**
     * 按需记录时间戳分配阶段。
     */
    private long nextTimestamp(TransactionStage stage) {
        if (!metricsEnabled()) {
            return timestampOracle.nextTimestamp();
        }
        return recordMeasuredStage(stage, timestampOracle::nextTimestamp);
    }

    /**
     * 按需记录 begin metadata 创建阶段。
     */
    private void createBeginMetadata(long txId, long startTs, IsolationLevel isolationLevel) {
        if (!metricsEnabled()) {
            metadataStore.create(txId, startTs, isolationLevel);
            return;
        }
        recordMeasuredStage(TransactionStage.BEGIN_METADATA, () -> metadataStore.create(txId, startTs, isolationLevel));
    }

    /**
     * 按需记录单分片 metadata 提交阶段。
     */
    private void markSingleShardCommittedStage(Transaction tx, String participant, long commitTs) {
        if (!metricsEnabled()) {
            metadataStore.persistSingleShardCommitted(tx.txId(), participant, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_METADATA_COMMIT,
                () -> metadataStore.persistSingleShardCommitted(tx.txId(), participant, commitTs)
        );
    }

    /**
     * 按需记录缓冲写预写阶段。
     */
    private void prewriteBufferedMutationsStage(Transaction tx) {
        if (!metricsEnabled()) {
            prewriteBufferedMutations(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.BUFFERED_PREWRITE, () -> prewriteBufferedMutations(tx));
    }

    /**
     * 按需记录非缓冲预写阶段。
     */
    private void prewriteEagerMutationStage(Transaction tx, String shardId, Mutation mutation, StorageAdapter adapter) {
        if (!metricsEnabled()) {
            prewriteEagerMutation(tx, shardId, mutation, adapter);
            return;
        }
        recordMeasuredStage(TransactionStage.EAGER_PREWRITE, () -> prewriteEagerMutation(tx, shardId, mutation, adapter));
    }

    /**
     * 按需记录显式 prepare 阶段。
     */
    private void prepareParticipantsStage(Transaction tx) {
        if (!metricsEnabled()) {
            runAcrossParticipants(
                    tx.participants(),
                    participant -> prepareParticipant(tx, participant),
                    options.crossShardCommitPolicy().parallelPrepare()
            );
            return;
        }
        recordMeasuredStage(
                TransactionStage.PREPARE,
                () -> runAcrossParticipants(
                        tx.participants(),
                        participant -> prepareParticipant(tx, participant),
                        options.crossShardCommitPolicy().parallelPrepare()
                )
        );
    }

    /**
     * 按需记录跨分片 metadata 提交阶段。
     */
    private void persistCommittedMetadataStage(Transaction tx, long commitTs, boolean explicitPrepareRequired) {
        if (!metricsEnabled()) {
            persistCommittedMetadata(tx, commitTs, explicitPrepareRequired);
            return;
        }
        recordMeasuredStage(
                TransactionStage.CROSS_SHARD_METADATA_COMMIT,
                () -> persistCommittedMetadata(tx, commitTs, explicitPrepareRequired)
        );
    }

    /**
     * 按需记录参与者提交阶段。
     */
    private void commitParticipantStage(TransactionStage stage, Transaction tx, String shardId, long commitTs) {
        if (!metricsEnabled()) {
            commitParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(stage, () -> commitParticipant(tx, shardId, commitTs));
    }

    /** 中心已确认后传递持久决议凭据，分片仍自行校验完整意向和本地终态。 */
    private void commitDecidedParticipantStage(Transaction tx, String shardId, CommittedProof proof) {
        Runnable action = () -> withShardPermit(shardId, () -> {
            ((MysqlStorageAdapter) adapter(shardId)).commitDecided(proof,
                    tx.bufferedMutations(shardId), true);
            return null;
        });
        if (!metricsEnabled()) {
            action.run();
            return;
        }
        recordMeasuredStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT, action);
    }

    /**
     * 按需记录单分片直接提交阶段。
     */
    private void directCommitSingleShardParticipantStage(Transaction tx, String shardId, long commitTs) {
        if (!metricsEnabled()) {
            directCommitSingleShardParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT,
                () -> directCommitSingleShardParticipant(tx, shardId, commitTs)
        );
    }

    /**
     * 按需记录回滚阶段。
     */
    private void rollbackStage(Transaction tx) {
        if (!metricsEnabled()) {
            rollbackInternal(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.ROLLBACK, () -> rollbackInternal(tx));
    }

    /**
     * 执行事务回滚的实际动作。
     */
    private void rollbackInternal(Transaction tx) {
        TxStatus decision = metadataStore.withPublicationLock(() -> {
            if (!tx.metadataPersisted()) {
                return TxStatus.ABORTED;
            }
            TxRecord record = metadataStore.get(tx.txId()).orElseThrow(() ->
                    unknownCommit(tx, new IllegalStateException("transaction metadata missing")));
            if (record.status() == TxStatus.COMMITTED) {
                return TxStatus.COMMITTED;
            }
            if (tx.decisionStarted() && tx.participants().size() == 1
                    && adapter(tx.participants().iterator().next()).supportsAtomicDirectCommit()) {
                String shardId = tx.participants().iterator().next();
                // 先在参与者侧隔离可能迟到的本地提交，再根据实际本地终态更新中心。
                var local = withShardPermit(shardId, () -> {
                    adapter(shardId).abort(tx.txId());
                    return adapter(shardId).participantRecord(tx.txId());
                });
                if (local.isEmpty() || local.get().txId() != tx.txId() || local.get().startTs() != tx.startTs()
                        || (local.get().status() != TxStatus.COMMITTED && local.get().status() != TxStatus.ABORTED)) {
                    throw unknownCommit(tx, new IllegalStateException("participant fencing did not establish a terminal decision"));
                }
                if (local.get().status() == TxStatus.COMMITTED) {
                    if (local.get().commitTs() == null) {
                        throw unknownCommit(tx, new IllegalStateException("participant commit timestamp missing"));
                    }
                    metadataStore.markSingleShardCommitted(tx.txId(), shardId, local.get().commitTs());
                    return TxStatus.COMMITTED;
                }
            }
            // markAborted 返回的终态才是权威，不能根据调用前读到的状态删除 intent。
            return metadataStore.markAborted(tx.txId()).status();
        });
        tx.status(decision);
        if (decision == TxStatus.COMMITTED) {
            releaseTransaction(tx);
            return;
        }
        runAcrossParticipants(
                tx.participants(),
                participant -> withShardPermit(participant, () -> {
                    adapter(participant).abort(tx.txId());
                    return null;
                }),
                options.crossShardCommitPolicy().parallelRollback()
        );
        markFinalizedQuietly(tx);
    }

    /**
     * 按需记录 intent 清理阶段。
     */
    private void cleanupIntentsStage(Transaction tx, String shardId) {
        if (!metricsEnabled()) {
            cleanupParticipantIntents(tx, shardId);
            return;
        }
        recordMeasuredStage(TransactionStage.INTENT_CLEANUP, () -> cleanupParticipantIntents(tx, shardId));
    }

    /**
     * 记录带返回值的计量阶段执行结果。
     */
    private <T> T recordMeasuredStage(TransactionStage stage, Supplier<T> action) {
        long startNanos = System.nanoTime();
        try {
            T result = action.get();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
            return result;
        } catch (RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * 记录无返回值的计量阶段执行结果。
     */
    private void recordMeasuredStage(TransactionStage stage, Runnable action) {
        long startNanos = System.nanoTime();
        try {
            action.run();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
        } catch (RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * 尽力记录成功指标，避免指标实现异常反向影响事务语义。
     */
    private void recordMetricSuccess(TransactionStage stage, long elapsedNanos) {
        try {
            metrics.recordSuccess(stage, elapsedNanos);
        } catch (RuntimeException | Error ignored) {
        }
    }

    /**
     * 尽力记录失败指标，避免遮蔽原始业务异常。
     */
    private void recordMetricFailure(TransactionStage stage, long elapsedNanos) {
        try {
            metrics.recordFailure(stage, elapsedNanos);
        } catch (RuntimeException | Error ignored) {
        }
    }
}
