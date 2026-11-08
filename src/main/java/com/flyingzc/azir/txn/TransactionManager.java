/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.txn;

import com.flyingzc.azir.api.Transaction;
import com.flyingzc.azir.api.TransactionCommitMode;
import com.flyingzc.azir.api.TransactionCommitResult;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.gc.NoOpSafePointStore;
import com.flyingzc.azir.gc.SafePointStore;
import com.flyingzc.azir.metadata.TransactionMetadataStore;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.metadata.FastTransactionBegin;
import com.flyingzc.azir.metadata.FastTransactionCommit;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.metrics.NoOpTransactionMetrics;
import com.flyingzc.azir.metrics.TransactionMetrics;
import com.flyingzc.azir.metrics.TransactionStage;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger.CommittedProof;
import com.flyingzc.azir.publication.SegmentPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.PublicationEntry;
import com.flyingzc.azir.publication.PublicationEntryStatus;
import com.flyingzc.azir.publication.PublicationCommitResult;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.ReadContext;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.ParticipantRecord;
import com.flyingzc.azir.storage.ScanPage;
import com.flyingzc.azir.tso.TimestampOracle;

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
 * Coordinates global MVCC transactions, durable decisions, participant finalization and snapshot protection.
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
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param adapters adapters
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, TransactionOptions.defaults(), adapters);
    }

    /**
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param adapters adapters
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final SafePointStore safePointStore,
                              final Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, TransactionOptions.defaults(), adapters);
    }

    /**
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final TransactionOptions options,
                              final Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters);
    }

    /**
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final TransactionOptions options,
                              final Map<String, StorageAdapter> adapters,
                              final TransactionMetrics metrics) {
        this(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE, options, adapters, metrics);
    }

    /**
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final SafePointStore safePointStore,
                              final TransactionOptions options,
                              final Map<String, StorageAdapter> adapters) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, NoOpTransactionMetrics.INSTANCE);
    }

    /**
     * Construct a TransactionManager instance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     */
    public TransactionManager(final TimestampOracle timestampOracle,
                              final TransactionMetadataStore metadataStore,
                              final SafePointStore safePointStore,
                              final TransactionOptions options,
                              final Map<String, StorageAdapter> adapters,
                              final TransactionMetrics metrics) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics, null);
    }

    /**
     * The new account mode is only constructed when authority is consistent on the control side and writes to resolve an commit indent.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics, publicationLedger, false);
    }

    /**
     * Verify pressure and isolation can be selected to enable bounded account book batch decision, with the default path to switch after acceptance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger,
                       final boolean batchLedgerCommits) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, 2);
    }

    /**
     * Isolation pressure measures can adjust the batch decision work-lines, and the production entrance is still subject to long and steady acceptance.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     * @param batchWorkers batch workers
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger,
                       final boolean batchLedgerCommits,
                       final int batchWorkers) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, false);
    }

    /**
     * Only visible enable single shard local direct commit with the default to keep a higher intent path of throughput.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     * @param batchWorkers batch workers
     * @param ledgerDirectSingleShard ledger direct single shard
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger,
                       final boolean batchLedgerCommits,
                       final int batchWorkers,
                       final boolean ledgerDirectSingleShard) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard, false, 1);
    }

    /**
     * Batch snapshot starts only with a visible test option and defaults to keep single begin of verify.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     * @param batchWorkers batch workers
     * @param ledgerDirectSingleShard ledger direct single shard
     * @param batchLedgerBegins batch ledger begins
     * @param beginWorkers begin workers
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger,
                       final boolean batchLedgerCommits,
                       final int batchWorkers,
                       final boolean ledgerDirectSingleShard,
                       final boolean batchLedgerBegins,
                       final int beginWorkers) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard,
                batchLedgerBegins, beginWorkers, null);
    }

    /**
     * The coordinator snapshot lease is only a visible run-time assembly and cannot be enabled at the same time as batch.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     * @param batchWorkers batch workers
     * @param ledgerDirectSingleShard ledger direct single shard
     * @param batchLedgerBegins batch ledger begins
     * @param beginWorkers begin workers
     * @param coordinatorRegistry coordinator registry
     */
    TransactionManager(final TimestampOracle timestampOracle,
                       final TransactionMetadataStore metadataStore,
                       final SafePointStore safePointStore,
                       final TransactionOptions options,
                       final Map<String, StorageAdapter> adapters,
                       final TransactionMetrics metrics,
                       final MysqlPublicationLedger publicationLedger,
                       final boolean batchLedgerCommits,
                       final int batchWorkers,
                       final boolean ledgerDirectSingleShard,
                       final boolean batchLedgerBegins,
                       final int beginWorkers,
                       final CoordinatorSnapshotRegistry coordinatorRegistry) {
        this(timestampOracle, metadataStore, safePointStore, options, adapters, metrics,
                publicationLedger, batchLedgerCommits, batchWorkers, ledgerDirectSingleShard,
                batchLedgerBegins, beginWorkers, coordinatorRegistry, null, null, null, null,
                false, false, false);
    }

    /**
     * Visible segment mode only corresponds to the activated new time line and same database metadata authority.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @return the operation result
     */
    static TransactionManager forSegment(final TimestampOracle timestampOracle,
                                         final TransactionMetadataStore metadataStore,
                                         final TransactionOptions options,
                                         final Map<String, StorageAdapter> adapters,
                                         final TransactionMetrics metrics,
                                         final MysqlSegmentPublicationLedger segmentLedger,
                                         final PublicationSegmentOwnerRegistry segmentOwner,
                                         final SegmentPublicationMaintenanceScheduler segmentScheduler) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, null);
    }

    /**
     * Allows bounded row batch decision queues when visible experiments are running.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @param segmentCommitBatcher segment commit batcher
     * @return the operation result
     */
    static TransactionManager forSegment(final TimestampOracle timestampOracle,
                                         final TransactionMetadataStore metadataStore,
                                         final TransactionOptions options,
                                         final Map<String, StorageAdapter> adapters,
                                         final TransactionMetrics metrics,
                                         final MysqlSegmentPublicationLedger segmentLedger,
                                         final PublicationSegmentOwnerRegistry segmentOwner,
                                         final SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         final PublicationSegmentCommitBatcher segmentCommitBatcher) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher, false);
    }

    /**
     * Division shard Physics 1PC is only enabled when a visible run of local terminal state recovery already exists.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @param segmentCommitBatcher segment commit batcher
     * @param segmentDirectSingleShard segment direct single shard
     * @return the operation result
     */
    static TransactionManager forSegment(final TimestampOracle timestampOracle,
                                         final TransactionMetadataStore metadataStore,
                                         final TransactionOptions options,
                                         final Map<String, StorageAdapter> adapters,
                                         final TransactionMetrics metrics,
                                         final MysqlSegmentPublicationLedger segmentLedger,
                                         final PublicationSegmentOwnerRegistry segmentOwner,
                                         final SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         final PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         final boolean segmentDirectSingleShard) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, false);
    }

    /**
     * Shard materialization and global release waiting only overtly under the parallel commit policy.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @param segmentCommitBatcher segment commit batcher
     * @param segmentDirectSingleShard segment direct single shard
     * @param segmentOverlapMaterialization segment overlap materialization
     * @return the operation result
     */
    static TransactionManager forSegment(final TimestampOracle timestampOracle,
                                         final TransactionMetadataStore metadataStore,
                                         final TransactionOptions options,
                                         final Map<String, StorageAdapter> adapters,
                                         final TransactionMetrics metrics,
                                         final MysqlSegmentPublicationLedger segmentLedger,
                                         final PublicationSegmentOwnerRegistry segmentOwner,
                                         final SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         final PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         final boolean segmentDirectSingleShard,
                                         final boolean segmentOverlapMaterialization) {
        return forSegment(timestampOracle, metadataStore, options, adapters, metrics,
                segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, segmentOverlapMaterialization, false);
    }

    /**
     * The segment centre decision receipt is open only to MySQL participant which is synchronized and not batch.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @param segmentCommitBatcher segment commit batcher
     * @param segmentDirectSingleShard segment direct single shard
     * @param segmentOverlapMaterialization segment overlap materialization
     * @param segmentFastMaterialization segment fast materialization
     * @return the operation result
     */
    static TransactionManager forSegment(final TimestampOracle timestampOracle,
                                         final TransactionMetadataStore metadataStore,
                                         final TransactionOptions options,
                                         final Map<String, StorageAdapter> adapters,
                                         final TransactionMetrics metrics,
                                         final MysqlSegmentPublicationLedger segmentLedger,
                                         final PublicationSegmentOwnerRegistry segmentOwner,
                                         final SegmentPublicationMaintenanceScheduler segmentScheduler,
                                         final PublicationSegmentCommitBatcher segmentCommitBatcher,
                                         final boolean segmentDirectSingleShard,
                                         final boolean segmentOverlapMaterialization,
                                         final boolean segmentFastMaterialization) {
        return new TransactionManager(timestampOracle, metadataStore, NoOpSafePointStore.INSTANCE,
                options, adapters, metrics, null, false, 1, false, false, 1,
                null, segmentLedger, segmentOwner, segmentScheduler, segmentCommitBatcher,
                segmentDirectSingleShard, segmentOverlapMaterialization, segmentFastMaterialization);
    }

    /**
     * Only one of the old ledgers and sub-books is allowed to bear global reading and writing time lines.
     *
     * @param timestampOracle timestamp oracle
     * @param metadataStore metadata store
     * @param safePointStore safe point store
     * @param options options
     * @param adapters adapters
     * @param metrics metrics
     * @param publicationLedger publication ledger
     * @param batchLedgerCommits batch ledger commits
     * @param batchWorkers batch workers
     * @param ledgerDirectSingleShard ledger direct single shard
     * @param batchLedgerBegins batch ledger begins
     * @param beginWorkers begin workers
     * @param coordinatorRegistry coordinator registry
     * @param segmentLedger segment ledger
     * @param segmentOwner segment owner
     * @param segmentScheduler segment scheduler
     * @param segmentCommitBatcher segment commit batcher
     * @param segmentDirectSingleShard segment direct single shard
     * @param segmentOverlapMaterialization segment overlap materialization
     * @param segmentFastMaterialization segment fast materialization
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private TransactionManager(final TimestampOracle timestampOracle,
                               final TransactionMetadataStore metadataStore,
                               final SafePointStore safePointStore,
                               final TransactionOptions options,
                               final Map<String, StorageAdapter> adapters,
                               final TransactionMetrics metrics,
                               final MysqlPublicationLedger publicationLedger,
                               final boolean batchLedgerCommits,
                               final int batchWorkers,
                               final boolean ledgerDirectSingleShard,
                               final boolean batchLedgerBegins,
                               final int beginWorkers,
                               final CoordinatorSnapshotRegistry coordinatorRegistry,
                               final MysqlSegmentPublicationLedger segmentLedger,
                               final PublicationSegmentOwnerRegistry segmentOwner,
                               final SegmentPublicationMaintenanceScheduler segmentScheduler,
                               final PublicationSegmentCommitBatcher segmentCommitBatcher,
                               final boolean segmentDirectSingleShard,
                               final boolean segmentOverlapMaterialization,
                               final boolean segmentFastMaterialization) {
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
            // The old manager rejected the converted durable mode during the construction period to avoid each begin having encountered lease external key fence.
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
     * Begin a new transaction.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public Transaction begin(final IsolationLevel isolationLevel) {
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
        } catch (final RuntimeException | Error ex) {
            if (registered == null || activeTransactions.remove(registered.txId(), registered)) {
                transactionPermits.release();
            }
            if (registered != null && coordinatorRegistry != null && registered.txId() < 0) {
                coordinatorRegistry.release(registered.txId());
            }
            throw ex;
        }
    }

    /**
     * Sharing watermark snapshot and new lease must be in the same control durable storage for transaction before creating transaction objects.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private Transaction beginFromLedger(final IsolationLevel isolationLevel) {
        BeginReservation reservation = ledgerBeginStage(isolationLevel);
        return new Transaction(reservation.txId(), reservation.startTs(), isolationLevel,
                reservation.deadlineMillis(), transactionOwner);
    }

    /**
     * The start of the segment mode snapshot and transaction lease will not be exposed until transaction durable storage.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private Transaction beginFromSegment(final IsolationLevel isolationLevel) {
        BeginReservation reservation = segmentBeginStage(isolationLevel);
        return new Transaction(reservation.txId(), reservation.startTs(), isolationLevel,
                reservation.deadlineMillis(), transactionOwner);
    }

    /**
     * Only the concurrent sheet is enabled when timestamp and metadata determine to share a control side authority.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private Optional<Transaction> acceleratedBegin(final IsolationLevel isolationLevel) {
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

    /**
     * Not MySQL or different control authority continues to use the original release protocol.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private Transaction legacyBegin(final IsolationLevel isolationLevel) {
        return metadataStore.withPublicationLock(() -> {
            ensureOpen();
            metadataStore.assertNoPendingPublications();
            long txId = nextTimestamp(TransactionStage.BEGIN_TSO);
            Transaction created = new Transaction(txId, txId, isolationLevel,
                    metadataStore.currentTimeMillis() + options.limits().timeoutMillis(), transactionOwner);
            // An independent snapshot registration protection lazy read-only transaction cannot rely on writing to transaction metadata.
            metadataStore.registerSnapshot(txId, txId, created.deadlineMillis());
            if (!options.lazyBeginMetadata()) {
                createBeginMetadata(txId, txId, isolationLevel);
                created.metadataPersisted(true);
            }
            return created;
        });
    }

    /**
     * Read the currently visible data for the key or transaction.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<byte[]> get(final Transaction tx, final String shardId, final String key) {
        return get(tx, shardId, key, beginReadStatement(tx));
    }

    /**
     * Execute a reading point under the fixed snapshot of the same SQL.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @param snapshot snapshot
     * @return the operation result
     */
    public Optional<byte[]> get(final Transaction tx, final String shardId, final String key, final StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
        StorageAdapter adapter = adapter(shardId);
        Optional<byte[]> bufferedValue = bufferedRead(tx, shardId, key);
        if (bufferedValue != null) {
            return bufferedValue;
        }
        Optional<VersionedValue> result = withShardPermit(shardId, () -> adapter.get(
                new ReadContext(tx.txId(), snapshot.readTs(), tx.isolationLevel()),
                key));
        ensureLeaseValid(tx);
        if (result.isEmpty() || result.get().deleted()) {
            return Optional.empty();
        }
        return Optional.of(result.get().value().clone());
    }

    /**
     * Scan the key range using the current snapshot.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @return the operation result
     */
    public List<VersionedValue> scan(final Transaction tx, final String shardId, final String startKeyInclusive, final String endKeyExclusive) {
        return scan(tx, shardId, startKeyInclusive, endKeyExclusive, beginReadStatement(tx));
    }

    /**
     * Create statement-level reading snapshot for binding the current transaction.
     *
     * @param tx transaction context
     * @return the operation result
     */
    public StatementReadSnapshot beginReadStatement(final Transaction tx) {
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

    /**
     * The original verify statement after SQL has finished sorting or aggregating is still in lease and no new RC timestamp is allocated.
     *
     * @param tx transaction context
     * @param snapshot snapshot
     */
    public void validateReadStatement(final Transaction tx, final StatementReadSnapshot snapshot) {
        validateStatement(tx, snapshot);
    }

    /**
     * Use the specified statement-level snapshot scanning key range to reuse the same visible boundary once with multiple tables or more shard scans in SQL.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @param snapshot snapshot
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public List<VersionedValue> scan(final Transaction tx,
                                     final String shardId,
                                     final String startKeyInclusive,
                                     final String endKeyExclusive,
                                     final StatementReadSnapshot snapshot) {
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

    /**
     * By binary key, consolidates a page storage result and your own buffer change. Empty pages can also continue in position.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param startKeyInclusive start key inclusive
     * @param endKeyExclusive end key exclusive
     * @param snapshot snapshot
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public ScanPage scanPage(final Transaction tx, final String shardId, final String startKeyInclusive, final String endKeyExclusive,
                             final StatementReadSnapshot snapshot, final String afterKeyExclusive, final int limit) {
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
                if (mutation.type() == com.flyingzc.azir.common.MutationType.DELETE) {
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
     * Write a key-value pair in the transaction.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @param value value to write
     */
    public void put(final Transaction tx, final String shardId, final String key, final byte[] value) {
        Objects.requireNonNull(value, "value");
        write(tx, shardId, Mutation.put(key, value));
    }

    /**
     * Delete the key in the transaction.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     */
    public void delete(final Transaction tx, final String shardId, final String key) {
        write(tx, shardId, Mutation.delete(key));
    }

    /**
     * Execute a callback in the transaction template and follow policy retry.
     *
     * @param <T> value type
     * @param isolationLevel global transaction isolation level
     * @param callback callback
     * @return the operation result
     */
    public <T> T execute(final IsolationLevel isolationLevel, final TransactionCallback<T> callback) {
        MvccException lastRetryable = null;
        for (int attempt = 1; attempt <= options.retryPolicy().maxAttempts(); attempt++) {
            Transaction tx = begin(isolationLevel);
            try {
                T result = callback.doInTransaction(tx);
                commit(tx);
                return result;
            } catch (final MvccException ex) {
                safeRollback(tx);
                if (tx.decisionStarted() || tx.status() == TxStatus.COMMITTED) {
                    throw ex.errorCode() == ErrorCode.TRANSACTION_STATUS_UNKNOWN ? ex : unknownCommit(tx, ex);
                }
                if (!options.retryPolicy().shouldRetry(ex, attempt)) {
                    throw ex;
                }
                lastRetryable = ex;
                sleepQuietly(options.retryPolicy().nextDelayMillis(attempt - 1));
            } catch (final RuntimeException ex) {
                safeRollback(tx);
                throw ex;
            }
        }
        throw lastRetryable == null
                ? new MvccException(ErrorCode.ILLEGAL_STATE, "retry loop exhausted without terminal exception", false, null)
                : lastRetryable;
    }

    /**
     * Finalize transaction intents as committed versions.
     *
     * @param tx transaction context
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    public TransactionCommitResult commit(final Transaction tx) {
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
        } catch (final RuntimeException ex) {
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
     * Commit read-only transaction.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private TransactionCommitResult commitReadOnly(final Transaction tx) {
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
            } catch (final RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
            return timestamp;
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * Write transaction first, participant intent, then durable slot commit Center decision and wait for release.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private TransactionCommitResult commitSegmentWrite(final Transaction tx) {
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
        } catch (final RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /**
     * Center decision is after durable storage parallel materialization shard with a prefix, and both are still waiting to succeed before returning.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    private TransactionCommitResult commitSegmentOverlapped(final Transaction tx, final long commitTs) {
        CompletableFuture<Void> participants;
        try {
            // Version is still bound by committeeTs watermark before release; materialization shard has not been corrected by read path after release commit intent.
            participants = CompletableFuture.runAsync(() -> runAcrossParticipants(tx.participants(),
                    participant -> commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT,
                            tx, participant, commitTs),
                    true), participantExecutor);
        } catch (final RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        RuntimeException publicationFailure = null;
        try {
            segmentAwaitStage(tx, commitTs);
        } catch (final RuntimeException ex) {
            publicationFailure = ex;
        }
        Throwable participantFailure = null;
        try {
            participants.join();
        } catch (final CompletionException ex) {
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
                    ? runtime
                    : new CompletionException(participantFailure));
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /**
     * Single shard First durable storage candidate slot, then local transaction directly down version and participant terminal state.
     *
     * @param tx transaction context
     * @param participant participant
     * @return the operation result
     */
    private TransactionCommitResult commitSegmentSingleShardDirect(final Transaction tx, final String participant) {
        registerWriteParticipants(tx);
        ensureLeaseValid(tx);
        PublicationSegmentOwnerRegistry.Assignment slot = segmentReserveStage(tx);
        tx.decisionStarted(true);
        segmentClaimLocalStage(tx, slot, participant);
        try {
            directCommitSingleShardParticipantStage(tx, participant, slot.commitTs());
        } catch (final RuntimeException ex) {
            return resolveSegmentDirectOutcome(tx, participant, slot, ex);
        }
        // DirectCommit's normal return means local transaction already commit; read durable only when responding to unknown.
        return finishSegmentDirectCommit(tx, participant, slot,
                new ParticipantRecord(tx.txId(), tx.startTs(), TxStatus.COMMITTED, slot.commitTs()));
    }

    /**
     * Local Responds to unknown first by fending shard and then pressing durable terminal state patch centre commit or aborting.
     *
     * @param tx transaction context
     * @param participant participant
     * @param slot slot
     * @param original original
     * @return the operation result
     */
    private TransactionCommitResult resolveSegmentDirectOutcome(final Transaction tx, final String participant,
                                                                final PublicationSegmentOwnerRegistry.Assignment slot,
                                                                final RuntimeException original) {
        ParticipantRecord local;
        try {
            withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            });
            local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "segmented direct participant lacks durable terminal proof", false, tx.txId()));
        } catch (final RuntimeException ex) {
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
        } catch (final RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        tx.status(TxStatus.ABORTED);
        tx.decisionStarted(false);
        throw original;
    }

    /**
     * The real local COMMITTED certificate patches the center decision and waits for global already published watermark.
     *
     * @param tx transaction context
     * @param participant participant
     * @param slot slot
     * @param local local
     * @return the operation result
     */
    private TransactionCommitResult finishSegmentDirectCommit(final Transaction tx, final String participant,
                                                              final PublicationSegmentOwnerRegistry.Assignment slot,
                                                              final ParticipantRecord local) {
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

    /**
     * Only watermark that has been driven by an independent background launcher durable can be identified as commit.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @throws MvccException if the operation cannot complete
     */
    private void awaitSegmentPublication(final Transaction tx, final long commitTs) {
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

    /**
     * New protocol all transaction scripts start with durable storage participant, then release centre decision and continuous watermark.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private TransactionCommitResult commitLedgerWrite(final Transaction tx) {
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
        } catch (final RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /**
     * Single shard leaves a candidate for PENDING that can be recovery and once again local transaction goes directly to version and terminal state.
     *
     * @param tx transaction context
     * @param participant participant
     * @return the operation result
     */
    private TransactionCommitResult commitLedgerSingleShardDirect(final Transaction tx, final String participant) {
        registerWriteParticipants(tx);
        ensureLeaseValid(tx);
        tx.decisionStarted(true);
        long commitTs = ledgerLocalReserveStage(tx, participant);
        try {
            directCommitSingleShardParticipantStage(tx, participant, commitTs);
        } catch (final RuntimeException ex) {
            return resolveLedgerDirectOutcome(tx, participant, commitTs, ex);
        }
        return finishLedgerDirectCommit(tx, participant, commitTs);
    }

    /**
     * Local Responds to loss by pressing the real terminal state patch centre decision or rollback safe.
     *
     * @param tx transaction context
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @param original original
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private TransactionCommitResult resolveLedgerDirectOutcome(final Transaction tx, final String participant,
                                                               final long commitTs, final RuntimeException original) {
        ParticipantRecord local;
        try {
            withShardPermit(participant, () -> {
                adapter(participant).abort(tx.txId());
                return null;
            });
            local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                    "direct participant has no durable terminal record", false, tx.txId()));
        } catch (final RuntimeException ex) {
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
            } catch (final RuntimeException publicationFailure) {
                backgroundFailures.increment();
            }
        } catch (final RuntimeException ex) {
            throw unknownCommit(tx, ex);
        }
        tx.status(TxStatus.ABORTED);
        tx.decisionStarted(false);
        throw original;
    }

    /**
     * Reconcile local commit proof to complete the centre and book decision and wait until global is released.
     *
     * @param tx transaction context
     * @param participant participant
     * @param commitTs durable commit timestamp
     * @return the operation result
     */
    private TransactionCommitResult finishLedgerDirectCommit(final Transaction tx, final String participant, final long commitTs) {
        ParticipantRecord local = adapter(participant).participantRecord(tx.txId()).orElseThrow(() -> new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
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
     * Commit single shard transaction.
     *
     * @param tx transaction context
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private TransactionCommitResult commitSingleShard(final Transaction tx, final String participant) {
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
                    // Local version and transaction records first atomic commit and centre decision records recovery when lost.
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
            } catch (final MvccException ex) {
                if (atomicDirect && !tx.status().equals(TxStatus.COMMITTED)
                        && (ex.errorCode() == ErrorCode.WRITE_CONFLICT || ex.errorCode() == ErrorCode.LOCK_CONFLICT)) {
                    tx.decisionStarted(false);
                    metadataStore.markAborted(tx.txId());
                    tx.status(TxStatus.ABORTED);
                    throw ex;
                }
                throw unknownCommit(tx, ex);
            } catch (final RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
        completeTransaction(tx, true);
        return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.SINGLE_SHARD_FAST_PATH);
    }

    /**
     * Commit cross-shard transaction.
     *
     * @param tx transaction context
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private TransactionCommitResult commitCrossShard(final Transaction tx) {
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

        // Multi shard transaction first durable storage but recovery states, then commit participates in shard each to ensure recovery after the anomaly.
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
                // Metadata decision has been durable storage; lease is released by aniso materialization thread after the markup has been completed.
                releaseLocalTransaction(tx);
                return new TransactionCommitResult(tx.txId(), commitTs, TransactionCommitMode.ASYNC_PRIMARY_COMMIT);
            }
            // Sync commit mode first commit main participant and then commit with the rest of participant, keeping an unknown abnormal border.
            commitParticipantStage(TransactionStage.PRIMARY_PARTICIPANT_COMMIT, tx, primaryParticipant, commitTs);
            Set<String> secondaryParticipants = secondaryParticipants(tx.participants(), primaryParticipant);
            runAcrossParticipants(
                    secondaryParticipants,
                    participant -> commitParticipantStage(
                            TransactionStage.SECONDARY_PARTICIPANT_COMMIT,
                            tx,
                            participant,
                            commitTs),
                    options.crossShardCommitPolicy().parallelCommit());
        } catch (final RuntimeException ex) {
            completeTransaction(tx, false);
            throw unknownCommit(tx, ex);
        }

        completeTransaction(tx, true);
        return new TransactionCommitResult(
                tx.txId(),
                commitTs,
                options.crossShardCommitPolicy().asyncCommit()
                        ? TransactionCommitMode.ASYNC_PRIMARY_COMMIT
                        : TransactionCommitMode.TWO_PHASE_COMMIT);
    }

    /**
     * Priority is given to merging concurrent and decision in the same MySQL authority; the rest achieves retention of the original agreement.
     *
     * @param tx transaction context
     * @param explicitPrepareRequired explicit prepare required
     * @return the operation result
     */
    private long publishCrossShardDecision(final Transaction tx, final boolean explicitPrepareRequired) {
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
            } catch (final RuntimeException ex) {
                throw unknownCommit(tx, ex);
            }
        });
    }

    /**
     * Close resources and background executors.
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
        // Async committee may continue the commit inent cleanup mission, which must first draw async before turning off the cleanup.
        shutdownExecutor(intentCleanupExecutor, deadline);
        shutdownExecutor(participantExecutor, deadline);
        // Unfinished rollback keeps durable storage lease and recovery records, only local writing buffer releases the closed manager.
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
     * Schedule a step participant commit task.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @param participants participants
     */
    private void scheduleAsyncParticipantCommit(final Transaction tx, final long commitTs, final Set<String> participants) {
        if (participants.isEmpty()) {
            return;
        }
        Set<String> stableParticipants = Set.copyOf(participants);
        submitOrRun(asyncCommitExecutor, () -> {
            boolean succeeded = true;
            for (String participant : stableParticipants) {
                try {
                    commitParticipantStage(TransactionStage.ASYNC_PARTICIPANT_COMMIT, tx, participant, commitTs);
                } catch (final RuntimeException ex) {
                    backgroundFailures.increment();
                    succeeded = false;
                }
            }
            finishAsyncCommit(tx, succeeded);
        });
    }

    /**
     * After the step materialization, record completion and release snapshot; failed decision is still completed by an recovery task.
     *
     * @param tx transaction context
     * @param finalized finalized
     */
    private void finishAsyncCommit(final Transaction tx, final boolean finalized) {
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
        } catch (final RuntimeException ex) {
            backgroundFailures.increment();
        }
    }

    /**
     * Durable storage cross-shard transaction Readys medium metadata.
     *
     * @param tx transaction context
     */
    private void persistPreparingMetadata(final Transaction tx) {
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
     * Durable storage cross-shard transaction has commit metadata.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @param explicitPrepareRequired explicit prepare required
     */
    private void persistCommittedMetadata(final Transaction tx, final long commitTs, final boolean explicitPrepareRequired) {
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
     * Select the main cross-shard transaction participant.
     *
     * @param participants participants
     * @return the operation result
     */
    private String primaryParticipant(final Set<String> participants) {
        return participants.stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new MvccException(ErrorCode.ILLEGAL_STATE, "cross-shard tx has no participants", false, null));
    }

    /**
     * Calculates a collection from participant other than the main participant.
     *
     * @param participants participants
     * @param primaryParticipant primary participant
     * @return the operation result
     */
    private Set<String> secondaryParticipants(final Set<String> participants, final String primaryParticipant) {
        return participants.stream()
                .filter(participant -> !participant.equals(primaryParticipant))
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    /**
     * Rollback transaction.
     *
     * @param tx transaction context
     */
    public void rollback(final Transaction tx) {
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

    /**
     * Division decision maintains the unknown outcome when it has started but is missing the terminal state slot, avoiding blind coverage of the commit in transit.
     *
     * @param tx transaction context
     */
    private void rollbackSegment(final Transaction tx) {
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
            TxRecord current = metadataStore.get(tx.txId()).orElseThrow(() -> unknownCommit(tx, new IllegalStateException("claimed segment slot has no metadata")));
            if (current.commitTs() != null) {
                throw unknownCommit(tx, new IllegalStateException(
                        "segmented local candidate requires durable participant proof"));
            }
        }
        if (tx.metadataPersisted()) {
            TxRecord decided = metadataStore.markAborted(tx.txId());
            if (decided.status() == TxStatus.COMMITTED) {
                PublicationEntry committed = segmentLedger.findByTxId(tx.txId()).orElseThrow(() -> unknownCommit(tx, new IllegalStateException("committed metadata lost segmented slot")));
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

    /**
     * The new protocol first confirms the centre terminal state, then idempotent fincing participant and unblocks the account book watermark.
     *
     * @param tx transaction context
     */
    private void rollbackLedger(final Transaction tx) {
        PublicationEntry entry = publicationLedger.findByTxId(tx.txId()).orElse(null);
        if (tx.status() == TxStatus.COMMITTED || entry != null && entry.status() == PublicationEntryStatus.COMMITTED) {
            tx.status(TxStatus.COMMITTED);
            releaseTransaction(tx);
            return;
        }
        if (entry != null && entry.status() == PublicationEntryStatus.PENDING) {
            TxRecord current = metadataStore.get(tx.txId()).orElseThrow(() -> unknownCommit(tx, new IllegalStateException("pending publication has no metadata")));
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
        // The response to preset commit may be lost; the central ABORTED line lock will wait until it is set aside and then read the true book.
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
     * Read transaction metadata records.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    public Optional<TxRecord> getRecord(final long txId) {
        return metadataStore.get(txId);
    }

    /**
     * Add a change to transaction write set.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param mutation mutation
     */
    private void write(final Transaction tx, final String shardId, Mutation mutation) {
        ensureActive(tx);
        ensureLeaseValid(tx);
        adapter(shardId);
        com.flyingzc.azir.storage.Utf8KeyOrder.encode(mutation.key());
        tx.reserveWrite(shardId, mutation, options.limits().maxWriteKeys(), options.limits().maxWriteBytes());
        // Copy value avoids the caller changing arrays to influence write set already billed, prewrite or background commit.
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
        } catch (final RuntimeException | Error ex) {
            tx.markRollbackOnly();
            throw ex;
        }
    }

    /**
     * Prewrite with conflict awaiting strategy entry of non- buffer single changes.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param mutation mutation
     * @param adapter adapter
     * @throws MvccException if the operation cannot complete
     */
    private void prewriteEagerMutation(final Transaction tx, final String shardId, final Mutation mutation, final StorageAdapter adapter) {
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
            } catch (final MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on key " + mutation.key() + ", tx=" + tx.txId() + ", shard=" + shardId,
                            true,
                            tx.txId(),
                            ex);
                }
                long delayMillis = Math.min(
                        lockWaitPolicy.nextDelayMillis(attempt++),
                        remainingMillis(deadlineNanos));
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * Require the transaction to remain usable.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    private void ensureActive(final Transaction tx) {
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
     * Calculates the transaction -reading snapshot timestamp.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private long readTs(final Transaction tx) {
        return tx.isolationLevel() == IsolationLevel.REPEATABLE_READ
                ? tx.startTs()
                : segmentLedger != null ? segmentLedger.currentPublished()
                        : publicationLedger == null ? nextTimestamp(TransactionStage.READ_TSO)
                                : publicationLedger.currentPublished();
    }

    /**
     * Validate Read snapshot not yet safe point collection.
     *
     * @param readTs snapshot read timestamp
     * @param txId transaction identifier
     * @throws MvccException if the operation cannot complete
     */
    private void ensureSnapshotVisible(final long readTs, final long txId) {
        long safePoint = segmentLedger != null ? segmentLedger.currentSafePoint()
                : publicationLedger == null
                        ? safePointStore.currentSafePoint()
                        : publicationLedger.currentSafePoint();
        if (readTs < safePoint) {
            throw new MvccException(
                    ErrorCode.SNAPSHOT_TOO_OLD,
                    "snapshot too old, readTs=" + readTs + ", safePoint=" + safePoint,
                    false,
                    txId);
        }
    }

    /**
     * Gets the specified shard storage adapter.
     *
     * @param shardId logical shard identifier
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private StorageAdapter adapter(final String shardId) {
        StorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "unknown shard: " + shardId, false, null);
        }
        return adapter;
    }

    /**
     * Determine whether participants require an explicit prepare phase.
     *
     * @param participants participants
     * @return the operation result
     */
    private boolean requiresExplicitPreparePhase(final Set<String> participants) {
        for (String participant : participants) {
            if (adapter(participant).requiresExplicitPreparePhase()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines whether to wait for retry in post-conflict situations.
     *
     * @param ex ex
     * @param lockWaitPolicy lock wait policy
     * @return the operation result
     */
    private boolean shouldWaitOnConflict(final MvccException ex, final LockWaitPolicy lockWaitPolicy) {
        return lockWaitPolicy.conflictMode() == ConflictMode.WAIT
                && TransientStorageConflictDetector.isTransientConflict(ex);
    }

    /**
     * Calculates the number of milliseconds remaining from the deadline.
     *
     * @param deadlineNanos deadline nanos
     * @return the operation result
     */
    private long remainingMillis(final long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0L) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(remainingNanos);
    }

    /**
     * Rollback transaction safe and swallow rollback abnormal.
     *
     * @param tx transaction context
     */
    private void safeRollback(final Transaction tx) {
        try {
            rollback(tx);
        } catch (final RuntimeException ignored) {
        }
    }

    /**
     * Execute the operation across participating shards.
     *
     * @param participants participants
     * @param operation operation
     * @param parallel parallel
     * @throws CompletionException if the operation cannot complete
     */
    private void runAcrossParticipants(final Set<String> participants, final Consumer<String> operation, final boolean parallel) {
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
                    } catch (final Throwable ex) {
                        // The parallel participant thrown Error must also be passed back to the caller to avoid miscalculation of the commit path.
                        failures.add(ex);
                        throw ex;
                    }
                }, participantExecutor))
                .toArray(CompletableFuture[]::new);

        try {
            CompletableFuture.allOf(futures).join();
        } catch (final CompletionException ignored) {
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
     * Quietly wait for the specified milliseconds.
     *
     * @param delayMillis delay millis
     * @throws MvccException if the operation cannot complete
     */
    private void sleepQuietly(final long delayMillis) {
        if (delayMillis <= 0L) {
            return;
        }
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for retry", true, null, ex);
        }
    }

    /**
     * Insert transaction buffered write set to prewrite in each shard.
     *
     * @param tx transaction context
     */
    private void prewriteBufferedMutations(final Transaction tx) {
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
                options.crossShardCommitPolicy().parallelPrewrite());
    }

    /**
     * Waiting for a conflict with the prewrite to specify an shard change.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param mutations transaction write set
     * @throws MvccException if the operation cannot complete
     */
    private void prewriteMutations(final Transaction tx, final String shardId, final List<Mutation> mutations) {
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
            } catch (final MvccException ex) {
                if (!shouldWaitOnConflict(ex, lockWaitPolicy)) {
                    throw ex;
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new MvccException(
                            ErrorCode.LOCK_WAIT_TIMEOUT,
                            "lock wait timeout on shard " + shardId + ", tx=" + tx.txId(),
                            true,
                            tx.txId(),
                            ex);
                }
                // The waiting-type lock strategy will only eject retry in an instant conflict and avoid making meaningless retry for a permanent error.
                long delayMillis = Math.min(lockWaitPolicy.nextDelayMillis(attempt++), remainingMillis(deadlineNanos));
                sleepQuietly(delayMillis);
            }
        }
    }

    /**
     * Read the latest changes from transaction local Write buffer.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param key business key
     * @return the operation result
     */
    private Optional<byte[]> bufferedRead(final Transaction tx, final String shardId, final String key) {
        if (!options.bufferWrites()) {
            return null;
        }
        Optional<Mutation> mutation = tx.bufferedMutation(shardId, key);
        if (mutation.isEmpty()) {
            return null;
        }
        return mutation.get().type() == com.flyingzc.azir.common.MutationType.DELETE
                ? Optional.empty()
                : Optional.of(mutation.get().value().clone());
    }

    /**
     * Determines whether the commit policy needs a parallel implementer.
     *
     * @param policy policy
     * @return the operation result
     */
    private boolean requiresParallelExecutor(final CrossShardCommitPolicy policy) {
        return policy.parallelPrewrite()
                || policy.parallelPrepare()
                || policy.parallelCommit()
                || policy.parallelRollback()
                || policy.asyncCommit();
    }

    /**
     * Turn off the parallel implementer.
     *
     * @param executor executor
     * @param deadlineNanos deadline nanos
     */
    private void shutdownExecutor(final ExecutorService executor, final long deadlineNanos) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                // Unfinished materialization by durable storage decision recovery cannot prevent the closing of an infinite waiting period.
                executor.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    /**
     * Creates a background linear factory with a prefix name.
     *
     * @param prefix prefix
     * @return the operation result
     */
    private ThreadFactory namedFactory(final String prefix) {
        return Thread.ofPlatform()
                .daemon(true)
                .name(prefix, 0)
                .factory();
    }

    /**
     * Commit specifies participation in shard.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param commitTs durable commit timestamp
     */
    private void commitParticipant(final Transaction tx, final String shardId, final long commitTs) {
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
                    } catch (final RuntimeException ex) {
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
     * Direct commit single shard participant.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param commitTs durable commit timestamp
     */
    private void directCommitSingleShardParticipant(final Transaction tx, final String shardId, final long commitTs) {
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

    /**
     * Returns the bounded execution statistics that you can call to export to the monitoring system.
     *
     * @return the operation result
     */
    public TransactionRuntimeStats runtimeStats() {
        return new TransactionRuntimeStats(activeTransactions.size(), queuedTasks(asyncCommitExecutor),
                queuedTasks(intentCleanupExecutor), synchronousFallbacks.sum(), backgroundFailures.sum());
    }

    /**
     * Creates an background implementer with fixed threads and bounded waiting queues.
     *
     * @param prefix prefix
     * @return the operation result
     */
    private ThreadPoolExecutor boundedExecutor(final String prefix) {
        int workers = options.crossShardCommitPolicy().asyncCommitWorkers();
        return new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(options.limits().asyncQueueCapacity()), namedFactory(prefix),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Decision jobs are completed by the current line when queue is full or closing, without discarding commit.
     *
     * @param executor executor
     * @param action action
     */
    private void submitOrRun(final ExecutorService executor, final Runnable action) {
        try {
            executor.execute(action);
        } catch (final RejectedExecutionException ex) {
            synchronousFallbacks.increment();
            action.run();
        }
    }

    /**
     * Returns the background implementer waiting for queue length.
     *
     * @param executor executor
     * @return the operation result
     */
    private int queuedTasks(final ExecutorService executor) {
        return executor instanceof ThreadPoolExecutor pool ? pool.getQueue().size() : 0;
    }

    /**
     * Limits the in-transit operation of each shard to an explicit failure when budget is exhausted.
     *
     * @param <T> value type
     * @param shardId logical shard identifier
     * @param action action
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private <T> T withShardPermit(final String shardId, final Supplier<T> action) {
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
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "interrupted while waiting for shard budget",
                    false, null, ex);
        } finally {
            if (acquired) {
                permits.release();
            }
        }
    }

    /**
     * Register full participant before the first durable storage writing to ensure that all inents can be located after the process crashes.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    private void registerWriteParticipants(final Transaction tx) {
        ensureLeaseValid(tx);
        TxRecord record;
        if (!tx.metadataPersisted()) {
            // Lease precedes this record durable storage; prewrite is still being executed after registration has been completed.
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

    /**
     * Validate transaction with statement ownership, keep fixed snapshot for RR and cross-shard SQL.
     *
     * @param tx transaction context
     * @param snapshot snapshot
     * @throws MvccException if the operation cannot complete
     */
    private void validateStatement(final Transaction tx, final StatementReadSnapshot snapshot) {
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

    /**
     * Reject lease expired transaction; once the query is completed, it must also be checked to avoid returning the result read during the expired GC.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    private void ensureLeaseValid(final Transaction tx) {
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

    /**
     * The manager refuses to accept new transaction and new operations after closing.
     *
     * @throws MvccException if the operation cannot complete
     */
    private void ensureOpen() {
        if (closed.get()) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction manager closed", false, null);
        }
    }

    /**
     * Transaction operations, including rollback, must come from the manager that created this transaction.
     *
     * @param tx transaction context
     * @throws MvccException if the operation cannot complete
     */
    private void ensureOwned(final Transaction tx) {
        Objects.requireNonNull(tx, "tx");
        if (!tx.ownedBy(transactionOwner)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "transaction belongs to another manager", false, tx.txId());
        }
    }

    /**
     * Release local slots, durable storage lease and transaction due transaction, processed by the recovery task.
     */
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

    /**
     * Mark decision transaction as complete and record recovery progress by materialization.
     *
     * @param tx transaction context
     * @param finalized finalized
     */
    private void completeTransaction(final Transaction tx, final boolean finalized) {
        tx.status(TxStatus.COMMITTED);
        if (finalized) {
            markFinalizedQuietly(tx);
        }
        releaseTransaction(tx);
    }

    /**
     * Release snapshot and local resources; lease expires collection when release failed.
     *
     * @param tx transaction context
     */
    private void releaseTransaction(final Transaction tx) {
        try {
            if (segmentLedger != null) {
                segmentLedger.releaseSnapshot(tx.txId());
            } else if (publicationLedger == null) {
                metadataStore.releaseSnapshot(tx.txId());
            } else if (coordinatorRegistry == null || tx.txId() >= 0
                    || !coordinatorRegistry.release(tx.txId())) {
                publicationLedger.releaseSnapshot(tx.txId());
            }
        } catch (final RuntimeException ex) {
            backgroundFailures.increment();
        } finally {
            releaseLocalTransaction(tx);
        }
    }

    /**
     * Only the transaction slot of this instance is released without changes to durable storage lease or decision.
     *
     * @param tx transaction context
     */
    private void releaseLocalTransaction(final Transaction tx) {
        if (activeTransactions.remove(tx.txId(), tx)) {
            transactionPermits.release();
        }
    }

    /**
     * Record materialization completed; recovery replay left when failed cannot be converted to business failure.
     *
     * @param tx transaction context
     */
    private void markFinalizedQuietly(final Transaction tx) {
        if (!tx.metadataPersisted() || (options.deferIntentCleanup() && tx.status() == TxStatus.COMMITTED)) {
            return;
        }
        try {
            metadataStore.markFinalized(tx.txId());
        } catch (final RuntimeException ex) {
            backgroundFailures.increment();
        }
    }

    /**
     * Construct an unknown-commit-outcome exception that forbids blindly rerunning the business transaction.
     *
     * @param tx transaction context
     * @param cause cause
     * @return the operation result
     */
    private MvccException unknownCommit(final Transaction tx, final RuntimeException cause) {
        if (tx.status() != TxStatus.COMMITTED) {
            tx.status(TxStatus.PREPARING);
        }
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "resolve transaction decision before retrying business: " + tx.txId(), false, tx.txId(), cause);
    }

    /**
     * Press the UTF-8 without symbol byte order used by MySQL Varbinary.
     *
     * @param left left
     * @param right right
     * @return the operation result
     */
    private static int compareKeys(final String left, final String right) {
        return com.flyingzc.azir.storage.Utf8KeyOrder.compare(left, right);
    }

    /**
     * Execute commit indent cleanup in shard budget.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     */
    private void cleanupParticipantIntents(final Transaction tx, final String shardId) {
        withShardPermit(shardId, () -> {
            adapter(shardId).cleanupIntents(tx.txId());
            return null;
        });
    }

    /**
     * Visible preparation also uses the same shard concurrent budget and cannot bypass foreground flow limits.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     */
    private void prepareParticipant(final Transaction tx, final String shardId) {
        withShardPermit(shardId, () -> {
            ensureLeaseValid(tx);
            adapter(shardId).prepare(tx.txId());
            return null;
        });
    }

    /**
     * Determines whether the transaction stage indicator is currently enabled.
     *
     * @return the operation result
     */
    private boolean metricsEnabled() {
        return metrics != NoOpTransactionMetrics.INSTANCE;
    }

    /**
     * The default indicator is closed with a direct access to the snapshot and avoids the business entry allocation of metering packages.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private BeginReservation segmentBeginStage(final IsolationLevel isolationLevel) {
        if (!metricsEnabled()) {
            return segmentLedger.begin(isolationLevel, options.limits().timeoutMillis(), false);
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_BEGIN,
                () -> segmentLedger.begin(isolationLevel, options.limits().timeoutMillis(), false));
    }

    /**
     * The commit slot is obtained directly from the local durable segment when the default indicator closes.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private PublicationSegmentOwnerRegistry.Assignment segmentReserveStage(final Transaction tx) {
        if (!metricsEnabled()) {
            return segmentOwner.allocate(tx.startTs());
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_RESERVE,
                () -> segmentOwner.allocate(tx.startTs()));
    }

    /**
     * Center candidate for shard Physical commit and terminal state complement, respectively.
     *
     * @param tx transaction context
     * @param slot slot
     * @param participant participant
     */
    private void segmentClaimLocalStage(final Transaction tx, final PublicationSegmentOwnerRegistry.Assignment slot,
                                        final String participant) {
        if (!metricsEnabled()) {
            segmentOwner.claimLocal(slot, tx.txId(), tx.deadlineMillis(), participant);
            return;
        }
        recordMeasuredStage(TransactionStage.SEGMENT_LOCAL_CLAIM, () -> {
            segmentOwner.claimLocal(slot, tx.txId(), tx.deadlineMillis(), participant);
            return null;
        });
    }

    /**
     * Success and anomaly recovery records the time-consuming transaction centre for local terminal proof.
     *
     * @param slot slot
     * @param local local
     * @param participant participant
     * @param committed committed
     */
    private void segmentCompleteLocalStage(final PublicationSegmentOwnerRegistry.Assignment slot,
                                           final ParticipantRecord local, final String participant, final boolean committed) {
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

    /**
     * When the default indicator is closed, the centre and slot decision atomic will be entered directly.
     *
     * @param tx transaction context
     * @param slot slot
     * @return the operation result
     */
    private long segmentDecisionStage(final Transaction tx, final PublicationSegmentOwnerRegistry.Assignment slot) {
        Supplier<Long> action = segmentCommitBatcher == null
                ? () -> segmentOwner.decide(slot, tx.txId(), tx.deadlineMillis(), tx.participants())
                : () -> segmentCommitBatcher.commit(slot, tx.txId(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_DECISION, action);
    }

    /**
     * The single slot centre decision successfully transfers a non-false commit receipt to the sync materialization phase.
     *
     * @param tx transaction context
     * @param slot slot
     * @return the operation result
     */
    private CommittedProof segmentDecisionProofStage(final Transaction tx,
                                                     final PublicationSegmentOwnerRegistry.Assignment slot) {
        Supplier<CommittedProof> action = () -> segmentOwner.decideWithProof(slot, tx.txId(),
                tx.startTs(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.SEGMENT_DECISION, action);
    }

    /**
     * Returns the cumulative group form of an optional subparagraph batch decision.
     *
     * @return the operation result
     */
    PublicationSegmentCommitBatcher.Stats segmentBatchStats() {
        return segmentCommitBatcher == null ? null : segmentCommitBatcher.stats();
    }

    /**
     * By default, wait directly for durable to release watermark overwrite transaction timestamp when the indicator closes.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     */
    private void segmentAwaitStage(final Transaction tx, final long commitTs) {
        if (!metricsEnabled()) {
            awaitSegmentPublication(tx, commitTs);
            return;
        }
        recordMeasuredStage(TransactionStage.SEGMENT_AWAIT,
                () -> awaitSegmentPublication(tx, commitTs));
    }

    /**
     * Durable storage to get the new protocol directly under no-op indicator Start snapshot.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     */
    private BeginReservation ledgerBeginStage(final IsolationLevel isolationLevel) {
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

    /**
     * Returns the cumulative batch size of an experimental batch start.
     *
     * @return the operation result
     */
    PublicationBeginBatcher.Stats ledgerBeginBatchStats() {
        return beginBatcher == null ? null : beginBatcher.stats();
    }

    /**
     * Direct preset durable storage commit timestamp under no-op indicator.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private long ledgerReserveStage(final Transaction tx) {
        if (!metricsEnabled()) {
            return publicationLedger.reserveCommit(tx.txId(), tx.deadlineMillis());
        }
        return recordMeasuredStage(TransactionStage.LEDGER_RESERVE,
                () -> publicationLedger.reserveCommit(tx.txId(), tx.deadlineMillis()));
    }

    /**
     * Single shard candidate time and centre PREPARING status equal to account slot atomic reservation.
     *
     * @param tx transaction context
     * @param participant participant
     * @return the operation result
     */
    private long ledgerLocalReserveStage(final Transaction tx, final String participant) {
        Supplier<Long> action = localReserveBatcher == null
                ? () -> publicationLedger.reserveLocalCommit(tx.txId(), tx.deadlineMillis(), participant)
                : () -> localReserveBatcher.reserve(tx.txId(), tx.deadlineMillis(), participant);
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_RESERVE, action);
    }

    /**
     * Returns an experimental local preset cumulative aggregate form for batch.
     *
     * @return the operation result
     */
    PublicationLocalReserveBatcher.Stats ledgerLocalReserveStats() {
        return localReserveBatcher == null ? null : localReserveBatcher.stats();
    }

    /**
     * Returns the cumulative aggregate polymer form of experimental local plus decision batch.
     *
     * @return the operation result
     */
    PublicationLocalCompletionBatcher.Stats ledgerLocalCompletionStats() {
        return localCompletionBatcher == null ? null : localCompletionBatcher.stats();
    }

    /**
     * Local Completes the centre with the same decision as the book after commit proof has satisfied the candidate time.
     *
     * @param local local
     * @param commitTs durable commit timestamp
     * @param participant participant
     * @return the operation result
     */
    private boolean ledgerLocalDecisionStage(final ParticipantRecord local, final long commitTs, final String participant) {
        Supplier<Boolean> action = localCompletionBatcher == null
                ? () -> publicationLedger.completeLocalCommitAfterFencing(local, commitTs, participant)
                : () -> localCompletionBatcher.complete(local, commitTs, participant);
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_DECISION, action);
    }

    /**
     * Atomic decision under no-op indicator directly from durable storage centre and account book.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     */
    private void ledgerDecisionStage(final Transaction tx, final long commitTs) {
        if (!metricsEnabled()) {
            publicationLedger.decideCommitted(tx.txId(), commitTs, tx.participants());
            return;
        }
        recordMeasuredStage(TransactionStage.LEDGER_DECISION,
                () -> publicationLedger.decideCommitted(tx.txId(), commitTs, tx.participants()));
    }

    /**
     * Inent write set allocates timestamp and commit decision in a control transaction at durable storage.
     *
     * @param tx transaction context
     * @return the operation result
     */
    private PublicationCommitResult ledgerAtomicCommitStage(final Transaction tx) {
        Supplier<PublicationCommitResult> action = publicationCommitBatcher == null
                ? () -> publicationLedger.commitPreparedIntents(tx.txId(), tx.deadlineMillis(), tx.participants())
                : () -> publicationCommitBatcher.commit(tx.txId(), tx.deadlineMillis(), tx.participants());
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.LEDGER_ATOMIC_COMMIT, action);
    }

    /**
     * Returns the cumulative form of an optional book batch decision, which is not accounted for in the non- batch mode.
     *
     * @return the operation result
     */
    PublicationCommitBatcher.Stats ledgerBatchStats() {
        return publicationCommitBatcher == null ? null : publicationCommitBatcher.stats();
    }

    /**
     * Returns the time-consuming batch control transaction phase of the current account book example used by the transaction manager.
     *
     * @return the operation result
     */
    MysqlPublicationLedger.BatchTiming ledgerBatchTiming() {
        return publicationLedger == null ? null : publicationLedger.batchTiming();
    }

    /**
     * Directly advance the continuous watermark account book under no-op indicator.
     *
     * @return the operation result
     */
    private long ledgerPublishStage() {
        if (!metricsEnabled()) {
            return publicationLedger.advancePublished(256);
        }
        return recordMeasuredStage(TransactionStage.LEDGER_PUBLISH,
                () -> publicationLedger.advancePublished(256));
    }

    /**
     * No-op indicator directly waits for its own commit to be covered by global watermark.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     */
    private void ledgerAwaitStage(final Transaction tx, final long commitTs) {
        if (!metricsEnabled()) {
            publicationLedger.awaitPublished(tx.txId(), commitTs, options.limits().timeoutMillis());
            return;
        }
        recordMeasuredStage(TransactionStage.LEDGER_AWAIT,
                () -> publicationLedger.awaitPublished(tx.txId(), commitTs, options.limits().timeoutMillis()));
    }

    /**
     * Directly execute server endside under no-op indicator to avoid measurement of package distribution.
     *
     * @param action action
     * @return the operation result
     */
    private Optional<BeginReservation> acceleratedBeginStage(final Supplier<Optional<BeginReservation>> action) {
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.BEGIN_PUBLICATION, action);
    }

    /**
     * Directly execute the server commit decision under no-op indicator.
     *
     * @param action action
     * @return the operation result
     */
    private OptionalLong acceleratedCommitStage(final Supplier<OptionalLong> action) {
        if (!metricsEnabled()) {
            return action.get();
        }
        return recordMeasuredStage(TransactionStage.COMMIT_PUBLICATION, action);
    }

    /**
     * Record the timestamp distribution phase as required.
     *
     * @param stage stage
     * @return the operation result
     */
    private long nextTimestamp(final TransactionStage stage) {
        if (!metricsEnabled()) {
            return timestampOracle.nextTimestamp();
        }
        return recordMeasuredStage(stage, timestampOracle::nextTimestamp);
    }

    /**
     * Record the creation phase of begin metadata on demand.
     *
     * @param txId transaction identifier
     * @param startTs transaction start timestamp
     * @param isolationLevel global transaction isolation level
     */
    private void createBeginMetadata(final long txId, final long startTs, final IsolationLevel isolationLevel) {
        if (!metricsEnabled()) {
            metadataStore.create(txId, startTs, isolationLevel);
            return;
        }
        recordMeasuredStage(TransactionStage.BEGIN_METADATA, () -> metadataStore.create(txId, startTs, isolationLevel));
    }

    /**
     * The shard mettadata commit phase of the required loglist.
     *
     * @param tx transaction context
     * @param participant participant
     * @param commitTs durable commit timestamp
     */
    private void markSingleShardCommittedStage(final Transaction tx, final String participant, final long commitTs) {
        if (!metricsEnabled()) {
            metadataStore.persistSingleShardCommitted(tx.txId(), participant, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_METADATA_COMMIT,
                () -> metadataStore.persistSingleShardCommitted(tx.txId(), participant, commitTs));
    }

    /**
     * Record buffer writing phase prewrite as required.
     *
     * @param tx transaction context
     */
    private void prewriteBufferedMutationsStage(final Transaction tx) {
        if (!metricsEnabled()) {
            prewriteBufferedMutations(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.BUFFERED_PREWRITE, () -> prewriteBufferedMutations(tx));
    }

    /**
     * Recording non- buffer prewrite stages on demand.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param mutation mutation
     * @param adapter adapter
     */
    private void prewriteEagerMutationStage(final Transaction tx, final String shardId, final Mutation mutation, final StorageAdapter adapter) {
        if (!metricsEnabled()) {
            prewriteEagerMutation(tx, shardId, mutation, adapter);
            return;
        }
        recordMeasuredStage(TransactionStage.EAGER_PREWRITE, () -> prewriteEagerMutation(tx, shardId, mutation, adapter));
    }

    /**
     * Record the visible prepare phase as needed.
     *
     * @param tx transaction context
     */
    private void prepareParticipantsStage(final Transaction tx) {
        if (!metricsEnabled()) {
            runAcrossParticipants(
                    tx.participants(),
                    participant -> prepareParticipant(tx, participant),
                    options.crossShardCommitPolicy().parallelPrepare());
            return;
        }
        recordMeasuredStage(
                TransactionStage.PREPARE,
                () -> runAcrossParticipants(
                        tx.participants(),
                        participant -> prepareParticipant(tx, participant),
                        options.crossShardCommitPolicy().parallelPrepare()));
    }

    /**
     * Record cross-shard metadata commit phase as required.
     *
     * @param tx transaction context
     * @param commitTs durable commit timestamp
     * @param explicitPrepareRequired explicit prepare required
     */
    private void persistCommittedMetadataStage(final Transaction tx, final long commitTs, final boolean explicitPrepareRequired) {
        if (!metricsEnabled()) {
            persistCommittedMetadata(tx, commitTs, explicitPrepareRequired);
            return;
        }
        recordMeasuredStage(
                TransactionStage.CROSS_SHARD_METADATA_COMMIT,
                () -> persistCommittedMetadata(tx, commitTs, explicitPrepareRequired));
    }

    /**
     * Record the participant commit phase as required.
     *
     * @param stage stage
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param commitTs durable commit timestamp
     */
    private void commitParticipantStage(final TransactionStage stage, final Transaction tx, final String shardId, final long commitTs) {
        if (!metricsEnabled()) {
            commitParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(stage, () -> commitParticipant(tx, shardId, commitTs));
    }

    /**
     * Center confirmed durable decision receipt, shard still has validate complete intent and local terminal state.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param proof proof
     */
    private void commitDecidedParticipantStage(final Transaction tx, final String shardId, final CommittedProof proof) {
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
     * Record the shard direct commit phase as required.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     * @param commitTs durable commit timestamp
     */
    private void directCommitSingleShardParticipantStage(final Transaction tx, final String shardId, final long commitTs) {
        if (!metricsEnabled()) {
            directCommitSingleShardParticipant(tx, shardId, commitTs);
            return;
        }
        recordMeasuredStage(
                TransactionStage.SINGLE_SHARD_PARTICIPANT_COMMIT,
                () -> directCommitSingleShardParticipant(tx, shardId, commitTs));
    }

    /**
     * Record the rollback phase as required.
     *
     * @param tx transaction context
     */
    private void rollbackStage(final Transaction tx) {
        if (!metricsEnabled()) {
            rollbackInternal(tx);
            return;
        }
        recordMeasuredStage(TransactionStage.ROLLBACK, () -> rollbackInternal(tx));
    }

    /**
     * Execute the actual action for transaction rollback.
     *
     * @param tx transaction context
     */
    private void rollbackInternal(final Transaction tx) {
        TxStatus decision = metadataStore.withPublicationLock(() -> {
            if (!tx.metadataPersisted()) {
                return TxStatus.ABORTED;
            }
            TxRecord record = metadataStore.get(tx.txId()).orElseThrow(() -> unknownCommit(tx, new IllegalStateException("transaction metadata missing")));
            if (record.status() == TxStatus.COMMITTED) {
                return TxStatus.COMMITTED;
            }
            if (tx.decisionStarted() && tx.participants().size() == 1
                    && adapter(tx.participants().iterator().next()).supportsAtomicDirectCommit()) {
                String shardId = tx.participants().iterator().next();
                // First isolate the local commit that may be late on the participant side, then update the centre based on actual local terminal state.
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
            // The markAborted returned terminal state is authority and cannot delete incent according to the status read before call.
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
                options.crossShardCommitPolicy().parallelRollback());
        markFinalizedQuietly(tx);
    }

    /**
     * Record the intent cleanup phase as required.
     *
     * @param tx transaction context
     * @param shardId logical shard identifier
     */
    private void cleanupIntentsStage(final Transaction tx, final String shardId) {
        if (!metricsEnabled()) {
            cleanupParticipantIntents(tx, shardId);
            return;
        }
        recordMeasuredStage(TransactionStage.INTENT_CLEANUP, () -> cleanupParticipantIntents(tx, shardId));
    }

    /**
     * Records the results of the measurement phase of return values.
     *
     * @param <T> value type
     * @param stage stage
     * @param action action
     * @return the operation result
     */
    private <T> T recordMeasuredStage(final TransactionStage stage, final Supplier<T> action) {
        long startNanos = System.nanoTime();
        try {
            T result = action.get();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
            return result;
        } catch (final RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * The results of the measurement phase, which has no return value, are recorded.
     *
     * @param stage stage
     * @param action action
     */
    private void recordMeasuredStage(final TransactionStage stage, final Runnable action) {
        long startNanos = System.nanoTime();
        try {
            action.run();
            recordMetricSuccess(stage, System.nanoTime() - startNanos);
        } catch (final RuntimeException | Error ex) {
            recordMetricFailure(stage, System.nanoTime() - startNanos);
            throw ex;
        }
    }

    /**
     * Every effort is made to document success indicators and avoid the achievement of an abnormal reverse impact in transaction semantics.
     *
     * @param stage stage
     * @param elapsedNanos elapsed nanos
     */
    private void recordMetricSuccess(final TransactionStage stage, final long elapsedNanos) {
        try {
            metrics.recordSuccess(stage, elapsedNanos);
        } catch (final RuntimeException | Error ignored) {
        }
    }

    /**
     * Every effort is made to record the failure indicators and avoid covering up the original business anomalies.
     *
     * @param stage stage
     * @param elapsedNanos elapsed nanos
     */
    private void recordMetricFailure(final TransactionStage stage, final long elapsedNanos) {
        try {
            metrics.recordFailure(stage, elapsedNanos);
        } catch (final RuntimeException | Error ignored) {
        }
    }
}
