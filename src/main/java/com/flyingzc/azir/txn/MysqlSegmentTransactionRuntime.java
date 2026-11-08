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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.BackgroundTaskStats;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.mysql.MysqlTransactionMetadataStore;
import com.flyingzc.azir.metrics.TransactionMetrics;
import com.flyingzc.azir.publication.SegmentPublicationGcManager;
import com.flyingzc.azir.publication.SegmentPublicationMaintenanceScheduler;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryManager;
import com.flyingzc.azir.publication.SegmentPublicationRecoveryRunner;
import com.flyingzc.azir.publication.mysql.MysqlSegmentPublicationLedger;
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

/**
 * Visible transaction entry and independent background owner, recovery, release and GC life cycle of the sub-agreement activated.
 */
public final class MysqlSegmentTransactionRuntime implements AutoCloseable {

    private final TransactionManager transactionManager;

    private final SegmentPublicationMaintenanceScheduler scheduler;

    private final PublicationSegmentOwnerRegistry owner;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Saves a start-up foreground manager and an background job.
     *
     * @param transactionManager transaction manager
     * @param scheduler scheduler
     * @param owner owner
     */
    private MysqlSegmentTransactionRuntime(final TransactionManager transactionManager,
                                           final SegmentPublicationMaintenanceScheduler scheduler,
                                           final PublicationSegmentOwnerRegistry owner) {
        this.transactionManager = transactionManager;
        this.scheduler = scheduler;
        this.owner = owner;
    }

    /**
     * Starts only when the durable segment mode has been switched and before background connection pool is independent with authority.
     *
     * @param foregroundControl foreground control
     * @param backgroundControl background control
     * @param foregroundShards foreground shards
     * @param backgroundShards background shards
     * @param transactionOptions transaction options
     * @param runtimeOptions runtime options
     * @param metrics metrics
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static MysqlSegmentTransactionRuntime open(final DataSource foregroundControl,
                                                      final DataSource backgroundControl,
                                                      final Map<String, DataSource> foregroundShards,
                                                      final Map<String, DataSource> backgroundShards,
                                                      final TransactionOptions transactionOptions,
                                                      final SegmentRuntimeOptions runtimeOptions,
                                                      final TransactionMetrics metrics) {
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
        } catch (final RuntimeException | Error ex) {
            if (manager != null) {
                try {
                    manager.close();
                } catch (final RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            } else {
                if (batcher != null) {
                    try {
                        batcher.close(transactionOptions.limits().shutdownTimeoutMillis());
                    } catch (final RuntimeException closeFailure) {
                        ex.addSuppressed(closeFailure);
                    }
                }
                if (owner != null) {
                    try {
                        owner.close();
                    } catch (final RuntimeException closeFailure) {
                        ex.addSuppressed(closeFailure);
                    }
                }
            }
            if (scheduler != null) {
                try {
                    scheduler.close();
                } catch (final RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            throw ex;
        }
    }

    /**
     * Returns the RC/RR transaction manager of the visible segment mode.
     *
     * @return the operation result
     */
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    /**
     * Returns background recovery, publishes running statistics with GC.
     *
     * @return the operation result
     */
    public SegmentPublicationMaintenanceScheduler maintenanceScheduler() {
        return scheduler;
    }

    /**
     * Returns the count of the duration of the own lease and the number of times the wheel is actively sealed.
     *
     * @return the operation result
     */
    public BackgroundTaskStats ownerRefreshStats() {
        return owner.refreshStats();
    }

    /**
     * Returns the most recent leaser renewal failure.
     *
     * @return the operation result
     */
    public RuntimeException lastOwnerRefreshFailure() {
        return owner.lastRefreshFailure();
    }

    /**
     * Stop foreground transaction and split owner first, then release and recovery; data sources are closed from the caller.
     */
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

    /**
     * Two validate sets of connection pool actually landed on the same MySQL server UUID and database.
     *
     * @param foreground foreground
     * @param background background
     * @param route route
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireSameAuthority(final DataSource foreground, final DataSource background, final String route) {
        if (!authority(foreground).equals(authority(background))) {
            throw new IllegalArgumentException("segmented background authority differs from foreground: " + route);
        }
    }

    /**
     * Look for connection actual database identity and do not believe only the route names provided by JDBC URL.
     *
     * @param dataSource database connection source
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    private static String authority(final DataSource dataSource) {
        try (
                Connection connection = dataSource.getConnection();
                Statement query = connection.createStatement();
                ResultSet row = query.executeQuery("select @@server_uuid, database()")) {
            if (!row.next() || row.getString(1) == null || row.getString(2) == null) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "segmented runtime requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect segmented runtime database authority failed", false, null, ex);
        }
    }
}
