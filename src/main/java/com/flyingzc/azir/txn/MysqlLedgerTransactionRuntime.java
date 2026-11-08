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

/**
 * Visible assembly book transaction entrance and independent connection budget background recovery, GC life cycle.
 */
public final class MysqlLedgerTransactionRuntime implements AutoCloseable {

    private final TransactionManager transactionManager;

    private final PublicationMaintenanceScheduler maintenanceScheduler;

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Saves the start-up transaction entry and background tasks.
     *
     * @param transactionManager transaction manager
     * @param maintenanceScheduler maintenance scheduler
     */
    private MysqlLedgerTransactionRuntime(final TransactionManager transactionManager,
                                          final PublicationMaintenanceScheduler maintenanceScheduler) {
        this.transactionManager = transactionManager;
        this.maintenanceScheduler = maintenanceScheduler;
    }

    /**
     * When loading database on a activated account book is running; the data source is held by the caller, and the background pool must be independent and point to the same database.
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
    public static MysqlLedgerTransactionRuntime open(final DataSource foregroundControl,
                                                     final DataSource backgroundControl,
                                                     final Map<String, DataSource> foregroundShards,
                                                     final Map<String, DataSource> backgroundShards,
                                                     final TransactionOptions transactionOptions,
                                                     final LedgerRuntimeOptions runtimeOptions,
                                                     final TransactionMetrics metrics) {
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
        } catch (final RuntimeException | Error ex) {
            if (scheduler != null) {
                try {
                    scheduler.close();
                } catch (final RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            if (manager != null) {
                try {
                    manager.close();
                } catch (final RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            if (manager == null && registry != null) {
                try {
                    registry.close();
                } catch (final RuntimeException closeFailure) {
                    ex.addSuppressed(closeFailure);
                }
            }
            throw ex;
        }
    }

    /**
     * Returns the transaction manager of the RC/RR account book protocol that is enabled.
     *
     * @return the operation result
     */
    public TransactionManager transactionManager() {
        return transactionManager;
    }

    /**
     * Returns recovery and GC periodic statistics with recent failures.
     *
     * @return the operation result
     */
    public PublicationMaintenanceScheduler maintenanceScheduler() {
        return maintenanceScheduler;
    }

    /**
     * Stop the background task and then close the foreground transaction implementer; data sources are closed by the creator.
     */
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

    /**
     * Compares the server UUID with the selected database, rejecting an error receipt of the background pool.
     *
     * @param foreground foreground
     * @param background background
     * @param route route
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireSameAuthority(final DataSource foreground, final DataSource background, final String route) {
        if (!authority(foreground).equals(authority(background))) {
            throw new IllegalArgumentException("background database authority differs from foreground: " + route);
        }
    }

    /**
     * Reads a MySQL server and database that connection actually belongs to.
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
                        "ledger runtime datasource requires a selected MySQL database", false, null);
            }
            return row.getString(1) + "\u0000" + row.getString(2);
        } catch (final SQLException ex) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "inspect ledger runtime database authority failed", false, null, ex);
        }
    }
}
