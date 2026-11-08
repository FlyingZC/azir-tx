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

package com.flyingzc.azir.sql;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.percolator.MysqlPercolatorStore;
import com.flyingzc.azir.percolator.MysqlPercolatorLifecycle;
import com.flyingzc.azir.percolator.PercolatorKey;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.percolator.PercolatorStore;
import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Explicit SQL entry point for the project's MySQL Percolator runtime, sharing the Calcite planner and executor.
 */
public final class PercolatorSqlEngine implements SqlEngine, AutoCloseable {

    private final PercolatorRuntime runtime;

    private final SqlPlanner planner;

    private final ShardResolver resolver;

    private final SqlValueCodec codec;

    private final IsolationLevel isolation;

    private final SqlExecutionOptions options;

    private com.flyingzc.azir.maintenance.MaintenanceService maintenance;

    private boolean ownsRuntime;

    private volatile boolean closed;

    private final java.util.concurrent.locks.ReentrantLock lifecycleLock = new java.util.concurrent.locks.ReentrantLock();

    /**
     * The lightweight master key plan, Calcite relationship plan and SQL resource budget can be configured when using the existing run.
     *
     * @param runtime runtime
     * @param planner planner
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     */
    public PercolatorSqlEngine(final PercolatorRuntime runtime, final SqlPlanner planner, final ShardResolver resolver,
                               final SqlValueCodec codec, final IsolationLevel isolation, final SqlExecutionOptions options) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.planner = Objects.requireNonNull(planner, "planner");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.isolation = Objects.requireNonNull(isolation, "isolation");
        this.options = Objects.requireNonNull(options, "options");
    }

    /**
     * Select the entrance with a visible backend symmetrical AzirSqlEngine.calcite.
     *
     * @param runtime runtime
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public static PercolatorSqlEngine calcite(final PercolatorRuntime runtime, final ShardResolver resolver,
                                              final SqlValueCodec codec, final IsolationLevel isolation) {
        return new PercolatorSqlEngine(runtime, new CalciteSqlPlanner(), resolver, codec, isolation,
                SqlExecutionOptions.defaults());
    }

    /**
     * Simple primary key statement takes a light schedule, and the rest of SQL still uses the same complete Calcite implementer.
     *
     * @param runtime runtime
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public static PercolatorSqlEngine hybrid(final PercolatorRuntime runtime, final ShardResolver resolver,
                                             final SqlValueCodec codec, final IsolationLevel isolation) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), resolver, codec, isolation,
                SqlExecutionOptions.defaults());
    }

    /**
     * Use the connection pool configuration of MySQL Percolator, which is owned by the caller. All examples must be shared in the same durable TSO library.
     *
     * @param timestampControl timestamp control
     * @param shardSources shard sources
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public static PercolatorSqlEngine mysql(final DataSource timestampControl, final Map<String, DataSource> shardSources,
                                            final ShardResolver resolver, final SqlValueCodec codec, final IsolationLevel isolation) {
        return mysql(timestampControl, shardSources, resolver, codec, isolation,
                PercolatorRuntime.Options.defaults(), SqlExecutionOptions.defaults());
    }

    /**
     * Validate Configure and Physical namespace Initialize protocol tables; unclear empty data, do not migrate old models or close borrowed pools.
     *
     * @param timestampControl timestamp control
     * @param shardSources shard sources
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param transactionOptions transaction options
     * @param sqlOptions sql options
     * @return the operation result
     */
    public static PercolatorSqlEngine mysql(final DataSource timestampControl, final Map<String, DataSource> shardSources,
                                            final ShardResolver resolver, final SqlValueCodec codec, final IsolationLevel isolation,
                                            final PercolatorRuntime.Options transactionOptions, final SqlExecutionOptions sqlOptions) {
        return mysql(timestampControl, shardSources, resolver, codec, isolation, transactionOptions, sqlOptions,
                com.flyingzc.azir.maintenance.MaintenanceOptions.defaults());
    }

    /**
     * Production portal allows the configuration of recovery /GC cycles and budget; callers must close before the pool to return handles.
     *
     * @param timestampControl timestamp control
     * @param shardSources shard sources
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param transactionOptions transaction options
     * @param sqlOptions sql options
     * @param maintenanceOptions maintenance options
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static PercolatorSqlEngine mysql(DataSource timestampControl, final Map<String, DataSource> shardSources,
                                            final ShardResolver resolver, final SqlValueCodec codec, final IsolationLevel isolation,
                                            final PercolatorRuntime.Options transactionOptions, final SqlExecutionOptions sqlOptions,
                                            final com.flyingzc.azir.maintenance.MaintenanceOptions maintenanceOptions) {
        Objects.requireNonNull(timestampControl, "timestampControl");
        Objects.requireNonNull(resolver, "resolver");
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(isolation, "isolation");
        Objects.requireNonNull(transactionOptions, "transactionOptions");
        Objects.requireNonNull(sqlOptions, "sqlOptions");
        timestampControl = new com.flyingzc.azir.control.ProtocolMysqlDataSource(timestampControl);
        var validated = new LinkedHashMap<String, DataSource>();
        shardSources.forEach((id, source) -> validated.put(id, new com.flyingzc.azir.control.ProtocolMysqlDataSource(source)));
        Map<String, DataSource> sources = Map.copyOf(validated);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("shards must not be empty");
        }
        requireAutoCommit(timestampControl);
        var identities = new HashSet<MysqlDatabaseAuthority.Identity>();
        Map<String, PercolatorStore> stores = new LinkedHashMap<>();
        sources.forEach((id, source) -> {
            new PercolatorKey(id, "");
            requireAutoCommit(source);
            if (!identities.add(MysqlDatabaseAuthority.inspect(source))) {
                throw new IllegalArgumentException("Percolator shards must use distinct physical databases");
            }
            stores.put(id, new MysqlPercolatorStore(id, source));
        });
        var lifecycle = new MysqlPercolatorLifecycle(timestampControl, sources, transactionOptions.lockTtlMillis());
        try {
            sources.forEach((id, source) -> stores.put(id, new MysqlPercolatorStore(id, source, lifecycle.operations())));
            stores.values().forEach(PercolatorStore::initialize);
            com.flyingzc.azir.operations.MysqlProtocolSchema.requireRole(timestampControl, "pc", "control");
            sources.forEach((id, source) -> com.flyingzc.azir.operations.MysqlProtocolSchema.requireRole(source, "pc", "shard:" + id));
            var runtime = new PercolatorRuntime(new MysqlTimestampOracle(timestampControl), stores, transactionOptions, lifecycle);
            var engine = hybrid(runtime, resolver, codec, isolation, sqlOptions);
            engine.ownsRuntime = true;
            engine.maintenance = new com.flyingzc.azir.maintenance.MaintenanceService("percolator", maintenanceOptions, runtime::maintain);
            return engine;
        } catch (final RuntimeException | Error failure) {
            lifecycle.close();
            throw failure;
        }
    }

    /**
     * Use connection pool must default autocommit to avoid initialization or TSO timestamp allocation quietly relying on local transaction without commit.
     *
     * @param source database connection source
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    private static void requireAutoCommit(final DataSource source) {
        try (var connection = source.getConnection()) {
            if (!connection.getAutoCommit()) {
                throw new IllegalArgumentException("Percolator requires an auto-commit connection pool");
            }
        } catch (final SQLException failure) {
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "inspect Percolator pool failed", false, null, failure);
        }
    }

    /**
     * The assembly plant sets up SQL budget to maintain the same back end of the light primary key and complete relationship plan.
     *
     * @param runtime runtime
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     * @return the operation result
     */
    private static PercolatorSqlEngine hybrid(final PercolatorRuntime runtime, final ShardResolver resolver,
                                              final SqlValueCodec codec, final IsolationLevel isolation, final SqlExecutionOptions options) {
        return new PercolatorSqlEngine(runtime, new HybridSqlPlanner(), resolver, codec, isolation, options);
    }

    /**
     * Returns the KV, the original Primary Query and bounded recovery entry to the same backend.
     *
     * @return the operation result
     */
    public PercolatorRuntime runtime() {
        return runtime;
    }

    /**
     * Each caller acquires an independent SQL transaction session, without prejudice to the original default backend.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public SqlSession openSession() {
        if (closed) {
            throw new IllegalStateException("Percolator SQL engine closed");
        }
        lifecycleLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Percolator SQL engine closed");
            }
            return new PercolatorSqlSession(runtime, planner, resolver, codec, isolation, options);
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Returns the automatic state of health; there is no automated scheduler for a custom running engine.
     *
     * @return the operation result
     */
    public java.util.Optional<com.flyingzc.azir.maintenance.MaintenanceService.Health> maintenanceHealth() {
        return java.util.Optional.ofNullable(maintenance).map(com.flyingzc.azir.maintenance.MaintenanceService::health);
    }

    /**
     * Stop and wait for background maintenance to close the plant-owned running time; close timeout allows later retry.
     */
    @Override
    public void close() {
        closed = true;
        lifecycleLock.lock();
        try {
            if (maintenance != null) {
                maintenance.close();
            }
            if (ownsRuntime) {
                runtime.close();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }
}
