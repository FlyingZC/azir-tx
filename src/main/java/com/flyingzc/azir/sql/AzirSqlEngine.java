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
import com.flyingzc.azir.txn.TransactionManager;

import java.util.Objects;

/**
 * The Azir SQL engine, responsible for creating session components that bind the transaction manager and SQL components.
 */
public final class AzirSqlEngine implements SqlEngine {

    private final TransactionManager transactionManager;

    private final SqlPlanner planner;

    private final ShardResolver shardResolver;

    private final SqlValueCodec valueCodec;

    private final IsolationLevel defaultIsolationLevel;

    private final SqlStorageKeyCodec keyCodec;

    private final SqlExecutionOptions executionOptions;

    /**
     * Construct the Azir SQL engine.
     *
     * @param transactionManager transaction manager
     * @param planner planner
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param defaultIsolationLevel default isolation level
     */
    public AzirSqlEngine(final TransactionManager transactionManager,
                         final SqlPlanner planner,
                         final ShardResolver shardResolver,
                         final SqlValueCodec valueCodec,
                         final IsolationLevel defaultIsolationLevel) {
        this(transactionManager, planner, shardResolver, valueCodec, defaultIsolationLevel, SqlExecutionOptions.defaults());
    }

    /**
     * Construct an engine that can configure page size and result budget, save the planner to insert interface.
     *
     * @param transactionManager transaction manager
     * @param planner planner
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param defaultIsolationLevel default isolation level
     * @param executionOptions execution options
     */
    public AzirSqlEngine(final TransactionManager transactionManager,
                         final SqlPlanner planner,
                         final ShardResolver shardResolver,
                         final SqlValueCodec valueCodec,
                         final IsolationLevel defaultIsolationLevel,
                         final SqlExecutionOptions executionOptions) {
        this.transactionManager = transactionManager;
        this.planner = planner;
        this.shardResolver = shardResolver;
        this.valueCodec = valueCodec;
        this.defaultIsolationLevel = defaultIsolationLevel;
        this.keyCodec = new SqlStorageKeyCodec();
        this.executionOptions = Objects.requireNonNull(executionOptions, "executionOptions");
    }

    /**
     * Creates the Azir SQL engine using Calcite Schemer.
     *
     * @param transactionManager transaction manager
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param defaultIsolationLevel default isolation level
     * @return the operation result
     */
    public static AzirSqlEngine calcite(final TransactionManager transactionManager,
                                        final ShardResolver shardResolver,
                                        final SqlValueCodec valueCodec,
                                        final IsolationLevel defaultIsolationLevel) {
        return calcite(transactionManager, shardResolver, valueCodec, defaultIsolationLevel, SqlExecutionOptions.defaults());
    }

    /**
     * Creates the Calcite engine that configures page size and result budget.
     *
     * @param transactionManager transaction manager
     * @param shardResolver shard resolver
     * @param valueCodec value codec
     * @param defaultIsolationLevel default isolation level
     * @param executionOptions execution options
     * @return the operation result
     */
    public static AzirSqlEngine calcite(final TransactionManager transactionManager,
                                        final ShardResolver shardResolver,
                                        final SqlValueCodec valueCodec,
                                        final IsolationLevel defaultIsolationLevel,
                                        final SqlExecutionOptions executionOptions) {
        return new AzirSqlEngine(
                transactionManager,
                new CalciteSqlPlanner(),
                shardResolver,
                valueCodec,
                defaultIsolationLevel,
                executionOptions);
    }

    /**
     * Creates new SQL session.
     *
     * @return the operation result
     */
    @Override
    public SqlSession openSession() {
        return new AzirSqlSession(
                transactionManager,
                planner,
                shardResolver,
                valueCodec,
                keyCodec,
                defaultIsolationLevel,
                executionOptions);
    }
}
