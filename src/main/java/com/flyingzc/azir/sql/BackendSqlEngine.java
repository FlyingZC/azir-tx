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

import com.flyingzc.azir.backend.TransactionBackend;
import com.flyingzc.azir.common.IsolationLevel;
import java.util.Objects;

/**
 * SQL adapter for public transaction SPI, with the source backend not dependent on Calcite packages.
 */
public final class BackendSqlEngine implements SqlEngine {

    private final TransactionBackend backend;

    private final ShardResolver resolver;

    private final SqlValueCodec codec;

    private final IsolationLevel isolation;

    private final SqlExecutionOptions options;

    /**
     * Fits the back end and SQL route and budget, without taking over the life cycle of the back end.
     *
     * @param backend backend
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     */
    public BackendSqlEngine(final TransactionBackend backend, final ShardResolver resolver, final SqlValueCodec codec,
                            final IsolationLevel isolation, final SqlExecutionOptions options) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.isolation = Objects.requireNonNull(isolation, "isolation");
        this.options = Objects.requireNonNull(options, "options");
    }

    /**
     * Creates a default SQL engine for budget, primary key and full relationship plans to share the transaction kernel.
     *
     * @param backend backend
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public static BackendSqlEngine calcite(final TransactionBackend backend, final ShardResolver resolver,
                                           final SqlValueCodec codec, final IsolationLevel isolation) {
        return new BackendSqlEngine(backend, resolver, codec, isolation, SqlExecutionOptions.defaults());
    }

    /**
     * Creates a stand-alone business session, does not hold session variable SQL transaction.
     *
     * @return the operation result
     */
    @Override
    public SqlSession openSession() {
        return new BackendSqlSession(backend, new HybridSqlPlanner(), resolver, codec, isolation, options);
    }
}
