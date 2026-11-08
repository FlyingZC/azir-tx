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

package com.flyingzc.azir.control;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Initialization lock before durable namespace is created to prevent the initial start-up of concurrent to reset TSO or bind different identity.
 */
public final class MysqlBootstrapLock implements AutoCloseable {

    private final Connection connection;

    private final String name;

    private final MysqlNamedLock.Lease lease;

    /**
     * The physical control library is bound and initialized with a limited waiting period, without the commit doorbar.
     *
     * @param source database connection source
     * @param protocol protocol
     * @throws IllegalStateException if the operation cannot complete
     */
    public MysqlBootstrapLock(final DataSource source, final String protocol) {
        name = "azir:init:" + protocol + ":" + UUID.nameUUIDFromBytes(MysqlDatabaseAuthority.inspect(source).toString().getBytes(StandardCharsets.UTF_8));
        Connection acquired = null;
        try {
            acquired = source.getConnection();
            lease = MysqlNamedLock.lock(acquired, name, 10);
            connection = acquired;
        } catch (final SQLException | RuntimeException | Error failure) {
            if (acquired != null) {
                MysqlNamedLock.discard(acquired, failure);
                try {
                    acquired.close();
                } catch (final SQLException close) {
                    failure.addSuppressed(close);
                }
            }
            if (failure instanceof SQLException sql) {
                throw new IllegalStateException("cannot serialize protocol initialization", sql);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw (Error) failure;
        }
    }

    /**
     * Unlocking failed to destroy the physical connection and avoid initializing locks on the session residual in the pool.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public void close() {
        Throwable problem = null;
        try {
            lease.close();
        } catch (final SQLException | RuntimeException | Error failure) {
            problem = failure;
        } finally {
            try {
                connection.close();
            } catch (final SQLException | RuntimeException | Error close) {
                if (problem == null) {
                    problem = close;
                } else if (problem != close) {
                    problem.addSuppressed(close);
                }
            }
        }
        if (problem instanceof SQLException sql) {
            throw new IllegalStateException("protocol initialization unlock failed", sql);
        }
        if (problem instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (problem instanceof Error fatal) {
            throw fatal;
        }
    }
}
