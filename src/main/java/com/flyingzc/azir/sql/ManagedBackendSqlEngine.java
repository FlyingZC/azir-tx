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

/**
 * SQL assembly handles with a source backend; the caller must close session before closing this handle.
 */
public final class ManagedBackendSqlEngine implements SqlEngine, AutoCloseable {

    private final TransactionBackend backend;

    private final BackendSqlEngine engine;

    private final com.flyingzc.azir.maintenance.MaintenanceService maintenance;

    private volatile boolean closed;

    private final java.util.concurrent.locks.ReentrantLock lifecycleLock = new java.util.concurrent.locks.ReentrantLock();

    private boolean backendClosed;

    /**
     * Take over created backend and SQL components are still re-used as public implementers.
     *
     * @param backend backend
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     */
    public ManagedBackendSqlEngine(final TransactionBackend backend, final ShardResolver resolver, final SqlValueCodec codec,
                                   final IsolationLevel isolation, final SqlExecutionOptions options) {
        this(backend, resolver, codec, isolation, options, com.flyingzc.azir.maintenance.MaintenanceOptions.defaults());
    }

    /**
     * Visible configuration of background recovery and GC cycles and budget, when constructed, to run automatically.
     *
     * @param backend backend
     * @param resolver resolver
     * @param codec codec
     * @param isolation global transaction isolation level
     * @param options options
     * @param maintenanceOptions maintenance options
     */
    public ManagedBackendSqlEngine(final TransactionBackend backend, final ShardResolver resolver, final SqlValueCodec codec,
                                   final IsolationLevel isolation, final SqlExecutionOptions options,
                                   final com.flyingzc.azir.maintenance.MaintenanceOptions maintenanceOptions) {
        this.backend = java.util.Objects.requireNonNull(backend, "backend");
        try {
            engine = new BackendSqlEngine(backend, resolver, codec, isolation, options);
            maintenance = new com.flyingzc.azir.maintenance.MaintenanceService(backend.implementation(), maintenanceOptions, backend::maintain);
        } catch (final RuntimeException | Error failure) {
            try {
                backend.close();
            } catch (final RuntimeException | Error cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    /**
     * Get a uniform original identity query and bounded recovery entrance.
     *
     * @return the operation result
     */
    public TransactionBackend backend() {
        return backend;
    }

    /**
     * Exposure to background success, failure, safe point and latest error for health screening.
     *
     * @return the operation result
     */
    public com.flyingzc.azir.maintenance.MaintenanceService.Health maintenanceHealth() {
        return maintenance.health();
    }

    /**
     * Do not create new business session after the backend closes.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    @Override
    public SqlSession openSession() {
        if (closed) {
            throw new IllegalStateException("backend SQL engine closed");
        }
        lifecycleLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("backend SQL engine closed");
            }
            return engine.openSession();
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Close the entrance first, with a visible lock to avoid fixing the Java 21 virtual thread carrier while waiting for background; timeout will still allow retry to close after timeout.
     */
    @Override
    public void close() {
        closed = true;
        lifecycleLock.lock();
        try {
            if (backendClosed) {
                return;
            }
            maintenance.close();
            backend.close();
            backendClosed = true;
        } finally {
            lifecycleLock.unlock();
        }
    }
}
