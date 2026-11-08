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

package com.flyingzc.azir.backend;

import com.flyingzc.azir.common.IsolationLevel;

/**
 * Independent source code transaction backend SPI; achieves possession of its own decision authority and storage space.
 */
public interface TransactionBackend extends AutoCloseable {

    /**
     * Returns the name of the backend.
     *
     * @return the operation result
     */
    String implementation();

    /**
     * Returns immutable data space identity.
     *
     * @return the operation result
     */
    String namespace();

    /**
     * Creates the global -Segregated Level transaction.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    BackendTransaction begin(IsolationLevel isolation);

    /**
     * Press original identity for durable status, missing does not mean aborted.
     *
     * @param id id
     * @return the operation result
     */
    BackendDecision lookup(BackendId id);

    /**
     * Bounded recovery durable terminal state or expired transaction, does not perform unproven physics version GC.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    int recover(int limit);

    /**
     * Execute round recovery and secure version cleanup; extension of GC is not supported only recovery.
     *
     * @param limit maximum number of items
     * @return the operation result
     */
    default com.flyingzc.azir.maintenance.MaintenanceResult maintain(final int limit) {
        return new com.flyingzc.azir.maintenance.MaintenanceResult(recover(limit), 0, 0);
    }

    /**
     * Durable snapshot protection and maintenance backlog; extension not provided for diagnosis must be explicitly rejected.
     *
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default com.flyingzc.azir.maintenance.MaintenanceDiagnostics diagnostics() {
        throw new UnsupportedOperationException("maintenance diagnostics unavailable");
    }

    /**
     * Returning to a visible production management capability, the unsupported backend cannot be downgraded silently.
     *
     * @return the operation result
     * @throws UnsupportedOperationException if the operation cannot complete
     */
    default com.flyingzc.azir.operations.MysqlProtocolAdmin administration() {
        throw new UnsupportedOperationException("administration unavailable");
    }

    /**
     * Release your own resource from the back end without closing connection pool borrowed by the caller.
     */
    @Override
    void close();
}
