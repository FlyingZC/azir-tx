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

/**
 * Transaction kernel session, SQL does not explain the middle state of the original protocol.
 */
public interface BackendTransaction extends AutoCloseable {

    /**
     * Returns full transaction identity.
     *
     * @return the operation result
     */
    BackendId id();

    /**
     * Return the current session state.
     *
     * @return the operation result
     */
    State state();

    /**
     * Creates a fixed statement view for a single SQL.
     *
     * @return the operation result
     */
    BackendReadView statement();

    /**
     * Writing non-empty values for current business transaction.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    void put(String shard, String key, byte[] value);

    /**
     * Write a deletion tombstone in the current business transaction.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    void delete(String shard, String key);

    /**
     * Durable commit confirmed with original core and returned the same receipt successfully.
     *
     * @return the operation result
     */
    BackendCommit commit();

    /**
     * Only the original transaction which is not yet commit is specified rollback.
     */
    void rollback();

    /**
     * Close active transaction, keep the original identity and decision of UNKNOWN.
     */
    @Override
    void close();

    /**
     * Operationally detectable session state, UNKNOWN does not allow fake rollback.
     */
    enum State {
        ACTIVE, COMMITTED, ABORTED, UNKNOWN
    }
}
