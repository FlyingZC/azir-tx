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

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

/**
 * Keeps the unknown results of the original backend and data space identity, which are abnormal, and prevents automatic renumbering.
 */
public final class BackendUnknownException extends MvccException {

    private final BackendId id;

    /**
     * Handing the original storage anomaly with the complete transaction identity to the caller.
     *
     * @param id id
     * @param cause cause
     */
    public BackendUnknownException(final BackendId id, final Throwable cause) {
        super(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "query the original backend transaction", false,
                java.util.Objects.requireNonNull(id, "id").startTs(), cause);
        this.id = id;
    }

    /**
     * Returns stable original identity for new running queries.
     *
     * @return the operation result
     */
    public BackendId id() {
        return id;
    }
}
