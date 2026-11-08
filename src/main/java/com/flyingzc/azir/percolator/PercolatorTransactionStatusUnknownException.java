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

package com.flyingzc.azir.percolator;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import java.util.Objects;

/**
 * SQL or KV commit unknown outcome carry the complete transaction identity and cannot speculate on the startTs only as a primary.
 */
public final class PercolatorTransactionStatusUnknownException extends MvccException {

    private final PercolatorRuntime.Handle handle;

    /**
     * Keeps the primary shard, keys and startTs, and prohibits automatic exchange of identity retry.
     *
     * @param handle handle
     * @param cause cause
     */
    public PercolatorTransactionStatusUnknownException(final PercolatorRuntime.Handle handle, final Throwable cause) {
        super(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "query the original Percolator handle before retrying",
                false, Objects.requireNonNull(handle, "handle").startTs(), cause);
        this.handle = handle;
    }

    /**
     * Returns original identity for new running query or recovery.
     *
     * @return the operation result
     */
    public PercolatorRuntime.Handle handle() {
        return handle;
    }
}
