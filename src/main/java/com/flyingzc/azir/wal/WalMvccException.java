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

package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.ErrorCode;
import java.util.UUID;

/**
 * The new mode is a stand-alone transaction anomaly and does not modify the original anomaly interface or error code.
 */
public final class WalMvccException extends RuntimeException {

    private final ErrorCode errorCode;

    private final UUID transactionId;

    /**
     * Saves the error category, stability transaction ID and original failure cause.
     *
     * @param code code
     * @param message message
     * @param transactionId transaction id
     * @param cause cause
     */
    public WalMvccException(final ErrorCode code, final String message, final UUID transactionId, final Throwable cause) {
        super(message, cause);
        this.errorCode = code;
        this.transactionId = transactionId;
    }

    /**
     * Returns the wrong code that can be used for business decision-making.
     *
     * @return the operation result
     */
    public ErrorCode errorCode() {
        return errorCode;
    }

    /**
     * Returns transaction ID for commit result reconciliation.
     *
     * @return the operation result
     */
    public UUID transactionId() {
        return transactionId;
    }
}
