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

package com.flyingzc.azir.common;

/**
 * The MVCC component harmonizes the abnormal types.
 */
public class MvccException extends RuntimeException {

    private final ErrorCode errorCode;

    private final boolean retryable;

    private final Long txId;

    /**
     * Construct a MvccException instance.
     *
     * @param errorCode error code
     * @param message message
     * @param retryable retryable
     * @param txId transaction identifier
     */
    public MvccException(final ErrorCode errorCode, final String message, final boolean retryable, final Long txId) {
        super(message);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    /**
     * Construct a MvccException instance.
     *
     * @param errorCode error code
     * @param message message
     * @param retryable retryable
     * @param txId transaction identifier
     * @param cause cause
     */
    public MvccException(final ErrorCode errorCode, final String message, final boolean retryable, final Long txId, final Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
        this.txId = txId;
    }

    /**
     * Returns the MVCC error code.
     *
     * @return the operation result
     */
    public ErrorCode errorCode() {
        return errorCode;
    }

    /**
     * Returns whether an anomaly allows retry.
     *
     * @return the operation result
     */
    public boolean retryable() {
        return retryable;
    }

    /**
     * Return the transaction identifier.
     *
     * @return the operation result
     */
    public Long txId() {
        return txId;
    }
}
