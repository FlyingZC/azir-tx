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

package com.flyingzc.azir.wal.ha;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Preserves the original startup failure and a cleanup-only handle for resources that remain owned.
 */
public class RatisStartupException extends IOException {

    private Closeable pending;

    /**
     * Only the clean-up capability is exposed and the transaction entrance of an uncompleted object is not allowed to be called.
     *
     * @param failure failure
     * @param pending pending
     */
    public RatisStartupException(final Throwable failure, final Closeable pending) {
        super("startup failed; resource cleanup remains pending", failure);
        this.pending = Objects.requireNonNull(pending);
    }

    /**
     * Clean-up is successful before the handle is empty, and failure or interruption can still follow the original phase retry.
     *
     * @throws IOException if the operation cannot complete
     */
    public synchronized void retryCleanup() throws IOException {
        if (pending != null) {
            pending.close();
            pending = null;
        }
    }

    /**
     * Wait for cleanup to complete before releasing the control pool or reopening the same directory.
     *
     * @return the operation result
     */
    public synchronized boolean cleanupPending() {
        return pending != null;
    }

    /**
     * Combining the inner layer does not complete clean-up and the outer layer has its own resources, and does not skip the inner layer dependent or overwhelm original start-up anomalies.
     *
     * @param failure failure
     * @param release release
     * @return the operation result
     */
    static IOException afterFailure(final Throwable failure, final Closeable release) {
        Closeable cleanup = failure instanceof RatisStartupException nested
                ? new RatisCloseSequence(nested::retryCleanup, release)
                : release;
        try {
            cleanup.close();
        } catch (final Throwable blocked) {
            var pending = new RatisStartupException(failure, cleanup);
            if (blocked != failure) {
                pending.addSuppressed(blocked);
            }
            return pending;
        }
        if (failure instanceof IOException io) {
            return io;
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IOException("startup failed after resource cleanup", failure);
    }
}
