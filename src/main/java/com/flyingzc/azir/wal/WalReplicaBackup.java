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

import java.io.IOException;

/**
 * A copy backup of the local mirror and back-end reading view has been fixed, can only be completed once, during which source commit is allowed to continue.
 */
public final class WalReplicaBackup implements AutoCloseable {

    private final WalBackup.Prepared prepared;

    private final Runnable released;

    private boolean running, closed;

    /**
     * Creates only when running with decision and materialization protection.
     *
     * @param prepared prepared
     * @param released released
     */
    WalReplicaBackup(final WalBackup.Prepared prepared, final Runnable released) {
        this.prepared = prepared;
        this.released = released;
    }

    /**
     * Fixed operational boundaries do not change with the source 's new commit.
     *
     * @return the operation result
     */
    public WalCheckpoint boundary() {
        return prepared.boundary();
    }

    /**
     * Executing complete export, verify and release in the maintenance thread does not affect confirmed source transaction.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public WalBackup.Info complete() throws IOException {
        synchronized (this) {
            if (running || closed) {
                throw new IOException("replica backup already running or closed");
            }
            running = true;
        }
        Throwable failure = null;
        try {
            return prepared.complete();
        } catch (final IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            try {
                synchronized (this) {
                    running = false;
                    close();
                }
            } catch (final IOException cleanup) {
                if (failure == null) {
                    throw cleanup;
                }
                failure.addSuppressed(cleanup);
            }
        }
    }

    /**
     * The exported view cannot be closed earlier by another thread; the caller should wait for the maintenance task to end.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        if (running) {
            throw new IOException("replica backup export still running");
        }
        try {
            prepared.close();
        } finally {
            closed = true;
            released.run();
        }
    }
}
