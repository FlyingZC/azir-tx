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
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;
import com.flyingzc.azir.storage.KeyRange;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Holds a Percolator transaction's original identity, fixed RR snapshot and buffered mutations.
 */
public final class PercolatorTransaction implements AutoCloseable {

    final PercolatorRuntime runtime;

    final long startTs;

    final IsolationLevel isolation;

    final NavigableMap<PercolatorKey, Mutation> writes = new TreeMap<>();

    State state = State.ACTIVE;

    PercolatorRuntime.Handle handle;

    PercolatorRuntime.Commit result;

    long writeBytes;

    private final ReentrantLock sessionLock = new ReentrantLock();

    /**
     * Session with a fixed snapshot border is only constructed from running time.
     *
     * @param runtime runtime
     * @param startTs transaction start timestamp
     * @param isolation global transaction isolation level
     */
    PercolatorTransaction(final PercolatorRuntime runtime, final long startTs, final IsolationLevel isolation) {
        this.runtime = runtime;
        this.startTs = startTs;
        this.isolation = isolation;
    }

    /**
     * Returns the original immutable -coded transaction identifier.
     *
     * @return the operation result
     */
    public long startTs() {
        return startTs;
    }

    /**
     * Return the current session state.
     *
     * @return the operation result
     */
    public State state() {
        return locked(() -> state);
    }

    /**
     * Returns prewrite to restart the Primary address and original startTs for query.
     *
     * @return the operation result
     */
    public Optional<PercolatorRuntime.Handle> handle() {
        return locked(() -> Optional.ofNullable(handle));
    }

    /**
     * Buffer Writes and copys business values to avoid the caller changing transaction after prewrite.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    public void put(final String shard, final String key, final byte[] value) {
        locked(() -> {
            java.util.Objects.requireNonNull(value, "value");
            runtime.live(this, startTs, () -> {
                buffer(new PercolatorKey(shard, key), Mutation.put(key, value));
                return null;
            });
            return null;
        });
    }

    /**
     * Buffer delete; expressed after commit as tombstone version.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    public void delete(final String shard, final String key) {
        locked(() -> runtime.live(this, startTs, () -> {
            buffer(new PercolatorKey(shard, key), Mutation.delete(key));
            return null;
        }));
    }

    /**
     * Each convenient reading represents a new statement; RR uses starttTs, RC allocates new read timestamp.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    public Optional<byte[]> get(final String shard, final String key) {
        return statement().get(shard, key);
    }

    /**
     * Facilitates a page break for each new statement; the view returned by maintaining RC snapshot over the page should be reused.
     *
     * @param shard logical shard identifier
     * @param range range
     * @param afterKeyExclusive after key exclusive
     * @param limit maximum number of items
     * @return the operation result
     */
    public PercolatorScanPage scanPage(final String shard, final KeyRange range, final String afterKeyExclusive, final int limit) {
        return statement().scanPage(shard, range, afterKeyExclusive, limit);
    }

    /**
     * Gets the same global boundary for multiple cross-shard dots of the same phrase.
     *
     * @return the operation result
     */
    public ReadView statement() {
        return locked(() -> {
            readable();
            long readTs = runtime.live(this, startTs, () -> isolation == IsolationLevel.REPEATABLE_READ ? startTs : runtime.nextTimestamp());
            return new ReadView(readTs);
        });
    }

    /**
     * Visible prewrite for phased implementation and recovery exercises; follow-up can only be commit, rollback or read.
     */
    public void prewrite() {
        locked(() -> {
            try {
                return runtime.live(this, startTs, () -> {
                    runtime.prewrite(this);
                    return null;
                });
            } catch (final Error fatal) {
                sealAfterFatal();
                throw fatal;
            }
        });
    }

    /**
     * Commit and returns durable receipt to the purimary, confirming that commit returns the same result.
     *
     * @return the operation result
     */
    public PercolatorRuntime.Commit commit() {
        return locked(() -> {
            if (state == State.COMMITTED) {
                return result;
            }
            try {
                return runtime.live(this, startTs, () -> runtime.commit(this));
            } catch (final RuntimeException failure) {
                if (state == State.COMMITTED && result != null) {
                    return result;
                }
                throw failure;
            } catch (final Error fatal) {
                sealAfterFatal();
                throw fatal;
            } finally {
                runtime.release(this);
            }
        });
    }

    /**
     * Active rollback must first determine durable on the Primary.
     */
    public void rollback() {
        locked(() -> {
            try {
                return runtime.terminal(() -> {
                    runtime.rollback(this);
                    return null;
                });
            } catch (final Error fatal) {
                sealAfterFatal();
                throw fatal;
            } finally {
                runtime.release(this);
            }
        });
    }

    /**
     * Disables the continuation of original write set after fatal error; prewrite has not yet created a Primary to definitively end local empty participant transaction.
     */
    private void sealAfterFatal() {
        if (state != State.COMMITTED && state != State.ABORTED) {
            state = handle == null ? State.ABORTED : State.UNKNOWN;
        }
    }

    /**
     * Rollback when active session closes; UNKNOWN keeps original identity without pretending that rollback is successful.
     */
    @Override
    public void close() {
        locked(() -> {
            if (state == State.ACTIVE || state == State.PREWRITTEN) {
                rollback();
            }
            return null;
        });
    }

    /**
     * JDBC uses unmounted visible locks during jamming to avoid Java 21 synchronized fixed virtual circuit carriers.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     */
    private <T> T locked(final Supplier<T> work) {
        sessionLock.lock();
        try {
            return work.get();
        } finally {
            sessionLock.unlock();
        }
    }

    /**
     * Use write set after bounded overwrite to avoid single transaction full process memory.
     *
     * @param key business key
     * @param mutation mutation
     * @throws MvccException if the operation cannot complete
     */
    private void buffer(final PercolatorKey key, final Mutation mutation) {
        if (state != State.ACTIVE) {
            throw illegal("writes are frozen after prewrite");
        }
        runtime.store(key.shard());
        Mutation old = writes.get(key);
        long oldSize = old == null ? 0 : size(old);
        long nextSize = writeBytes - oldSize + size(mutation);
        int nextKeys = writes.size() + (old == null ? 1 : 0);
        if (nextKeys > runtime.options().maxKeys() || nextSize > runtime.options().maxBytes()) {
            throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED, "Percolator write budget exceeded", false, startTs);
        }
        // Write set caps pre-exceed payload to avoid the creation of equivalent stack distributions when super big requests are rejected.
        Mutation frozen = mutation.value() == null ? mutation : new Mutation(mutation.key(), mutation.type(), mutation.value().clone());
        writes.put(key, frozen);
        writeBytes = nextSize;
    }

    /**
     * Calculates the UTF-8 write set occupancy of keys and values.
     *
     * @param mutation mutation
     * @return the operation result
     */
    private static long size(final Mutation mutation) {
        return com.flyingzc.azir.storage.Utf8KeyOrder.encode(mutation.key()).length
                + (long) (mutation.value() == null ? 0 : mutation.value().length);
    }

    /**
     * Rejects terminal state session to continue accessing buffer writing or storage.
     */
    void readable() {
        if (state != State.ACTIVE && state != State.PREWRITTEN) {
            throw illegal("transaction is " + state);
        }
    }

    /**
     * Verify More key direct committee check to use a single bar in query.
     *
     * @param message message
     * @return the operation result
     */
    MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, startTs);
    }

    /**
     * Session status; UNKNOWN cannot reset commit after rollback.
     */
    public enum State {
        ACTIVE, PREWRITTEN, COMMITTED, ABORTED, UNKNOWN
    }

    /**
     * Fixed timestamp statement view, still overwrites the current transaction own buffer writing.
     */
    public final class ReadView {

        private final long readTs;

        /**
         * Creates a statement view of the fixed boundary.
         *
         * @param readTs snapshot read timestamp
         */
        private ReadView(final long readTs) {
            this.readTs = readTs;
        }

        /**
         * Returns global Logic timestamp for statements that allow the isolation of verify.
         *
         * @return the operation result
         */
        public long readTs() {
            return readTs;
        }

        /**
         * Reading your own writing or parsing a remote snapshot, the view after terminal state is invalid at the time of the session lock.
         *
         * @param shard logical shard identifier
         * @param key business key
         * @return the operation result
         */
        public Optional<byte[]> get(final String shard, final String key) {
            return locked(() -> runtime.live(PercolatorTransaction.this, readTs, () -> {
                readable();
                PercolatorKey address = new PercolatorKey(shard, key);
                runtime.store(shard);
                Mutation own = writes.get(address);
                if (own != null) {
                    return Optional.ofNullable(own.value()).map(byte[]::clone);
                }
                return runtime.read(address, readTs);
            }));
        }

        /**
         * The same sentence keeps readTs over the page and covers transaction new, updated and deleted that has not yet been prewrite.
         *
         * @param shard logical shard identifier
         * @param range range
         * @param afterKeyExclusive after key exclusive
         * @param limit maximum number of items
         * @return the operation result
         */
        public PercolatorScanPage scanPage(final String shard, final KeyRange range, final String afterKeyExclusive, final int limit) {
            return locked(() -> runtime.live(PercolatorTransaction.this, readTs, () -> {
                readable();
                return runtime.scanPage(PercolatorTransaction.this, readTs, shard, range, afterKeyExclusive, limit);
            }));
        }
    }
}
