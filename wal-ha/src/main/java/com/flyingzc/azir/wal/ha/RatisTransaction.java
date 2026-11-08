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

import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalReadSnapshot;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalTransaction;
import com.flyingzc.azir.common.ErrorCode;

import java.util.Optional;
import java.util.UUID;

/**
 * Transaction session without moving, blind retry, the unknown result retains the original UUID and prohibits false rollback.
 */
public final class RatisTransaction implements AutoCloseable {

    private final RatisWalNode node;

    private final WalMvccRuntime original;

    private final WalTransaction transaction;

    private State state = State.ACTIVE;

    private WalReceipt receipt;

    private final Operations operations = new Operations() {

        /**
         * Original transaction ID does not change due to the return view.
         *
         * @return the operation result
         */
        @Override
        public UUID id() {
            return RatisTransaction.this.id();
        }

        /**
         * The operation interface entrusts the same session.
         *
         * @return the operation result
         */
        @Override
        public ReadView statement() {
            return RatisTransaction.this.statement();
        }

        /**
         * Do not expose the bottom transaction or kernel to business recall.
         *
         * @param shard logical shard identifier
         * @param key business key
         * @param value value to write
         */
        @Override
        public void put(final String shard, final String key, final byte[] value) {
            RatisTransaction.this.put(shard, key, value);
        }

        /**
         * Tombstone maintains the same ownership as normal.
         *
         * @param shard logical shard identifier
         * @param key business key
         */
        @Override
        public void delete(final String shard, final String key) {
            RatisTransaction.this.delete(shard, key);
        }
    };

    /**
     * The inside entrance of the package is maintained with the original kernel and the original nud transaction API remains unchanged.
     *
     * @param node node
     * @param original original
     * @param transaction transaction context
     */
    RatisTransaction(final RatisWalNode node, final WalMvccRuntime original, final WalTransaction transaction) {
        this.node = node;
        this.original = original;
        this.transaction = transaction;
    }

    /**
     * Commit can save stability transaction ID before commit, then search for that number after unknown.
     *
     * @return the operation result
     */
    public UUID id() {
        return transaction.id();
    }

    /**
     * Opens the current session state and does not extrapolate the missing result of another node to rollback.
     *
     * @return the operation result
     */
    public synchronized State state() {
        return state;
    }

    /**
     * The callback can only get an operational view.
     *
     * @return the operation result
     */
    public Operations operations() {
        return operations;
    }

    /**
     * A visible new statement, the same ReadView can read more than shard.
     *
     * @return the operation result
     */
    public synchronized ReadView statement() {
        active();
        return new ReadView(this, node.beginReadStatement(transaction));
    }

    /**
     * A new statement has been created to facilitate reading, which is consistent with the RC Visibility Contract.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    public synchronized Optional<byte[]> get(final String shard, final String key) {
        return statement().get(shard, key);
    }

    /**
     * Read write set and commit version from current statement boundaries.
     *
     * @param snapshot snapshot
     * @param shard logical shard identifier
     * @param key business key
     * @return the operation result
     */
    private synchronized Optional<byte[]> read(final WalReadSnapshot snapshot, final String shard, final String key) {
        active();
        return node.get(transaction, shard, key, snapshot);
    }

    /**
     * Original write set reproduced by kernel defence, role and capacity validate continues in effect.
     *
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    public synchronized void put(final String shard, final String key, final byte[] value) {
        active();
        node.put(transaction, shard, key, value);
    }

    /**
     * Delete only after actual consensus commit.
     *
     * @param shard logical shard identifier
     * @param key business key
     */
    public synchronized void delete(final String shard, final String key) {
        active();
        node.delete(transaction, shard, key);
    }

    /**
     * No automatic repetition of the same preparation batch, and UNKNOWN is conservatively classified as failure except for a clear conflict/interruption.
     *
     * @return the operation result
     * @throws WalMvccException if the operation cannot complete
     */
    public synchronized WalReceipt commit() {
        if (state == State.COMMITTED) {
            return receipt;
        }
        active();
        state = State.DECIDING;
        try {
            receipt = node.commit(transaction);
            state = State.COMMITTED;
            return receipt;
        } catch (final RuntimeException failure) {
            if (failure instanceof WalMvccException wal && (wal.errorCode() == ErrorCode.WRITE_CONFLICT || wal.errorCode() == ErrorCode.TRANSACTION_ABORTED)) {
                state = State.ROLLED_BACK;
                throw failure;
            }
            state = State.UNKNOWN;
            throw new WalMvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "commit unresolved; retain original UUID and resolve before business retry", id(), failure);
        } catch (final Error failure) {
            state = State.UNKNOWN;
            throw failure;
        }
    }

    /**
     * Allow only session cleanup of original core local write set that has not yet started decision.
     *
     * @throws WalMvccException if the operation cannot complete
     */
    public synchronized void rollback() {
        if (state == State.ROLLED_BACK) {
            return;
        }
        if (state == State.UNKNOWN || state == State.DECIDING) {
            throw new WalMvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN, "cannot roll back unresolved commit", id(), null);
        }
        active();
        var lifecycle = original.lifecycle();
        if (lifecycle.closed() || lifecycle.fatal() != null) {
            state = State.ROLLED_BACK;
            return;
        }
        try {
            original.rollback(transaction);
            state = State.ROLLED_BACK;
        } catch (final RuntimeException failure) {
            state = State.ABANDONED;
            throw failure;
        }
    }

    /**
     * Close unknown commit does not execute rollback; the cleanup of active session cannot be quietly swallowed.
     */
    @Override
    public synchronized void close() {
        if (state == State.ACTIVE) {
            rollback();
        }
    }

    /**
     * Terminal state handles cannot be reused for new transaction or new generation.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    private void active() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("transaction session is " + state);
        }
    }

    /**
     * The open result state of session. ABANDONED indicates that the local cleanup is incomplete without commit write set.
     */
    public enum State {
        ACTIVE, DECIDING, COMMITTED, ROLLED_BACK, UNKNOWN, ABANDONED
    }

    /**
     * Business callback only executes transaction operations, and cannot leave the outer layer wrong after half of commit.
     */
    public interface Operations {

        /**
         * You can write transaction operations idempotent tags before commit, and you can still associate with the original UUID after unknown.
         *
         * @return the operation result
         */
        UUID id();

        /**
         * The new statement creates a true majority barrier, and RR remains fixed to the transaction border.
         *
         * @return the operation result
         */
        ReadView statement();

        /**
         * Each convenient reading is a new statement, and multiple-key equivalents must be re-referenced to ReadView.
         *
         * @param shard logical shard identifier
         * @param key business key
         * @return the operation result
         */
        default Optional<byte[]> get(final String shard, final String key) {
            return statement().get(shard, key);
        }

        /**
         * Buffer This transaction writing.
         *
         * @param shard logical shard identifier
         * @param key business key
         * @param value value to write
         */
        void put(String shard, String key, byte[] value);

        /**
         * Buffer tombstone.
         *
         * @param shard logical shard identifier
         * @param key business key
         */
        void delete(String shard, String key);
    }

    /**
     * Multiple keys to the same sentence, cross-shard reading with the same fixed global watermark.
     */
    public static final class ReadView {

        private final RatisTransaction owner;

        private final WalReadSnapshot snapshot;

        /**
         * Creates only from the current active transaction.
         *
         * @param owner owner
         * @param snapshot snapshot
         */
        private ReadView(final RatisTransaction owner, final WalReadSnapshot snapshot) {
            this.owner = owner;
            this.snapshot = snapshot;
        }

        /**
         * Watermark is detectable and not allowed to be modified or used for other transaction.
         *
         * @return the operation result
         */
        public long sequence() {
            return snapshot.sequence();
        }

        /**
         * Transaction cannot continue reading after closing or entering decision.
         *
         * @param shard logical shard identifier
         * @param key business key
         * @return the operation result
         */
        public Optional<byte[]> get(final String shard, final String key) {
            return owner.read(snapshot, shard, key);
        }
    }
}
