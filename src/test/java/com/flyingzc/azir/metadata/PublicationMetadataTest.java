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

package com.flyingzc.azir.metadata;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Verify Fixed lease, terminal state Monolytic and bounded Metadata Clearing.
 */
class PublicationMetadataTest {

    /**
     * Default lease contract rejection of missing, mismatched, expired and released records, non-renewal or suspense validity.
     */
    @Test
    void leaseValidationRequiresOriginalLiveLease() {
        var store = new InMemoryTransactionMetadataStore();
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
        store.registerSnapshot(1, 1, Long.MAX_VALUE);
        assertTrue(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE - 1));
        store.registerSnapshot(2, 2, 1);
        assertFalse(store.isSnapshotLeaseValid(2, 1));
        Assertions.assertEquals(1L, store.snapshotExpiry(2).orElseThrow());
        store.releaseSnapshot(1);
        assertFalse(store.isSnapshotLeaseValid(1, Long.MAX_VALUE));
    }

    /**
     * Malformed durable records cannot create metadata that can be read silently ignored.
     */
    @Test
    void recordRejectsMissingCommittedTimestampStatusAndIsolation() {
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.COMMITTED, null));
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), null, null));
        assertThrows(MvccException.class,
                () -> new TxRecord(1, 1, null, Set.of(), TxStatus.ACTIVE, null));
        assertDoesNotThrow(() -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.PREPARING, null));
        assertDoesNotThrow(() -> new TxRecord(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), TxStatus.COMMITTED, 2L));
    }

    /**
     * All commit entrances reject commit timestamp no later than the start time and fail not to change status.
     */
    @Test
    void commitTimestampMustBeStrictlyAfterStartTimestamp() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 10, IsolationLevel.REPEATABLE_READ);
        store.registerParticipants(1, Set.of("a"));
        for (long commitTs : new long[]{Long.MIN_VALUE, 9, 10}) {
            Assertions.assertEquals(ErrorCode.ILLEGAL_STATE,
                    assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, commitTs)).errorCode());
            assertThrows(MvccException.class, () -> store.markCommitted(1, commitTs));
            assertThrows(MvccException.class, () -> store.persistCommitted(1, Set.of("a"), commitTs));
            assertThrows(MvccException.class, () -> store.markSingleShardCommitted(1, "a", commitTs));
            assertThrows(MvccException.class, () -> store.createCommitted(2, 10, IsolationLevel.REPEATABLE_READ, Set.of("a"), commitTs));
            Assertions.assertEquals(TxStatus.ACTIVE, store.get(1).orElseThrow().status());
            assertTrue(store.get(2).isEmpty());
        }
        store.markLocalCommitPending(1, 11);
        Assertions.assertEquals(11L, store.markCommitted(1, 11).commitTs());
    }

    /**
     * The CSV route sign must be legal in the creation, registration, rapid updating and terminal state replay entry.
     */
    @Test
    void participantIdentifiersCannotBeBlankOrContainCsvSeparators() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        for (String participant : new String[]{"", " ", "\t", "a,b", ","}) {
            Set<String> participants = Set.of(participant);
            assertThrows(MvccException.class, () -> store.createPreparing(2, 2, IsolationLevel.REPEATABLE_READ, participants));
            assertThrows(MvccException.class, () -> store.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, participants, 3));
            assertThrows(MvccException.class, () -> store.registerParticipants(1, participants));
            assertThrows(MvccException.class, () -> store.persistPreparing(1, participants));
            assertThrows(MvccException.class, () -> store.persistCommitted(1, participants, 3));
            assertThrows(MvccException.class, () -> store.markSingleShardCommitted(1, participant, 3));
            assertTrue(store.get(2).isEmpty());
            Assertions.assertEquals(TxStatus.ACTIVE, store.get(1).orElseThrow().status());
        }
        store.markCommitted(1, Set.of("valid-shard"), 3);
        assertThrows(MvccException.class, () -> store.registerParticipants(1, Set.of("a,b")));
        assertThrows(MvccException.class, () -> store.persistPreparing(1, Set.of(" ")));
        assertThrows(MvccException.class, () -> store.markCommitted(1, Set.of(","), 3));
        Assertions.assertEquals(Set.of("valid-shard"), store.get(1).orElseThrow().participants());
    }

    /**
     * The single shard unknown commit position prevents the new snapshot and only real terminal state can remove this release barrier.
     */
    @Test
    void pendingLocalCommitBlocksPublicationUntilTerminalDecision() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, 10));
        store.registerParticipants(1, Set.of("a"));
        store.markLocalCommitPending(1, 10);
        store.markLocalCommitPending(1, 10);
        var error = assertThrows(MvccException.class, store::assertNoPendingPublications);
        Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, error.errorCode());
        assertFalse(error.retryable());
        assertThrows(MvccException.class, () -> store.markLocalCommitPending(1, 11));
        assertThrows(MvccException.class, () -> store.markCommitted(1, 11));
        assertThrows(MvccException.class, () -> store.registerParticipants(1, Set.of("b")));
        store.markAborted(1);
        assertNull(store.get(1).orElseThrow().commitTs());
        assertDoesNotThrow(store::assertNoPendingPublications);
    }

    /**
     * Snapshots with lazily created metadata must also prevent the safe point from advancing past them.
     */
    @Test
    void lazySnapshotPinsOldestTimestampAndCannotBeRenewed() {
        var store = new InMemoryTransactionMetadataStore();
        store.registerSnapshot(10, 10, 100);
        Assertions.assertEquals(10L, store.oldestActiveStartTs().orElseThrow());
        store.registerSnapshot(10, 20, 100);
        Assertions.assertEquals(10L, store.oldestActiveStartTs().orElseThrow());
        assertThrows(MvccException.class, () -> store.registerSnapshot(10, 10, 200));
        Assertions.assertEquals(0, store.expireSnapshots(99, 1));
        Assertions.assertEquals(1, store.expireSnapshots(100, 1));
        assertTrue(store.snapshotExpiry(10).isEmpty());
    }

    /**
     * Repeat commit must keep timestamp and cannot cancel commit for quick updates or rollback.
     */
    @Test
    void terminalStatusAndCommitTimestampAreImmutable() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(10, 10, IsolationLevel.REPEATABLE_READ);
        store.registerParticipants(10, Set.of("a"));
        store.registerParticipants(10, Set.of("b"));
        Assertions.assertEquals(Set.of("a", "b"), store.get(10).orElseThrow().participants());
        store.persistCommitted(10, 20);
        store.persistPreparing(10, Set.of("c"));
        Assertions.assertEquals(TxStatus.COMMITTED, store.markAborted(10).status());
        Assertions.assertEquals(Set.of("a", "b"), store.registerParticipants(10, Set.of("c")).participants());
        var error = assertThrows(MvccException.class, () -> store.persistCommitted(10, 21));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, error.errorCode());
        Assertions.assertEquals(20L, store.markCommitted(10, 20).commitTs());
    }

    /**
     * Recovery candidate press transaction identifier page break and skip pending transaction that still holds lease.
     */
    @Test
    void recoveryCandidatesUseLeasesAndCursor() {
        var store = new InMemoryTransactionMetadataStore();
        store.create(1, 1, IsolationLevel.REPEATABLE_READ);
        store.registerSnapshot(1, 1, 100);
        store.create(2, 2, IsolationLevel.REPEATABLE_READ);
        store.createCommitted(3, 3, IsolationLevel.REPEATABLE_READ, Set.of(), 4);
        Assertions.assertEquals(2L, store.findRecoveryCandidates(99, 0, 1).getFirst().txId());
        Assertions.assertEquals(3L, store.findRecoveryCandidates(99, 2, 1).getFirst().txId());
        Assertions.assertEquals(1L, store.findRecoveryCandidates(100, 0, 1).getFirst().txId());
    }

    /**
     * Terminal state that completes only materialization and is earlier than safe point and retains the duration can be cleaned up.
     */
    @Test
    void purgeRequiresFinalizationAndHonorsBatchLimit() {
        var store = new InMemoryTransactionMetadataStore();
        store.createCommitted(1, 1, IsolationLevel.REPEATABLE_READ, Set.of(), 5);
        store.createCommitted(2, 2, IsolationLevel.REPEATABLE_READ, Set.of(), 6);
        Assertions.assertEquals(0, store.purgeFinalized(10, Long.MAX_VALUE, 10));
        store.markFinalized(1);
        store.markFinalized(2);
        Assertions.assertEquals(0, store.purgeFinalized(5, Long.MAX_VALUE, 10));
        Assertions.assertEquals(1, store.purgeFinalized(10, Long.MAX_VALUE, 1));
        Assertions.assertEquals(1, store.purgeFinalized(10, Long.MAX_VALUE, 1));
    }

    /**
     * The same example barrier can be re-entered and used after an abnormal exit.
     */
    @Test
    void publicationLockIsReentrantAndReleasedAfterFailure() {
        var store = new InMemoryTransactionMetadataStore();
        assertThrows(IllegalArgumentException.class, () -> store.withPublicationLock(() -> {
            store.withPublicationLock(() -> store.create(1, 1, IsolationLevel.REPEATABLE_READ));
            throw new IllegalArgumentException("failure");
        }));
        Assertions.assertEquals(1L, store.withPublicationLock(() -> store.get(1).orElseThrow()).txId());
    }
}
