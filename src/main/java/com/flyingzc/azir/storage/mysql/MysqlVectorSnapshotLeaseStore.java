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

package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.control.MysqlConnectionContext;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-level vector reader lease and GC shared a serialized prototype of the durable line.
 */
final class MysqlVectorSnapshotLeaseStore {

    private static final String FLOOR_TABLE = "mvcc_vector_gc_floor_v1";

    private static final String LEASE_TABLE = "mvcc_vector_reader_lease_v1";

    private final DataSource center;

    private final List<String> routes;

    private final int stripes;

    private final String layoutHash;

    private volatile String expectedIdentity;

    /**
     * Fixed route and strip layouts cannot be used to explain the lease vector with another pounce.
     *
     * @param center center
     * @param routes routes
     * @param stripes stripes
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorSnapshotLeaseStore(final DataSource center, final List<String> routes, final int stripes) {
        this.center = Objects.requireNonNull(center, "center");
        this.routes = routes.stream().sorted().toList();
        if (this.routes.isEmpty() || this.routes.stream().anyMatch(route -> route == null || route.isBlank())
                || this.routes.stream().distinct().count() != this.routes.size()
                || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector reader lease layout");
        }
        this.stripes = stripes;
        this.layoutHash = MysqlPublishedVectorCut.layoutFingerprint(this.routes, stripes);
    }

    /**
     * GC Scheduler check the actual identity control library before startup.
     *
     * @return the operation result
     */
    DataSource centerDataSource() {
        return center;
    }

    /**
     * Hashi, which fixes shard and splits, is a prerequisite for interpretation across the GC lower boundary of the process.
     *
     * @return the operation result
     */
    String layoutHash() {
        return layoutHash;
    }

    /**
     * Initialization only fills the missing bottom zero and lease tables, restarting cannot empty old reader or advance safe point.
     */
    void initializeProduct() {
        try (Connection connection = center.getConnection(); Statement sql = connection.createStatement()) {
            String identity = MysqlVectorDatabaseIdentity.read(connection);
            if (expectedIdentity != null && !expectedIdentity.equals(identity)) {
                throw illegal("vector reader control identity changed during initialization");
            }
            sql.execute("""
                    create table if not exists mvcc_vector_gc_floor_v1 (
                      singleton_id tinyint not null primary key,
                      layout_hash char(64) not null,
                      decision_epoch bigint not null,
                      vector_bytes blob not null,
                      generation bigint not null
                    ) engine=InnoDB
                    """);
            sql.execute("""
                    create table if not exists mvcc_vector_reader_lease_v1 (
                      owner_id varchar(128) collate utf8mb4_0900_bin not null primary key,
                      layout_hash char(64) not null,
                      generation bigint not null,
                      decision_epoch bigint not null,
                      vector_bytes blob not null,
                      expires_at bigint not null,
                      index mvcc_vector_reader_expiry (expires_at, owner_id)
                    ) engine=InnoDB
                    """);
            MysqlStripedSnapshotOracle.Snapshot zero = zeroSnapshot();
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_gc_floor_v1
                      (singleton_id, layout_hash, decision_epoch, vector_bytes, generation)
                    values (1, ?, 0, ?, 1)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setString(1, layoutHash);
                insert.setBytes(2, encode(zero));
                insert.executeUpdate();
            }
            readFloor(connection, false);
            expectedIdentity = identity;
        } catch (final SQLException ex) {
            throw unavailable("initialize vector snapshot protection failed", ex);
        }
    }

    /**
     * Locking the GC bottom-to-register process is the oldest read snapshot, and failed when the old cut side was lower than the already collection bottom.
     *
     * @param ownerId owner id
     * @param floor floor
     * @param leaseMillis lease millis
     * @return the operation result
     */
    Guard acquire(final String ownerId, final MysqlStripedSnapshotOracle.Snapshot floor,
                  final long leaseMillis) {
        requireOwnerAndLease(ownerId, leaseMillis);
        Objects.requireNonNull(floor, "floor");
        long requestStarted = System.nanoTime();
        try {
            long generation = MysqlConnectionContext.inTransaction(center, connection -> {
                Floor safe = readFloor(connection, true);
                requireAtLeast(floor, safe.snapshot());
                long now = databaseNow(connection);
                Long previous = null;
                Long expiry = null;
                try (PreparedStatement query = connection.prepareStatement("""
                        select generation, expires_at from mvcc_vector_reader_lease_v1
                        where owner_id = ? for update
                        """)) {
                    query.setString(1, ownerId);
                    try (ResultSet row = query.executeQuery()) {
                        if (row.next()) {
                            previous = row.getLong(1);
                            expiry = row.getLong(2);
                        }
                    }
                }
                if (expiry != null && expiry > now) {
                    throw illegal("vector reader owner already has an active lease");
                }
                if (previous != null && previous == Long.MAX_VALUE) {
                    throw illegal("vector reader lease generation exhausted");
                }
                long nextGeneration = previous == null ? 1 : previous + 1;
                try (PreparedStatement update = connection.prepareStatement("""
                        insert into mvcc_vector_reader_lease_v1
                          (owner_id, layout_hash, generation, decision_epoch,
                           vector_bytes, expires_at)
                        values (?, ?, ?, ?, ?, ?)
                        on duplicate key update
                          layout_hash = values(layout_hash),
                          generation = values(generation),
                          decision_epoch = values(decision_epoch),
                          vector_bytes = values(vector_bytes),
                          expires_at = values(expires_at)
                        """)) {
                    update.setString(1, ownerId);
                    update.setString(2, layoutHash);
                    update.setLong(3, nextGeneration);
                    update.setLong(4, floor.decisionEpoch());
                    update.setBytes(5, encode(floor));
                    update.setLong(6, Math.addExact(now, leaseMillis));
                    update.executeUpdate();
                }
                return nextGeneration;
            });
            return new Guard(ownerId, generation, leaseMillis,
                    conservativeDeadline(requestStarted, leaseMillis));
        } catch (final SQLException ex) {
            throw unavailable("acquire vector reader lease failed", ex);
        }
    }

    /**
     * GC will advance only to the minimum fractions protected by the current credible release cut-off and all unexpired processes.
     *
     * @param published published
     * @return the operation result
     */
    MysqlStripedSnapshotOracle.Snapshot advanceSafeFloor(
                                                         final MysqlStripedSnapshotOracle.Snapshot published) {
        Objects.requireNonNull(published, "published");
        try {
            return MysqlConnectionContext.inTransaction(center, connection -> {
                Floor current = readFloor(connection, true);
                requireAtLeast(published, current.snapshot());
                long now = databaseNow(connection);
                MysqlStripedSnapshotOracle.Snapshot next = published;
                try (PreparedStatement query = connection.prepareStatement("""
                        select layout_hash, decision_epoch, vector_bytes
                        from mvcc_vector_reader_lease_v1
                        where expires_at > ? for share
                        """)) {
                    query.setLong(1, now);
                    try (ResultSet row = query.executeQuery()) {
                        while (row.next()) {
                            if (!layoutHash.equals(row.getString(1))) {
                                throw new SQLException("active vector reader has incompatible layout");
                            }
                            next = minimum(next, new MysqlStripedSnapshotOracle.Snapshot(
                                    row.getLong(2), decode(row.getBytes(3))));
                        }
                    }
                }
                requireAtLeast(next, current.snapshot());
                if (!next.equals(current.snapshot())) {
                    try (PreparedStatement update = connection.prepareStatement("""
                            update mvcc_vector_gc_floor_v1
                            set decision_epoch = ?, vector_bytes = ?, generation = generation + 1
                            where singleton_id = 1 and generation = ? and layout_hash = ?
                            """)) {
                        update.setLong(1, next.decisionEpoch());
                        update.setBytes(2, encode(next));
                        update.setLong(3, current.generation());
                        update.setString(4, layoutHash);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("vector GC floor lost durable generation");
                        }
                    }
                }
                return next;
            });
        } catch (final SQLException ex) {
            throw unavailable("advance vector GC floor failed", ex);
        }
    }

    /**
     * Returns the lower bounds of GC that have been released by durable, without using local cache as the basis for collection.
     *
     * @return the operation result
     */
    MysqlStripedSnapshotOracle.Snapshot currentSafeFloor() {
        try (Connection connection = center.getConnection()) {
            return readFloor(connection, false).snapshot();
        } catch (final SQLException ex) {
            throw unavailable("read vector GC floor failed", ex);
        }
    }

    /**
     * The oldest active side of the current process can be moved up only in a single way and may not be extended backwards across the lower durable GC boundary.
     *
     * @param guard guard
     * @param floor floor
     */
    private void renew(final Guard guard, final MysqlStripedSnapshotOracle.Snapshot floor) {
        guard.requireActive();
        Objects.requireNonNull(floor, "floor");
        long requestStarted = System.nanoTime();
        try {
            MysqlConnectionContext.inTransaction(center, connection -> {
                Floor safe = readFloor(connection, true);
                requireAtLeast(floor, safe.snapshot());
                long now = databaseNow(connection);
                MysqlStripedSnapshotOracle.Snapshot previous;
                long expiry;
                try (PreparedStatement query = connection.prepareStatement("""
                        select layout_hash, generation, decision_epoch, vector_bytes, expires_at
                        from mvcc_vector_reader_lease_v1 where owner_id = ? for update
                        """)) {
                    query.setString(1, guard.ownerId);
                    try (ResultSet row = query.executeQuery()) {
                        if (!row.next() || !layoutHash.equals(row.getString(1))
                                || row.getLong(2) != guard.generation) {
                            throw illegal("vector reader lease was superseded");
                        }
                        previous = new MysqlStripedSnapshotOracle.Snapshot(
                                row.getLong(3), decode(row.getBytes(4)));
                        expiry = row.getLong(5);
                    }
                }
                if (expiry <= now) {
                    throw illegal("vector reader lease expired before renewal");
                }
                requireAtLeast(floor, previous);
                try (PreparedStatement update = connection.prepareStatement("""
                        update mvcc_vector_reader_lease_v1
                        set decision_epoch = ?, vector_bytes = ?, expires_at = ?
                        where owner_id = ? and generation = ? and layout_hash = ?
                        """)) {
                    update.setLong(1, floor.decisionEpoch());
                    update.setBytes(2, encode(floor));
                    update.setLong(3, Math.addExact(now, guard.leaseMillis));
                    update.setString(4, guard.ownerId);
                    update.setLong(5, guard.generation);
                    update.setString(6, layoutHash);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector reader lease renewal lost owner generation");
                    }
                }
                return null;
            });
            guard.validUntilNanos = conservativeDeadline(requestStarted, guard.leaseMillis);
        } catch (final SQLException | RuntimeException ex) {
            guard.revoked.set(true);
            if (ex instanceof SQLException sql) {
                throw unavailable("renew vector reader lease failed", sql);
            }
            throw (RuntimeException) ex;
        }
    }

    /**
     * Local First disallows service eligibility, then the same algebra durable lease expires and keeps the successor algebra.
     *
     * @param guard guard
     */
    private void release(final Guard guard) {
        guard.revoked.set(true);
        try {
            MysqlConnectionContext.inTransaction(center, connection -> {
                readFloor(connection, true);
                long now = databaseNow(connection);
                try (PreparedStatement expire = connection.prepareStatement("""
                        update mvcc_vector_reader_lease_v1
                        set expires_at = least(expires_at, ?)
                        where owner_id = ? and generation = ? and layout_hash = ?
                        """)) {
                    expire.setLong(1, now);
                    expire.setString(2, guard.ownerId);
                    expire.setLong(3, guard.generation);
                    expire.setString(4, layoutHash);
                    expire.executeUpdate();
                }
                return null;
            });
        } catch (final SQLException ex) {
            throw unavailable("release vector reader lease failed", ex);
        }
    }

    /**
     * Reads the centre list routinely with verify layout, algebra and non-negative vectors.
     *
     * @param connection connection
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Floor readFloor(final Connection connection, final boolean lock) throws SQLException {
        String expected = expectedIdentity;
        if (expected != null && !expected.equals(MysqlVectorDatabaseIdentity.read(connection))) {
            throw illegal("vector reader control identity changed before safe floor access");
        }
        String sql = "select layout_hash, decision_epoch, vector_bytes, generation from "
                + FLOOR_TABLE + " where singleton_id = 1" + (lock ? " for update" : "");
        try (
                PreparedStatement query = connection.prepareStatement(sql);
                ResultSet row = query.executeQuery()) {
            if (!row.next() || !layoutHash.equals(row.getString(1)) || row.getLong(4) < 1) {
                throw new SQLException("vector GC floor layout or generation is invalid");
            }
            return new Floor(new MysqlStripedSnapshotOracle.Snapshot(
                    row.getLong(2), decode(row.getBytes(3))), row.getLong(4));
        }
    }

    /**
     * Each path accepts only the same fixed layout and not earlier than the cut of an already collection lower boundary.
     *
     * @param candidate candidate
     * @param floor floor
     * @throws MvccException if the operation cannot complete
     */
    void requireAtLeast(final MysqlStripedSnapshotOracle.Snapshot candidate,
                        final MysqlStripedSnapshotOracle.Snapshot floor) {
        encode(candidate);
        if (candidate.decisionEpoch() < floor.decisionEpoch()) {
            throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                    "vector decision cut precedes GC floor", false, null);
        }
        for (String route : routes) {
            for (int stripe = 0; stripe < stripes; stripe++) {
                if (candidate.sequence(route, stripe) < floor.sequence(route, stripe)) {
                    throw new MvccException(ErrorCode.SNAPSHOT_TOO_OLD,
                            "vector data cut precedes GC floor", false, null);
                }
            }
        }
    }

    /**
     * GC takes the minimum value for each fraction of the current published upper bounds and all valid reader 's oldest sides.
     *
     * @param left left
     * @param right right
     * @return the operation result
     */
    MysqlStripedSnapshotOracle.Snapshot minimum(
                                                final MysqlStripedSnapshotOracle.Snapshot left,
                                                final MysqlStripedSnapshotOracle.Snapshot right) {
        Map<String, List<Long>> merged = new LinkedHashMap<>();
        for (String route : routes) {
            List<Long> components = new ArrayList<>(stripes);
            for (int stripe = 0; stripe < stripes; stripe++) {
                components.add(Math.min(left.sequence(route, stripe), right.sequence(route, stripe)));
            }
            merged.put(route, List.copyOf(components));
        }
        return new MysqlStripedSnapshotOracle.Snapshot(
                Math.min(left.decisionEpoch(), right.decisionEpoch()), merged);
    }

    /**
     * Vectors are coded in layout order, without Map going through order.
     *
     * @param snapshot snapshot
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private byte[] encode(final MysqlStripedSnapshotOracle.Snapshot snapshot) {
        ByteBuffer bytes = ByteBuffer.allocate(
                Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES));
        if (snapshot.localSequences().size() != routes.size()) {
            throw new IllegalArgumentException("vector reader snapshot route count differs from layout");
        }
        for (String route : routes) {
            List<Long> components = snapshot.localSequences().get(route);
            if (components == null || components.size() != stripes) {
                throw new IllegalArgumentException("vector reader snapshot has incomplete layout");
            }
            for (Long value : components) {
                if (value == null || value < 0) {
                    throw new IllegalArgumentException("vector reader snapshot has invalid sequence");
                }
                bytes.putLong(value);
            }
        }
        return bytes.array();
    }

    /**
     * Before decoding, verify byte length and each fraction, damage lease does not relax the lower level of safety.
     *
     * @param encoded encoded
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private Map<String, List<Long>> decode(final byte[] encoded) throws SQLException {
        int expected = Math.multiplyExact(Math.multiplyExact(routes.size(), stripes), Long.BYTES);
        if (encoded == null || encoded.length != expected) {
            throw new SQLException("vector reader lease bytes differ from layout");
        }
        ByteBuffer bytes = ByteBuffer.wrap(encoded);
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (String route : routes) {
            List<Long> components = new ArrayList<>(stripes);
            for (int stripe = 0; stripe < stripes; stripe++) {
                long value = bytes.getLong();
                if (value < 0) {
                    throw new SQLException("vector reader lease has negative sequence");
                }
                components.add(value);
            }
            result.put(route, List.copyOf(components));
        }
        return Map.copyOf(result);
    }

    /**
     * Initial GC bottom line is zero, failure reconstruction cannot skip the old reader from the current latest cut.
     *
     * @return the operation result
     */
    private MysqlStripedSnapshotOracle.Snapshot zeroSnapshot() {
        Map<String, List<Long>> values = new LinkedHashMap<>();
        for (String route : routes) {
            values.put(route, Collections.nCopies(stripes, 0L));
        }
        return new MysqlStripedSnapshotOracle.Snapshot(0, values);
    }

    /**
     * Database time is used to judge the durable lease duration common to all coordination processes.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long databaseNow(final Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                select cast(unix_timestamp(current_timestamp(3)) * 1000 as signed)
                """); ResultSet row = query.executeQuery()) {
            if (!row.next()) {
                throw new SQLException("vector reader database clock returned no row");
            }
            return row.getLong(1);
        }
    }

    /**
     * Process local service cutoff for database lease network and clock balance.
     *
     * @param startedNanos started nanos
     * @param leaseMillis lease millis
     * @return the operation result
     */
    private static long conservativeDeadline(final long startedNanos, final long leaseMillis) {
        long margin = Math.max(25, leaseMillis / 5);
        return Math.addExact(startedNanos,
                TimeUnit.MILLISECONDS.toNanos(leaseMillis - margin));
    }

    /**
     * Process identity and lease periods must be indexable and conservatively renewable.
     *
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireOwnerAndLease(final String ownerId, final long leaseMillis) {
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 128
                || leaseMillis < 200 || leaseMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector reader owner or lease duration");
        }
    }

    /**
     * The protocol stops reading and GC when the variable is not valid, rather than treating it as an retry SQL error.
     *
     * @param message message
     * @return the operation result
     */
    private static MvccException illegal(final String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, null);
    }

    /**
     * Database failed to close when providing durable fence.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unavailable(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * The algebra of the durable GC lower boundary only increases as it really advances.
     *
     * @param snapshot snapshot
     * @param generation generation
     */
    private record Floor(MysqlStripedSnapshotOracle.Snapshot snapshot, long generation) {
    }

    /**
     * The local lease guards in the same process do not allow other algebras to renew or continue their service.
     */
    final class Guard implements AutoCloseable {

        private final String ownerId;

        private final long generation;

        private final long leaseMillis;

        private final AtomicBoolean revoked = new AtomicBoolean();

        private final AtomicBoolean released = new AtomicBoolean();

        private volatile long validUntilNanos;

        /**
         * It can only be constructed from acquire that has successfully landed.
         *
         * @param ownerId owner id
         * @param generation generation
         * @param leaseMillis lease millis
         * @param validUntilNanos valid until nanos
         */
        private Guard(final String ownerId, final long generation, final long leaseMillis, final long validUntilNanos) {
            this.ownerId = ownerId;
            this.generation = generation;
            this.leaseMillis = leaseMillis;
            this.validUntilNanos = validUntilNanos;
        }

        /**
         * Returns the current durable algebra to recognize old processes after failure.
         *
         * @return the operation result
         */
        long generation() {
            return generation;
        }

        /**
         * Local By the end of the center lease, immediately stop the read snapshot service when it is disconnected.
         *
         * @throws MvccException if the operation cannot complete
         */
        void requireActive() {
            if (revoked.get() || System.nanoTime() - validUntilNanos >= 0) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "vector reader lease expired or was revoked", true, null);
            }
        }

        /**
         * The renewal of the lease must carry with it one of the oldest active aspects of the process at present.
         *
         * @param floor floor
         */
        synchronized void renew(final MysqlStripedSnapshotOracle.Snapshot floor) {
            MysqlVectorSnapshotLeaseStore.this.renew(this, floor);
        }

        /**
         * The publisher or reader process failure immediately rejects the local follow-up reading, and the centre lease automatically expires.
         */
        void revokeLocally() {
            revoked.set(true);
        }

        /**
         * Stop local service first, and then GC can no longer keep the old cut of this process.
         */
        @Override
        public synchronized void close() {
            revoked.set(true);
            if (released.compareAndSet(false, true)) {
                MysqlVectorSnapshotLeaseStore.this.release(this);
            }
        }
    }
}
