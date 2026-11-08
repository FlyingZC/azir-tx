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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The central lease experiment of the memory vector publisher prohibits the continuation of read snapshot and ACK after the old algebra has expired.
 */
final class MysqlVectorCutLeaderLease {

    private final DataSource center;

    private final String layoutHash;

    private final String leaseTable;

    /**
     * Ties the control library to a fixed route layout that prevents the successor from explaining another set of shard with the same algebra.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     */
    MysqlVectorCutLeaderLease(final DataSource center, final Map<String, DataSource> shards, final int stripes) {
        this(center, shards, stripes, false);
    }

    /**
     * The official transaction service uses a stand-alone durable lease table, which does not compete with the capacity experiment for leadership.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @return the operation result
     */
    static MysqlVectorCutLeaderLease forProduct(final DataSource center,
                                                final Map<String, DataSource> shards, final int stripes) {
        return new MysqlVectorCutLeaderLease(center, shards, stripes, true);
    }

    /**
     * Two sets of fixed table names share the same generation number and layout validate logic.
     *
     * @param center center
     * @param shards shards
     * @param stripes stripes
     * @param product product
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private MysqlVectorCutLeaderLease(final DataSource center, final Map<String, DataSource> shards,
                                      final int stripes, final boolean product) {
        this.center = Objects.requireNonNull(center, "center");
        Objects.requireNonNull(shards, "shards");
        List<String> routes = shards.keySet().stream().sorted().toList();
        if (routes.isEmpty() || stripes < 1 || stripes > 256) {
            throw new IllegalArgumentException("invalid vector leader route layout");
        }
        this.layoutHash = MysqlPublishedVectorCut.layoutFingerprint(routes, stripes);
        this.leaseTable = product ? "mvcc_vector_leader_v1" : "bench_striped_vector_leader";
    }

    /**
     * Run on the same durable lease namespace as a remote client.
     *
     * @return the operation result
     */
    boolean productMode() {
        return "mvcc_vector_leader_v1".equals(leaseTable);
    }

    /**
     * Only visible isolation experiments can recreate the lease table.
     *
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    void initializeIsolated() {
        if (!"bench_striped_vector_leader".equals(leaseTable)) {
            throw new IllegalStateException("product vector leader requires non-destructive initialization");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bench_striped_vector_leader");
            statement.execute("""
                    create table bench_striped_vector_leader (
                      singleton_id tinyint not null primary key,
                      owner_id varchar(128),
                      layout_hash char(64) not null,
                      generation bigint not null,
                      expires_at bigint not null
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into bench_striped_vector_leader
                      (singleton_id, owner_id, layout_hash, generation, expires_at)
                    values (1, null, ?, 0, 0)
                    """)) {
                insert.setString(1, layoutHash);
                if (insert.executeUpdate() != 1) {
                    throw new SQLException("initial vector leader row was not persisted");
                }
            }
        } catch (final SQLException ex) {
            throw unavailable("initialize vector leader lease failed", ex);
        }
    }

    /**
     * Formal lease initialization only fills a single case of missing and validate layout, without clearing the current effective leader.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    void initializeProduct() {
        if (!"mvcc_vector_leader_v1".equals(leaseTable)) {
            throw new IllegalStateException("benchmark vector leader requires isolated initialization");
        }
        try (Connection connection = center.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table if not exists mvcc_vector_leader_v1 (
                      singleton_id tinyint not null primary key,
                      owner_id varchar(128),
                      layout_hash char(64) not null,
                      generation bigint not null,
                      expires_at bigint not null
                    ) engine=InnoDB
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into mvcc_vector_leader_v1
                      (singleton_id, owner_id, layout_hash, generation, expires_at)
                    values (1, null, ?, 0, 0)
                    on duplicate key update singleton_id = singleton_id
                    """)) {
                insert.setString(1, layoutHash);
                insert.executeUpdate();
            }
            discover();
        } catch (final SQLException ex) {
            throw unavailable("initialize product vector leader lease failed", ex);
        }
    }

    /**
     * Lease rejects any new leader while still valid; expired succession atomic adds algebra.
     *
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @return the operation result
     */
    Optional<LeaseGuard> acquire(final String ownerId, final long leaseMillis) {
        requireIdentity(ownerId, leaseMillis);
        long requestStarted = System.nanoTime();
        try {
            Long generation = MysqlConnectionContext.inTransaction(center, connection -> {
                long previous;
                long expiry;
                long now;
                String durableLayout;
                String querySql = """
                        select layout_hash, generation, expires_at,
                               cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                        from %s where singleton_id = 1 for update
                        """.formatted(leaseTable);
                try (
                        PreparedStatement query = connection.prepareStatement(querySql);
                        ResultSet row = query.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("vector leader lease row is missing");
                    }
                    previous = row.getLong("generation");
                    expiry = row.getLong("expires_at");
                    now = row.getLong("now_millis");
                    durableLayout = row.getString("layout_hash");
                }
                if (!layoutHash.equals(durableLayout)) {
                    throw new SQLException("vector leader route layout differs from durable lease");
                }
                if (expiry > now) {
                    return null;
                }
                if (previous == Long.MAX_VALUE) {
                    throw new SQLException("vector leader generation exhausted");
                }
                String updateSql = """
                        update %s
                        set owner_id = ?, generation = ?, expires_at = ?
                        where singleton_id = 1 and layout_hash = ? and generation = ?
                        """.formatted(leaseTable);
                try (PreparedStatement update = connection.prepareStatement(updateSql)) {
                    update.setString(1, ownerId);
                    update.setLong(2, previous + 1);
                    update.setLong(3, Math.addExact(now, leaseMillis));
                    update.setString(4, layoutHash);
                    update.setLong(5, previous);
                    if (update.executeUpdate() != 1) {
                        throw new SQLException("vector leader generation changed during acquisition");
                    }
                }
                return previous + 1;
            });
            return generation == null ? Optional.empty()
                    : Optional.of(new LeaseGuard(center, leaseTable, ownerId, layoutHash,
                            generation, leaseMillis,
                            conservativeDeadline(requestStarted, leaseMillis)));
        } catch (final SQLException ex) {
            throw unavailable("acquire vector leader lease failed", ex);
        }
    }

    /**
     * Reads a side search center lease without modifying it, which failed to close when expired or the route layout was wrong.
     *
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    Optional<LeaderEndpoint> discover() {
        String querySql = """
                select owner_id, layout_hash, generation, expires_at,
                       cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                from %s where singleton_id = 1
                """.formatted(leaseTable);
        try (
                Connection connection = center.getConnection(); PreparedStatement query = connection.prepareStatement(querySql);
                ResultSet row = query.executeQuery()) {
            if (!row.next() || !layoutHash.equals(row.getString("layout_hash"))) {
                throw new SQLException("vector leader route layout is missing or changed");
            }
            String owner = row.getString("owner_id");
            long remaining = row.getLong("expires_at") - row.getLong("now_millis");
            if (owner == null || remaining <= 0) {
                return Optional.empty();
            }
            return Optional.of(new LeaderEndpoint(owner, row.getLong("generation"), remaining));
        } catch (final SQLException ex) {
            throw unavailable("discover vector leader failed", ex);
        }
    }

    /**
     * Limits the extraction values for lease identity, database duration and local safe balance.
     *
     * @param ownerId owner id
     * @param leaseMillis lease millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireIdentity(final String ownerId, final long leaseMillis) {
        if (ownerId == null || ownerId.isBlank() || ownerId.length() > 128
                || leaseMillis < 200 || leaseMillis > 60_000) {
            throw new IllegalArgumentException("invalid vector leader owner or lease duration");
        }
    }

    /**
     * Local Counts and leaves a balance before sending the request, prior to the expiration of database lease.
     *
     * @param requestStarted request started
     * @param leaseMillis lease millis
     * @return the operation result
     */
    private static long conservativeDeadline(final long requestStarted, final long leaseMillis) {
        long margin = Math.max(25, leaseMillis / 5);
        return Math.addExact(requestStarted, TimeUnit.MILLISECONDS.toNanos(leaseMillis - margin));
    }

    /**
     * Keeps the control face unreachable until it is closed and cannot be released by local surmise.
     *
     * @param message message
     * @param cause cause
     * @return the operation result
     */
    private static MvccException unavailable(final String message, final SQLException cause) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, message, true, null, cause);
    }

    /**
     * Client can only connection to the leader peer of the current unexpired algebra.
     *
     * @param ownerId owner id
     * @param generation generation
     * @param remainingMillis remaining millis
     */
    record LeaderEndpoint(String ownerId, long generation, long remainingMillis) {

        /**
         * Excludes empty addresses, uninitialized algebras and expired centres lease.
         *
         * @param ownerId owner id
         * @param generation generation
         * @param remainingMillis remaining millis
         * @throws IllegalArgumentException if the operation cannot complete
         */
        LeaderEndpoint {
            if (ownerId == null || ownerId.isBlank() || generation < 1 || remainingMillis < 1) {
                throw new IllegalArgumentException("invalid vector leader endpoint");
            }
        }
    }

    /**
     * Local of an algebra is extended only by the confirmed success centre lease.
     */
    static final class LeaseGuard {

        private final DataSource center;

        private final String leaseTable;

        private final String ownerId;

        private final String layoutHash;

        private final long generation;

        private final long leaseMillis;

        private final AtomicBoolean revoked = new AtomicBoolean();

        private volatile long validUntilNanos;

        /**
         * The Centre was created only successfully and did not accept any external forgery of algebras.
         *
         * @param center center
         * @param leaseTable lease table
         * @param ownerId owner id
         * @param layoutHash layout hash
         * @param generation generation
         * @param leaseMillis lease millis
         * @param validUntilNanos valid until nanos
         */
        private LeaseGuard(final DataSource center, final String leaseTable, final String ownerId,
                           final String layoutHash, final long generation,
                           final long leaseMillis, final long validUntilNanos) {
            this.center = center;
            this.leaseTable = leaseTable;
            this.ownerId = ownerId;
            this.layoutHash = layoutHash;
            this.generation = generation;
            this.leaseMillis = leaseMillis;
            this.validUntilNanos = validUntilNanos;
        }

        /**
         * Returns the algebra that the client must carry for its response.
         *
         * @return the operation result
         */
        long generation() {
            return generation;
        }

        /**
         * The publisher can only explain that the same shard layout is written to the centre when lease is obtained.
         *
         * @return the operation result
         */
        String layoutHash() {
            return layoutHash;
        }

        /**
         * The cut-off source must be consistent with the lease table at the centre where algebras are obtained.
         *
         * @return the operation result
         */
        boolean productMode() {
            return "mvcc_vector_leader_v1".equals(leaseTable);
        }

        /**
         * Check local for a conservative deadline before publishing, read snapshot and confirming.
         *
         * @throws MvccException if the operation cannot complete
         */
        void requireActive() {
            if (revoked.get() || remainingNanos() <= 0) {
                throw new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "vector leader lease expired or was revoked", true, null);
            }
        }

        /**
         * The waiting ACK exits in time with this cut-off instead of waiting for complete transaction timeout.
         *
         * @return the operation result
         */
        long remainingNanos() {
            return revoked.get() ? 0 : validUntilNanos - System.nanoTime();
        }

        /**
         * The local deadline can only be extended by the same algebra and unexpired centre line.
         */
        void renew() {
            requireActive();
            long requestStarted = System.nanoTime();
            try {
                MysqlConnectionContext.inTransaction(center, connection -> {
                    String currentOwner;
                    String currentLayout;
                    long currentGeneration;
                    long expiry;
                    long now;
                    String querySql = """
                            select owner_id, layout_hash, generation, expires_at,
                                   cast(unix_timestamp(current_timestamp(3)) * 1000 as signed) as now_millis
                            from %s where singleton_id = 1 for update
                            """.formatted(leaseTable);
                    try (
                            PreparedStatement query = connection.prepareStatement(querySql);
                            ResultSet row = query.executeQuery()) {
                        if (!row.next()) {
                            throw new SQLException("vector leader lease row is missing");
                        }
                        currentOwner = row.getString("owner_id");
                        currentLayout = row.getString("layout_hash");
                        currentGeneration = row.getLong("generation");
                        expiry = row.getLong("expires_at");
                        now = row.getLong("now_millis");
                    }
                    if (!ownerId.equals(currentOwner) || !layoutHash.equals(currentLayout)
                            || currentGeneration != generation
                            || expiry <= now) {
                        throw new SQLException("vector leader lease was superseded or expired");
                    }
                    String updateSql = """
                            update %s set expires_at = ?
                            where singleton_id = 1 and owner_id = ?
                              and layout_hash = ? and generation = ?
                            """.formatted(leaseTable);
                    try (PreparedStatement update = connection.prepareStatement(updateSql)) {
                        update.setLong(1, Math.addExact(now, leaseMillis));
                        update.setString(2, ownerId);
                        update.setString(3, layoutHash);
                        update.setLong(4, generation);
                        if (update.executeUpdate() != 1) {
                            throw new SQLException("vector leader lease renewal lost generation");
                        }
                    }
                    return null;
                });
                validUntilNanos = conservativeDeadline(requestStarted, leaseMillis);
            } catch (final SQLException ex) {
                revoked.set(true);
                throw unavailable("renew vector leader lease failed", ex);
            }
        }

        /**
         * Follow-up reading and writing is refused immediately upon the voluntary exit of this process, and Centre lease still waits until its natural expiry.
         */
        void revokeLocally() {
            revoked.set(true);
        }
    }
}
