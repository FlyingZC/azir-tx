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

package com.flyingzc.azir.e2e;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.storage.mysql.MysqlDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Segregation of the segment slot in database with a continuous release performance prototype; complete transaction recovery and multi-coordinate switch is still not achieved.
 */
public final class MysqlSegmentPublicationPrototype {

    private final HikariDataSource source;

    private final int segmentSize;

    private final boolean metadataMode;

    private final long leaseMillis;

    private final String ownerId;

    private final AtomicLong published = new AtomicLong();

    private final LongAdder reserved = new LongAdder();

    private final LongAdder sealedSegments = new LongAdder();

    private final LongAdder sealedSlots = new LongAdder();

    private final AtomicInteger deciding = new AtomicInteger();

    private long next = 1;

    private long end;

    private boolean closing;

    /**
     * Saves an independent database resource with a fixed segment size.
     *
     * @param source database connection source
     * @param segmentSize segment size
     * @param metadataMode metadata mode
     * @param leaseMillis lease millis
     */
    private MysqlSegmentPublicationPrototype(final HikariDataSource source, final int segmentSize,
                                             final boolean metadataMode, final long leaseMillis) {
        this(source, segmentSize, metadataMode, leaseMillis, UUID.randomUUID().toString());
    }

    /**
     * Failure probe can carry old owner identity verify expired and sealed fencing.
     *
     * @param source database connection source
     * @param segmentSize segment size
     * @param metadataMode metadata mode
     * @param leaseMillis lease millis
     * @param ownerId owner id
     */
    private MysqlSegmentPublicationPrototype(final HikariDataSource source, final int segmentSize,
                                             final boolean metadataMode, final long leaseMillis, final String ownerId) {
        this.source = source;
        this.segmentSize = segmentSize;
        this.metadataMode = metadataMode;
        this.leaseMillis = leaseMillis;
        this.ownerId = ownerId;
    }

    /**
     * Runs the prototype under a visible segregation reset switch, and the output has been published to confirm throughput and tail delay.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        boolean tailProbe = Boolean.parseBoolean(System.getenv().getOrDefault("AZIR_TX_PROTO_TAIL_PROBE", "false"));
        if (!Boolean.parseBoolean(System.getenv().getOrDefault("AZIR_TX_RESET", "false"))) {
            throw new IllegalArgumentException("prototype requires AZIR_TX_RESET=true");
        }
        int concurrency = Integer.parseInt(System.getenv().getOrDefault("AZIR_TX_BENCH_CONCURRENCY", "50"));
        int coordinators = Integer.parseInt(System.getenv().getOrDefault("AZIR_TX_PROTO_COORDINATORS", "1"));
        boolean crashOne = Boolean.parseBoolean(System.getenv().getOrDefault("AZIR_TX_PROTO_CRASH_ONE", "false"));
        int duration = Integer.parseInt(System.getenv().getOrDefault("AZIR_TX_BENCH_DURATION_SECONDS", "30"));
        int segment = Integer.parseInt(System.getenv().getOrDefault("AZIR_TX_SEGMENT_SIZE", "32"));
        long leaseMillis = Long.parseLong(System.getenv().getOrDefault("AZIR_TX_SEGMENT_LEASE_MS", "5000"));
        if (segment < 1 || segment > 4096 || leaseMillis < 1 || leaseMillis > 86_400_000
                || coordinators < 1 || coordinators > concurrency || crashOne && coordinators < 2) {
            throw new IllegalArgumentException("invalid segment or lease limit");
        }
        boolean metadataMode = Boolean.parseBoolean(System.getenv().getOrDefault("AZIR_TX_PROTO_METADATA", "false"));
        String url = System.getenv().getOrDefault("AZIR_TX_CONTROL_JDBC", "jdbc:mysql://127.0.0.1:23306/control");
        try (
                var source = (HikariDataSource) MysqlDataSourceFactory.create(url, System.getenv().getOrDefault("AZIR_TX_USERNAME", "root"),
                        System.getenv().getOrDefault("AZIR_TX_PASSWORD", "root"))) {
            var benchmark = new MysqlSegmentPublicationPrototype(source, segment, metadataMode, leaseMillis);
            if (tailProbe) {
                benchmark.probeTail();
            } else {
                benchmark.initialize();
                benchmark.run(concurrency, duration, coordinators, crashOne);
            }
        }
    }

    /**
     * Reset only the special prototype table and do not touch the actual transaction account book.
     *
     * @throws Exception if the operation cannot complete
     */
    private void initialize() throws Exception {
        try (Connection connection = source.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bench_segment_slot");
            statement.execute("drop table if exists bench_segment_txn");
            statement.execute("drop table if exists bench_segment_owner");
            statement.execute("drop table if exists bench_segment_state");
            statement.execute("create table if not exists bench_segment_state (id tinyint primary key, next_ts bigint not null, published_ts bigint not null) engine=InnoDB");
            statement.execute(
                    "create table if not exists bench_segment_slot (commit_ts bigint primary key, tx_id bigint unique, status varchar(16) not null, expires_at bigint not null) engine=InnoDB");
            statement.execute("create table if not exists bench_segment_txn (tx_id bigint primary key, status varchar(16) not null, commit_ts bigint null) engine=InnoDB");
            statement.execute(
                    "create table if not exists bench_segment_owner (segment_start bigint primary key, segment_end bigint not null, owner_id varchar(36) not null, generation bigint not null, expires_at bigint not null, status varchar(16) not null, index bench_owner_expiry(status,expires_at,segment_start), index bench_owner_heartbeat(owner_id,status,segment_end)) engine=InnoDB");
            statement.executeUpdate("insert into bench_segment_state(id,next_ts,published_ts) values (1,0,0) on duplicate key update next_ts=0,published_ts=0");
        }
    }

    /**
     * Locks the global status line once only when the previous part is exhausted, and durable creates all slots.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private synchronized long allocate() throws Exception {
        if (closing) {
            return -1;
        }
        if (next <= end) {
            deciding.incrementAndGet();
            return next++;
        }
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try (
                    PreparedStatement state = connection.prepareStatement("select next_ts from bench_segment_state where id=1 for update");
                    ResultSet row = state.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("segment state missing");
                }
                long lower = row.getLong(1);
                try (
                        PreparedStatement update = connection.prepareStatement("update bench_segment_state set next_ts=? where id=1");
                        PreparedStatement insert = connection.prepareStatement("insert into bench_segment_slot(commit_ts,tx_id,status,expires_at) values (?,null,'PENDING',?)");
                        PreparedStatement owner =
                                connection.prepareStatement("insert into bench_segment_owner(segment_start,segment_end,owner_id,generation,expires_at,status) values (?,?,?,1,?,'ACTIVE')");
                        PreparedStatement clock = connection.prepareStatement("select cast(unix_timestamp(current_timestamp(3))*1000 as signed)");
                        ResultSet now = clock.executeQuery()) {
                    now.next();
                    long deadline = now.getLong(1) + leaseMillis;
                    update.setLong(1, lower + segmentSize);
                    update.executeUpdate();
                    owner.setLong(1, lower + 1);
                    owner.setLong(2, lower + segmentSize);
                    owner.setString(3, ownerId);
                    owner.setLong(4, deadline);
                    owner.executeUpdate();
                    for (int index = 1; index <= segmentSize; index++) {
                        insert.setLong(1, lower + index);
                        insert.setLong(2, deadline);
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
                connection.commit();
                next = lower + 2;
                end = lower + segmentSize;
                reserved.add(segmentSize);
                deciding.incrementAndGet();
                return lower + 1;
            } catch (final Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Simulates a centre transaction record that was registered before prewrite.
     *
     * @param commitTs durable commit timestamp
     * @throws Exception if the operation cannot complete
     */
    private void createMetadata(final long commitTs) throws Exception {
        try (
                Connection connection = source.getConnection();
                PreparedStatement insert = connection.prepareStatement("insert into bench_segment_txn(tx_id,status,commit_ts) values (?,'ACTIVE',null)")) {
            insert.setLong(1, commitTs);
            insert.executeUpdate();
        }
    }

    /**
     * Decision Locks the durable own algebra first, then corresponds to the centre metadata with the transaction commit slot terminal state.
     *
     * @param commitTs durable commit timestamp
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private void decide(final long commitTs) throws Exception {
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                long segmentStart = ((commitTs - 1) / segmentSize) * segmentSize + 1;
                long expiresAt;
                try (PreparedStatement lease = connection.prepareStatement("select owner_id,generation,expires_at,status from bench_segment_owner where segment_start=? for share")) {
                    lease.setLong(1, segmentStart);
                    try (ResultSet row = lease.executeQuery()) {
                        if (!row.next() || !ownerId.equals(row.getString("owner_id"))
                                || row.getLong("generation") != 1 || !"ACTIVE".equals(row.getString("status"))) {
                            throw new IllegalStateException("segment owner was fenced: " + segmentStart);
                        }
                        expiresAt = row.getLong("expires_at");
                    }
                }
                try (
                        PreparedStatement clock = connection.prepareStatement("select cast(unix_timestamp(current_timestamp(3))*1000 as signed)");
                        ResultSet now = clock.executeQuery()) {
                    now.next();
                    if (expiresAt <= now.getLong(1)) {
                        throw new IllegalStateException("segment owner lease expired: " + segmentStart);
                    }
                }
                try (PreparedStatement update = connection.prepareStatement("update bench_segment_slot set tx_id=?,status='COMMITTED' where commit_ts=? and status='PENDING'")) {
                    update.setLong(1, commitTs);
                    update.setLong(2, commitTs);
                    if (update.executeUpdate() != 1) {
                        throw new IllegalStateException("segment slot already decided or expired: " + commitTs);
                    }
                    if (metadataMode) {
                        try (PreparedStatement metadata = connection.prepareStatement("update bench_segment_txn set status='COMMITTED',commit_ts=? where tx_id=? and status='ACTIVE'")) {
                            metadata.setLong(1, commitTs);
                            metadata.setLong(2, commitTs);
                            if (metadata.executeUpdate() != 1) {
                                throw new IllegalStateException("segment metadata missing: " + commitTs);
                            }
                        }
                    }
                }
                connection.commit();
            } catch (final Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Single publisher only crosses the continuous terminal state prefix and writes new watermark durable back to the control line.
     *
     * @throws Exception if the operation cannot complete
     */
    private void publish() throws Exception {
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try (
                    PreparedStatement state = connection.prepareStatement("select published_ts from bench_segment_state where id=1 for update");
                    ResultSet row = state.executeQuery()) {
                row.next();
                long current = row.getLong(1);
                long cursor = current;
                try (PreparedStatement slots = connection.prepareStatement("select commit_ts,status from bench_segment_slot where commit_ts>? order by commit_ts limit 512")) {
                    slots.setLong(1, current);
                    try (ResultSet entries = slots.executeQuery()) {
                        while (entries.next()) {
                            if (entries.getLong(1) != cursor + 1 || "PENDING".equals(entries.getString(2))) {
                                break;
                            }
                            cursor++;
                        }
                    }
                }
                if (cursor > current) {
                    try (PreparedStatement update = connection.prepareStatement("update bench_segment_state set published_ts=? where id=1")) {
                        update.setLong(1, cursor);
                        update.executeUpdate();
                    }
                }
                connection.commit();
                published.set(cursor);
            } catch (final Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Lease Expires before segregating fencing sub-section, and then atomic seal pending tailings.
     *
     * @param segmentStart segment start
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private int sealExpiredSegment(final long segmentStart) throws Exception {
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                long segmentEnd;
                long expiresAt;
                try (PreparedStatement owner = connection.prepareStatement("select segment_end,expires_at,status from bench_segment_owner where segment_start=? for update")) {
                    owner.setLong(1, segmentStart);
                    try (ResultSet row = owner.executeQuery()) {
                        if (!row.next() || !"ACTIVE".equals(row.getString("status"))) {
                            throw new IllegalStateException("segment cannot be fenced: " + segmentStart);
                        }
                        segmentEnd = row.getLong("segment_end");
                        expiresAt = row.getLong("expires_at");
                    }
                }
                try (
                        PreparedStatement clock = connection.prepareStatement("select cast(unix_timestamp(current_timestamp(3))*1000 as signed)");
                        ResultSet now = clock.executeQuery()) {
                    now.next();
                    if (expiresAt > now.getLong(1)) {
                        throw new IllegalStateException("segment owner lease is still active: " + segmentStart);
                    }
                }
                try (
                        PreparedStatement fence = connection.prepareStatement("update bench_segment_owner set status='SEALED',generation=generation+1 where segment_start=?");
                        PreparedStatement abort = connection.prepareStatement("update bench_segment_slot set status='ABORTED' where commit_ts>=? and commit_ts<=? and status='PENDING'")) {
                    fence.setLong(1, segmentStart);
                    fence.executeUpdate();
                    abort.setLong(1, segmentStart);
                    abort.setLong(2, segmentEnd);
                    int sealed = abort.executeUpdate();
                    connection.commit();
                    return sealed;
                }
            } catch (final Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * The normal stoppage prevents new distributions and waits for the end of the journey decision, before actively sealing the current slot.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private synchronized int sealOwnedTail() throws Exception {
        closing = true;
        while (deciding.get() > 0) {
            Thread.sleep(1);
        }
        if (end == 0 || next > end) {
            return 0;
        }
        long segmentStart = end - segmentSize + 1;
        try (Connection connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement owner = connection.prepareStatement("select owner_id,status from bench_segment_owner where segment_start=? for update")) {
                    owner.setLong(1, segmentStart);
                    try (ResultSet row = owner.executeQuery()) {
                        if (!row.next() || !ownerId.equals(row.getString("owner_id"))) {
                            throw new IllegalStateException("closing coordinator no longer owns segment");
                        }
                        if (!"ACTIVE".equals(row.getString("status"))) {
                            connection.rollback();
                            return 0;
                        }
                    }
                }
                try (
                        PreparedStatement fence = connection.prepareStatement("update bench_segment_owner set status='SEALED',generation=generation+1 where segment_start=?");
                        PreparedStatement abort = connection.prepareStatement("update bench_segment_slot set status='ABORTED' where commit_ts>=? and commit_ts<=? and status='PENDING'")) {
                    fence.setLong(1, segmentStart);
                    fence.executeUpdate();
                    abort.setLong(1, segmentStart);
                    abort.setLong(2, end);
                    int sealed = abort.executeUpdate();
                    connection.commit();
                    return sealed;
                }
            } catch (final Exception failure) {
                connection.rollback();
                throw failure;
            }
        }
    }

    /**
     * Background closes the unused slot without blocking an owner that still holds valid lease with bounded batch.
     *
     * @param limit maximum number of items
     * @throws Exception if the operation cannot complete
     */
    private void recoverExpiredSegments(final int limit) throws Exception {
        List<Long> expired = new ArrayList<>(limit);
        try (
                Connection connection = source.getConnection();
                PreparedStatement query = connection.prepareStatement(
                        "select segment_start from bench_segment_owner where status='ACTIVE' and expires_at<=cast(unix_timestamp(current_timestamp(3))*1000 as signed) order by expires_at,segment_start limit ?")) {
            query.setInt(1, limit);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    expired.add(rows.getLong(1));
                }
            }
        }
        for (long segmentStart : expired) {
            try {
                sealedSlots.add(sealExpiredSegment(segmentStart));
                sealedSegments.increment();
            } catch (final IllegalStateException ignored) {
                // Other receivers may have completed fencing and the next scan in this round will skip terminal state.
            }
        }
    }

    /**
     * The heart beat has not been extended beyond the release of this owner sub-section watermark, and is not renewed.
     *
     * @param publishedTs published ts
     * @throws Exception if the operation cannot complete
     */
    private void renewUnpublishedSegments(final long publishedTs) throws Exception {
        try (
                Connection connection = source.getConnection();
                PreparedStatement renew = connection.prepareStatement("""
                        update bench_segment_owner
                        set expires_at = cast(unix_timestamp(current_timestamp(3))*1000 as signed) + ?
                        where owner_id = ? and status = 'ACTIVE' and segment_end > ?
                          and expires_at > cast(unix_timestamp(current_timestamp(3))*1000 as signed)
                        """)) {
            renew.setLong(1, leaseMillis);
            renew.setString(2, ownerId);
            renew.setLong(3, publishedTs);
            renew.executeUpdate();
        }
    }

    /**
     * Simulates a new segment after the original coordinator exit is blocked by the old tailing slot and then the expired receiver fencing concurrent cloth.
     *
     * @throws Exception if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private void probeTail() throws Exception {
        long before;
        long oldUpper;
        try (
                Connection connection = source.getConnection();
                PreparedStatement query = connection.prepareStatement("select published_ts,next_ts from bench_segment_state where id=1");
                ResultSet row = query.executeQuery()) {
            row.next();
            before = row.getLong(1);
            oldUpper = row.getLong(2);
        }
        if (oldUpper == before) {
            var abandoned = new MysqlSegmentPublicationPrototype(source, segmentSize, metadataMode, leaseMillis);
            abandoned.allocate();
            oldUpper += segmentSize;
        }
        long later = allocate();
        if (metadataMode) {
            createMetadata(later);
        }
        decide(later);
        publish();
        long blocked = published.get();
        long oldStart = oldUpper - segmentSize + 1;
        long waitDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(leaseMillis + 2000);
        while (System.nanoTime() < waitDeadline) {
            try (
                    Connection connection = source.getConnection();
                    PreparedStatement lease =
                            connection.prepareStatement("select expires_at,cast(unix_timestamp(current_timestamp(3))*1000 as signed) from bench_segment_owner where segment_start=?")) {
                lease.setLong(1, oldStart);
                try (ResultSet row = lease.executeQuery()) {
                    if (row.next() && row.getLong(1) <= row.getLong(2)) {
                        break;
                    }
                }
            }
            Thread.sleep(20);
        }
        String oldOwner;
        try (
                Connection connection = source.getConnection();
                PreparedStatement owner = connection.prepareStatement("select owner_id from bench_segment_owner where segment_start=?")) {
            owner.setLong(1, oldStart);
            try (ResultSet row = owner.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("old segment owner missing");
                }
                oldOwner = row.getString(1);
            }
        }
        var stale = new MysqlSegmentPublicationPrototype(source, segmentSize, metadataMode, leaseMillis, oldOwner);
        boolean expiredFenced = false;
        try {
            stale.decide(before + 1);
        } catch (final IllegalStateException expected) {
            expiredFenced = expected.getMessage().contains("lease expired");
        }
        int sealed = sealExpiredSegment(oldStart);
        boolean sealedFenced = false;
        try {
            stale.decide(before + 1);
        } catch (final IllegalStateException expected) {
            sealedFenced = expected.getMessage().contains("fenced");
        }
        publish();
        System.out.printf("SEGMENT_TAIL_PROBE old_published=%d old_upper=%d later=%d blocked_at=%d sealed=%d after_seal=%d expired_fenced=%s sealed_fenced=%s%n",
                before, oldUpper, later, blocked, sealed, published.get(), expiredFenced, sealedFenced);
        if (blocked != before || published.get() < later || !expiredFenced || !sealedFenced) {
            throw new IllegalStateException("segment tail publication invariant failed");
        }
    }

    /**
     * Fixed concurrent lasts commit and waits for the durable release to be recorded successfully.
     *
     * @param concurrency concurrency
     * @param durationSeconds duration seconds
     * @param coordinatorCount coordinator count
     * @param crashOne crash one
     * @throws Exception if the operation cannot complete
     */
    private void run(final int concurrency, final int durationSeconds, final int coordinatorCount, final boolean crashOne) throws Exception {
        var running = new AtomicBoolean(true);
        var crashed = new AtomicBoolean();
        var failures = new LongAdder();
        var committed = new LongAdder();
        var abandoned = new LongAdder();
        var latencies = new ConcurrentLinkedQueue<Long>();
        var firstFailure = new java.util.concurrent.atomic.AtomicReference<String>();
        var gracefulSealed = new LongAdder();
        List<MysqlSegmentPublicationPrototype> coordinators = new ArrayList<>(coordinatorCount);
        coordinators.add(this);
        for (int index = 1; index < coordinatorCount; index++) {
            coordinators.add(new MysqlSegmentPublicationPrototype(source, segmentSize, metadataMode, leaseMillis));
        }
        Thread publisher = Thread.ofPlatform().daemon().name("segment-prototype-publisher").start(() -> {
            while (running.get()) {
                try {
                    publish();
                    Thread.sleep(1);
                } catch (final Exception failure) {
                    firstFailure.compareAndSet(null, failure.toString());
                    failures.increment();
                    running.set(false);
                }
            }
        });
        Thread reaper = Thread.ofPlatform().daemon().name("segment-prototype-reaper").start(() -> {
            while (running.get()) {
                try {
                    recoverExpiredSegments(32);
                    Thread.sleep(100);
                } catch (final Exception failure) {
                    firstFailure.compareAndSet(null, failure.toString());
                    failures.increment();
                    running.set(false);
                }
            }
        });
        Thread heartbeat = Thread.ofPlatform().daemon().name("segment-prototype-heartbeat").start(() -> {
            while (running.get()) {
                try {
                    for (int index = 0; index < coordinators.size(); index++) {
                        if (!crashOne || index != 0 || !crashed.get()) {
                            coordinators.get(index).renewUnpublishedSegments(published.get());
                        }
                    }
                    Thread.sleep(Math.max(50, leaseMillis / 3));
                } catch (final Exception failure) {
                    firstFailure.compareAndSet(null, failure.toString());
                    failures.increment();
                    running.set(false);
                }
            }
        });
        var ready = new CountDownLatch(concurrency);
        var start = new CountDownLatch(1);
        var stopAt = new AtomicLong();
        var crashAt = new AtomicLong();
        Thread crashTimer = Thread.ofPlatform().daemon().name("segment-prototype-owner-crash").start(() -> {
            try {
                start.await();
                if (crashOne) {
                    while (running.get() && System.nanoTime() < crashAt.get()) {
                        java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                    }
                    crashed.set(true);
                }
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
            }
        });
        Thread gracefulStop = Thread.ofPlatform().daemon().name("segment-prototype-graceful-stop").start(() -> {
            try {
                start.await();
                while (running.get() && System.nanoTime() < stopAt.get()) {
                    java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
                }
                if (running.get()) {
                    for (int index = 0; index < coordinators.size(); index++) {
                        if (!crashOne || index != 0) {
                            gracefulSealed.add(coordinators.get(index).sealOwnedTail());
                        }
                    }
                }
            } catch (final Exception failure) {
                firstFailure.compareAndSet(null, failure.toString());
                failures.increment();
                running.set(false);
            }
        });
        long started = System.nanoTime();
        try (var workers = Executors.newFixedThreadPool(concurrency)) {
            for (int index = 0; index < concurrency; index++) {
                int ownerIndex = index % coordinatorCount;
                MysqlSegmentPublicationPrototype coordinator = coordinators.get(ownerIndex);
                workers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        long stop = stopAt.get();
                        while (System.nanoTime() < stop && running.get()
                                && !(crashOne && ownerIndex == 0 && crashed.get())) {
                            long begun = System.nanoTime();
                            try {
                                long commitTs = coordinator.allocate();
                                if (commitTs < 0) {
                                    break;
                                }
                                try {
                                    if (metadataMode) {
                                        coordinator.createMetadata(commitTs);
                                    }
                                    coordinator.decide(commitTs);
                                } finally {
                                    coordinator.deciding.decrementAndGet();
                                }
                                long waitDeadline = System.nanoTime()
                                        + TimeUnit.MILLISECONDS.toNanos(leaseMillis * 2);
                                while (published.get() < commitTs && running.get()
                                        && !(crashOne && ownerIndex == 0 && crashed.get())
                                        && System.nanoTime() < waitDeadline) {
                                    java.util.concurrent.locks.LockSupport.parkNanos(100_000);
                                }
                                if (crashOne && ownerIndex == 0 && crashed.get()) {
                                    if (published.get() < commitTs) {
                                        abandoned.increment();
                                    }
                                    break;
                                }
                                if (published.get() < commitTs) {
                                    throw new IllegalStateException("segment publication blocked at "
                                            + published.get() + " before " + commitTs);
                                }
                                committed.increment();
                                latencies.add(System.nanoTime() - begun);
                            } catch (final Exception failure) {
                                firstFailure.compareAndSet(null, failure.toString());
                                failures.increment();
                                running.set(false);
                            }
                        }
                    } catch (final InterruptedException failure) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            ready.await();
            started = System.nanoTime();
            stopAt.set(started + TimeUnit.SECONDS.toNanos(durationSeconds));
            crashAt.set(started + TimeUnit.SECONDS.toNanos(durationSeconds / 2));
            start.countDown();
        } finally {
            running.set(false);
            start.countDown();
            crashTimer.join(5000);
            gracefulStop.join(5000);
            publisher.join(5000);
            reaper.join(5000);
            heartbeat.join(5000);
        }
        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();
        double p99 = sorted.length == 0 ? Double.NaN : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.99) - 1)] / 1_000_000.0;
        double p999 = sorted.length == 0 ? Double.NaN : sorted[Math.max(0, (int) Math.ceil(sorted.length * 0.999) - 1)] / 1_000_000.0;
        double maximum = sorted.length == 0 ? Double.NaN : sorted[sorted.length - 1] / 1_000_000.0;
        long reservedSlots = coordinators.stream().mapToLong(value -> value.reserved.sum()).sum();
        System.out.printf(
                "SEGMENT_PROTOTYPE concurrency=%d coordinators=%d crashed_one=%s duration_s=%.2f segment=%d metadata=%s committed=%d failures=%d abandoned=%d tps=%.2f p99_ms=%.2f p999_ms=%.2f max_ms=%.2f reserved=%d published=%d sealed_segments=%d sealed_slots=%d graceful_slots=%d first_failure=%s%n",
                concurrency, coordinatorCount, crashOne, seconds, segmentSize, metadataMode,
                committed.sum(), failures.sum(), abandoned.sum(),
                committed.sum() / seconds, p99, p999, maximum, reservedSlots, published.get(),
                sealedSegments.sum(), sealedSlots.sum(), gracefulSealed.sum(),
                firstFailure.get() == null ? "-" : firstFailure.get());
    }
}
