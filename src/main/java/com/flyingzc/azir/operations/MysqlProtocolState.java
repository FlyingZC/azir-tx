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

package com.flyingzc.azir.operations;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Durable Run generation and shard write fence; the late arrival of SQL is protected by local watermark after session has failed.
 */
public final class MysqlProtocolState {

    private static final ReentrantLock[] ADMISSION = new ReentrantLock[256];

    private static final ThreadLocal<String> ADMIN = new ThreadLocal<>();

    static {
        Arrays.setAll(ADMISSION, ignored -> new ReentrantLock(true));
    }

    private final String prefix;

    private final String namespace;

    private final long generation;

    private final DataSource control;

    private final Map<String, DataSource> shards;

    private final ThreadLocal<ControlOwner> controlOwner = new ThreadLocal<>();

    /**
     * Initialize version -II run fence; generation fixes the current object and recovery must recreate the client.
     *
     * @param prefix prefix
     * @param namespace persistent protocol namespace
     * @param control control
     * @param shards shards
     */
    public MysqlProtocolState(final String prefix, final String namespace, final DataSource control, final Map<String, DataSource> shards) {
        this(prefix, namespace, control, shards, true);
    }

    /**
     * Read-only Loads are managed to allow tools to fix local generation switches without initialization.
     *
     * @param prefix prefix
     * @param namespace persistent protocol namespace
     * @param control control
     * @param shards shards
     * @return the operation result
     */
    public static MysqlProtocolState existing(final String prefix, final String namespace, final DataSource control, final Map<String, DataSource> shards) {
        return new MysqlProtocolState(prefix, namespace, control, shards, false);
    }

    /**
     * Operation start-up allows controlled upgrades, and only the original fence is loaded with a D.D.
     *
     * @param prefix prefix
     * @param namespace persistent protocol namespace
     * @param control control
     * @param shards shards
     * @param initialize initialize
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private MysqlProtocolState(final String prefix, final String namespace, final DataSource control, final Map<String, DataSource> shards, final boolean initialize) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix)) {
            throw new IllegalArgumentException("invalid protocol");
        }
        this.prefix = prefix;
        this.namespace = namespace;
        this.control = control;
        this.shards = Map.copyOf(shards);
        this.shards.keySet().forEach(id -> {
            if (!id.matches("[A-Za-z0-9_-]{1,64}")) {
                throw new IllegalArgumentException("invalid shard id");
            }
        });
        String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
        boolean initialized = !initialize || operationsInitialized(config);
        if (initialize) {
            initialize(control, "control", initialized);
        }
        Status status = read(control, "control", false);
        generation = status.generation();
        if (initialize) {
            shards.forEach((id, source) -> initialize(source, "shard:" + id, initialized));
        }
        for (var entry : shards.entrySet()) {
            Status data = read(entry.getValue(), "shard:" + entry.getKey(), false);
            if ((status.mode().equals("ACTIVE") && data.generation() != generation) || (!data.namespace().equals(namespace) && !namespaceRepairAllowed(data.namespace()))) {
                throw new IllegalStateException("incomplete protocol generation transition; use administration recovery");
            }
        }
        if (initialize) {
            try (var c = control.getConnection(); var q = c.createStatement()) {
                q.executeUpdate("update " + config + " set operations_version=2 where id=1");
            } catch (final SQLException failure) {
                throw storage(failure);
            }
        }
    }

    /**
     * Returns the protocol prefix.
     *
     * @return the operation result
     */
    public String prefix() {
        return prefix;
    }

    /**
     * Returns durable namespace.
     *
     * @return the operation result
     */
    public String namespace() {
        return namespace;
    }

    /**
     * Returns a fixed run generation when creating the client.
     *
     * @return the operation result
     */
    public long generation() {
        return generation;
    }

    /**
     * Returns the controlled connection pool used by this object.
     *
     * @return the operation result
     */
    public DataSource control() {
        return control;
    }

    /**
     * Returns the immutable shard route.
     *
     * @return the operation result
     */
    public Map<String, DataSource> shards() {
        return shards;
    }

    /**
     * Returns the database name lock that corresponds to normal commit.
     *
     * @return the operation result
     */
    public String gateName() {
        return "azir:" + prefix + ":" + namespace;
    }

    /**
     * Get group replication control owner token; common fixed example relies on the original name lock and returns empty resource.
     *
     * @param connection connection
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public ControlScope enterControl(final Connection connection) throws SQLException {
        if (controlOwner.get() != null) {
            throw new IllegalStateException("nested control scope must reuse its owner");
        }
        if (!connection.getAutoCommit()) {
            throw new SQLException("control owner requires an auto-commit authority connection");
        }
        try (var q = connection.prepareStatement("select is_used_lock(?)=connection_id()")) {
            q.setString(1, gateName());
            try (var r = q.executeQuery()) {
                if (!r.next() || !r.getBoolean(1)) {
                    throw new SQLException("control owner requires the original named lock");
                }
            }
        }
        requireControl(connection);
        if (!grouped(control)) {
            return null;
        }
        ControlOwner owner = new ControlOwner(lastPulse(connection), connection);
        ControlScope scope = new ControlScope(owner);
        try {
            controlOwner.set(owner);
            return scope;
        } catch (final RuntimeException | Error failure) {
            controlOwner.remove();
            throw failure;
        }
    }

    /**
     * Rollback or commit closes the original area after it is unknown, and must exit the door-ban for further query results and cannot be reused with a temporary pulse.
     */
    public void invalidateControl() {
        ControlOwner owner = controlOwner.get();
        if (owner != null) {
            owner.invalid = true;
        }
    }

    /**
     * Shard local commit was re-confirmed before the original control enforcement flow, and the return of the original session to its main node did not restore the right of ownership that
     * had been replaced.
     *
     * @throws SQLException if the operation cannot complete
     */
    public void requireDataPublication() throws SQLException {
        ControlOwner owner = controlOwner.get();
        checkOwner(owner);
        if (owner == null) {
            return;
        }
        if (!owner.connection.getAutoCommit()) {
            throw new SQLException("data publication requires committed control metadata");
        }
        requireControl(owner.connection);
    }

    /**
     * Sub- local transaction read-only checks ownership and avoids writing it again when another connection already has a control line lock.
     *
     * @param connection connection
     * @throws SQLException if the operation cannot complete
     */
    public void verifyOwner(final Connection connection) throws SQLException {
        ControlOwner owner = controlOwner.get();
        checkOwner(owner);
        if (owner == null) {
            return;
        }
        try (var q = connection.prepareStatement("select pulse from " + prefix + "_runtime where role='control' and namespace=? and generation=?")) {
            q.setString(1, namespace);
            q.setLong(2, generation);
            try (var r = q.executeQuery()) {
                if (!r.next() || r.getLong(1) != owner.pulse) {
                    owner.invalid = true;
                    throw new SQLException("control owner was replaced");
                }
            }
        }
    }

    /**
     * The re-election of the old nodes cannot continue with the original execution stream when current objects and owners are checked within control doors.
     *
     * @param c c
     * @throws SQLException if the operation cannot complete
     */
    public void requireControl(final Connection c) throws SQLException {
        ControlOwner owner = controlOwner.get();
        checkOwner(owner);
        Status current = read(c, "control", false);
        require(current);
        if (grouped(control)) {
            try (
                    var q = c.createStatement();
                    var r = q.executeQuery("select member_state='ONLINE' and member_role='PRIMARY' from performance_schema.replication_group_members where member_id=@@server_uuid")) {
                if (!r.next() || !r.getBoolean(1)) {
                    throw new SQLException("control group primary changed");
                }
            }
            String compare = owner == null ? "" : " and pulse=?";
            try (
                    var q = c.prepareStatement("update " + prefix
                            + "_runtime set pulse=last_insert_id(pulse+1) where role='control' and namespace=? and generation=? and mode=? and pulse<9223372036854775807" + compare)) {
                q.setString(1, namespace);
                q.setLong(2, generation);
                q.setString(3, current.mode());
                if (owner != null) {
                    q.setLong(4, owner.pulse);
                }
                if (q.executeUpdate() != 1) {
                    invalidateControl();
                    throw new SQLException("control owner changed or pulse exhausted");
                }
            }
            if (owner != null) {
                owner.pulse = lastPulse(c);
            }
        }
    }

    /**
     * Read-only takes the pulse of the current session itself and does not treat the count that was subsequently updated by another execution stream as local ownership.
     *
     * @param connection connection
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static long lastPulse(final Connection connection) throws SQLException {
        try (var q = connection.createStatement(); var r = q.executeQuery("select last_insert_id()")) {
            if (!r.next() || r.getLong(1) <= 0) {
                throw new SQLException("control pulse result is invalid");
            }
            return r.getLong(1);
        }
    }

    /**
     * The failure field cannot be used recovery even if it happens to be the same as someone else's count.
     *
     * @param owner owner
     * @throws SQLException if the operation cannot complete
     */
    private static void checkOwner(final ControlOwner owner) throws SQLException {
        if (owner != null && owner.invalid) {
            throw new SQLException("control owner is invalidated");
        }
    }

    /**
     * Visible group replication data sources and their controlled packagingers use commit write set fence and regular fixed examples keep the local shared line lock.
     *
     * @param source database connection source
     * @return the operation result
     */
    static boolean grouped(final DataSource source) {
        return source instanceof com.flyingzc.azir.control.MysqlGroupDataSource
                || (source instanceof com.flyingzc.azir.control.ProtocolMysqlDataSource protocol && protocol.groupAuthority() != null);
    }

    /**
     * Control metadata also hold local run fence, and late old session cannot delete the reader number that was repeated in new namespace.
     *
     * @param connection connection
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public void lockControl(final Connection connection) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("control fence requires a transaction");
        }
        require(read(connection, "control", true));
        requireControl(connection);
    }

    /**
     * Local transaction first holds the shared fence line lock and then writes version; retirement and freezes the use of an exclusive lock until all old SQLs are over.
     *
     * @param c c
     * @param shard logical shard identifier
     * @param start start
     * @param terminalReplay terminal replay
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public long lockData(final Connection c, final String shard, final long start, final boolean terminalReplay) throws SQLException {
        if (c.getAutoCommit()) {
            throw new IllegalStateException("local fence requires a transaction");
        }
        Status status = read(c, "shard:" + shard, true);
        require(status);
        if (!terminalReplay && start <= status.retiredBefore()) {
            throw expired(start);
        }
        certify(c, "shard:" + shard, status);
        return status.retiredBefore();
    }

    /**
     * Each group replication local writing batch is written to the same fence row, overwrites business writing, GC and manage cleanup authentication sequences.
     *
     * @param connection connection
     * @param role role
     * @param observed observed
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    void certify(final Connection connection, final String role, final Status observed) throws SQLException {
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("group fence certification requires a transaction");
        }
        DataSource source = role.equals("control") ? control : role.startsWith("shard:") ? shards.get(role.substring(6)) : null;
        if (source == null) {
            throw new IllegalArgumentException("unconfigured fence role");
        }
        if (!grouped(source)) {
            return;
        }
        ControlOwner owner = controlOwner.get();
        checkOwner(owner);
        boolean ownControl = role.equals("control") && owner != null;
        String value = ownControl ? "last_insert_id(pulse+1)" : "pulse+1";
        String compare = ownControl ? " and pulse=?" : "";
        // Read lock does not enter group replication authentication write set, the pulse must be with version or candidate index changes commit.
        try (
                var q = connection.prepareStatement("update " + prefix + "_runtime set pulse=" + value
                        + " where role=? and namespace=? and generation=? and mode=? and retired_before=? and pulse<9223372036854775807" + compare)) {
            q.setString(1, role);
            q.setString(2, observed.namespace());
            q.setLong(3, observed.generation());
            q.setString(4, observed.mode());
            q.setLong(5, observed.retiredBefore());
            if (ownControl) {
                q.setLong(6, owner.pulse);
            }
            if (q.executeUpdate() != 1) {
                invalidateControl();
                throw new SQLException("group runtime fence changed or exhausted");
            }
        }
        if (ownControl) {
            owner.pulse = lastPulse(connection);
        }
    }

    /**
     * Query the current durable status and do not treat the local cache as running authority.
     *
     * @return the operation result
     */
    public Status status() {
        return read(control, "control", false);
    }

    /**
     * For identified individual management steps only, the business entry cannot hold this field across generation.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     */
    <T> T administrative(final Supplier<T> work) {
        String before = ADMIN.get();
        ADMIN.set(prefix + ":" + namespace + ":" + generation);
        try {
            return work.get();
        } finally {
            if (before == null) {
                ADMIN.remove();
            } else {
                ADMIN.set(before);
            }
        }
    }

    /**
     * Share budget with business client to avoid queuing first.
     *
     * @param <T> value type
     * @param name name
     * @param seconds seconds
     * @param work work
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     * @throws MvccException if the operation cannot complete
     */
    public static <T> T admitted(final String name, final int seconds, final Supplier<T> work) {
        ReentrantLock lock = ADMISSION[(name.hashCode() & Integer.MAX_VALUE) % ADMISSION.length];
        try {
            if (!lock.tryLock(seconds, TimeUnit.SECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT, "protocol admission timed out", true, null);
            }
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("protocol admission interrupted", failure);
        }
        try {
            return work.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Each round of background missions responds to the closure of a single boundary and cannot begin the next block SQL.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("protocol operation interrupted");
        }
    }

    /**
     * Late retirement receipt writing must fail, and recovery cannot automatically become a new ACTIVE transaction.
     *
     * @param start start
     * @return the operation result
     */
    public static MvccException expired(final long start) {
        return new MvccException(ErrorCode.SNAPSHOT_TOO_OLD, "transaction identity retired", false, start);
    }

    /**
     * Constructs the local durable fence table of the current validate protocol.
     *
     * @param source database connection source
     * @param role role
     * @param alreadyInitialized already initialized
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void initialize(final DataSource source, final String role, final boolean alreadyInitialized) {
        try (var c = source.getConnection(); var q = c.createStatement()) {
            q.execute("create table if not exists " + prefix
                    + "_runtime(role varchar(70) character set ascii collate ascii_bin primary key,namespace varchar(36) not null,generation bigint not null,mode varchar(20) not null,retired_before bigint not null,retention_ms bigint not null,checkpoint varchar(36) null,destination text null,retire_cursor bigint not null default 0,pulse bigint not null default 0,freeze_ready boolean not null default false) engine=InnoDB");
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='retire_cursor'")) {
                check.setString(1, prefix + "_runtime");
                boolean present;
                try (var r = check.executeQuery()) {
                    r.next();
                    present = r.getInt(1) > 0;
                }
                if (!present) {
                    q.execute("alter table " + prefix + "_runtime add column retire_cursor bigint not null default 0");
                }
            }
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='pulse'")) {
                check.setString(1, prefix + "_runtime");
                boolean present;
                try (var r = check.executeQuery()) {
                    r.next();
                    present = r.getInt(1) > 0;
                }
                if (!present) {
                    q.execute("alter table " + prefix + "_runtime add column pulse bigint not null default 0");
                }
            }
            try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='freeze_ready'")) {
                check.setString(1, prefix + "_runtime");
                boolean present;
                try (var r = check.executeQuery()) {
                    r.next();
                    present = r.getInt(1) > 0;
                }
                if (!present) {
                    q.execute("alter table " + prefix + "_runtime add column freeze_ready boolean not null default false");
                }
            }
            if (alreadyInitialized) {
                read(c, role, false);
            }
            try (
                    var insert = c.prepareStatement("insert into " + prefix
                            + "_runtime(role,namespace,generation,mode,retired_before,retention_ms,checkpoint,destination) values(?,?,1,'ACTIVE',0,604800000,null,null) on duplicate key update role=role")) {
                insert.setString(1, role);
                insert.setString(2, namespace);
                insert.executeUpdate();
            }
            Status found = read(c, role, false);
            if (!namespace.equals(found.namespace()) && !namespaceRepairAllowed(found.namespace())) {
                throw new IllegalArgumentException("protocol fence namespace mismatch");
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * The first version upgrade builds the logo; no missing character fence can be rebuilt to zero after the sign exists.
     *
     * @param config config
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private boolean operationsInitialized(final String config) {
        try (
                var c = control.getConnection();
                var q = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='operations_version'")) {
            q.setString(1, config);
            boolean exists;
            try (var r = q.executeQuery()) {
                r.next();
                exists = r.getInt(1) > 0;
            }
            if (!exists) {
                try (var ddl = c.createStatement()) {
                    ddl.execute("alter table " + config + " add column operations_version int not null default 1");
                }
            }
            try (var query = c.createStatement(); var r = query.executeQuery("select operations_version from " + config + " where id=1")) {
                if (!r.next()) {
                    throw new SQLException("protocol configuration missing");
                }
                return r.getInt(1) >= 2;
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Allows only durable storage 's new namespace activation plan to be partially repaired, and the regular entrance is still blocked by STAGED.
     *
     * @param value value to write
     * @return the operation result
     */
    private boolean namespaceRepairAllowed(final String value) {
        Status current = read(control, "control", false);
        String plan = current.destination();
        if (!current.mode().equals("STAGED") || plan == null || !plan.startsWith("new-namespace:")) {
            return false;
        }
        String[] parts = plan.split(":");
        return parts.length == 4 && (namespace.equals(parts[1]) || namespace.equals(parts[2])) && (value.equals(parts[1]) || value.equals(parts[2]));
    }

    /**
     * Uniformly check generation and freeze state, managing recovery cannot cross generation replay.
     *
     * @param status status
     * @throws MvccException if the operation cannot complete
     */
    private void require(final Status status) {
        boolean administrative = (prefix + ":" + namespace + ":" + generation).equals(ADMIN.get());
        if (!namespace.equals(status.namespace()) || generation != status.generation()
                || (!status.mode().equals("ACTIVE") && !(administrative && status.mode().equals("FROZEN") && !MysqlProtocolAdmin.transitionPending(status)))) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "protocol frozen, staged, handed off or generation changed; reopen after administration", false, null);
        }
    }

    /**
     * Read fence in caller local transaction, share locks and retirement exclusion locks.
     *
     * @param c c
     * @param role role
     * @param lock lock
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    Status read(final Connection c, final String role, final boolean lock) throws SQLException {
        try (
                var q = c.prepareStatement(
                        "select namespace,generation,mode,retired_before,retention_ms,checkpoint,destination,freeze_ready from " + prefix + "_runtime where role=?" + (lock ? " for share" : ""))) {
            q.setString(1, role);
            try (var r = q.executeQuery()) {
                if (!r.next()) {
                    throw new SQLException("protocol fence missing; refuse reinitialization");
                }
                return new Status(r.getString(1), r.getLong(2), r.getString(3), r.getLong(4), r.getLong(5), r.getString(6), r.getString(7), r.getBoolean(8));
            }
        }
    }

    /**
     * Use an independent connection for diagnosis or initialization without changing the status of the protocol.
     *
     * @param source database connection source
     * @param role role
     * @param lock lock
     * @return the operation result
     */
    Status read(final DataSource source, final String role, final boolean lock) {
        try (var c = source.getConnection()) {
            return read(c, role, lock);
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Retention of SQL failed, and commit level determines otherwise whether the original transaction is unknown.
     *
     * @param failure failure
     * @return the operation result
     */
    public static MvccException storage(final SQLException failure) {
        return new MvccException(ErrorCode.STORAGE_UNAVAILABLE, "protocol operations JDBC failure", true, null, failure);
    }

    /**
     * The pulse value of the current performance stream, which failed, transaction cannot be repossessed by a combination of values after rollback.
     */
    private static final class ControlOwner {

        private long pulse;

        private boolean invalid;

        private final Connection connection;

        /**
         * Only the control pulses that have been obtained are preserved, and subsequent updates must be compared with expected values.
         *
         * @param pulse pulse
         * @param connection connection
         */
        private ControlOwner(final long pulse, final Connection connection) {
            this.pulse = pulse;
            this.connection = connection;
        }
    }

    /**
     * Control owner field within the same line, close only to remove local token without changing durable status.
     */
    public final class ControlScope implements AutoCloseable {

        private final ControlOwner owner;

        private final Thread thread = Thread.currentThread();

        private boolean closed;

        /**
         * It is not permitted to falsify the right of control acquired from outside.
         *
         * @param owner owner
         */
        private ControlScope(final ControlOwner owner) {
            this.owner = owner;
        }

        /**
         * Only the original execution line exits and avoids the mistaken removal of ownership of another implementation stream.
         *
         * @throws IllegalStateException if the operation cannot complete
         */
        @Override
        public void close() {
            if (Thread.currentThread() != thread) {
                throw new IllegalStateException("control scope belongs to another thread");
            }
            if (!closed) {
                if (controlOwner.get() != owner) {
                    throw new IllegalStateException("control scopes closed out of order");
                }
                closed = true;
                controlOwner.remove();
            }
        }
    }

    /**
     * The durable state, the freeze and the handoff status do not allow ordinary clients to continue their service.
     *
     * @param namespace persistent protocol namespace
     * @param generation generation
     * @param mode mode
     * @param retiredBefore retired before
     * @param receiptRetentionMillis receipt retention millis
     * @param checkpoint checkpoint
     * @param destination destination
     * @param freezeReady freeze ready
     */
    public record Status(String namespace, long generation, String mode, long retiredBefore,
            long receiptRetentionMillis, String checkpoint, String destination, boolean freezeReady) {

        /**
         * The corrupted role identity, watermark or mode must be discontinued and the default generation and default retention periods cannot continue.
         *
         * @param namespace persistent protocol namespace
         * @param generation generation
         * @param mode mode
         * @param retiredBefore retired before
         * @param receiptRetentionMillis receipt retention millis
         * @param checkpoint checkpoint
         * @param destination destination
         * @param freezeReady freeze ready
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Status {
            UUID.fromString(namespace);
            if (checkpoint != null) {
                UUID.fromString(checkpoint);
            }
            if (generation < 1 || retiredBefore < 0 || receiptRetentionMillis < 0 || receiptRetentionMillis > 3_153_600_000_000L
                    || !Set.of("ACTIVE", "FROZEN", "RESTORING", "STAGED", "HANDED_OFF").contains(mode)
                    || (!mode.equals("ACTIVE") && checkpoint == null)) {
                throw new IllegalArgumentException("corrupt protocol runtime state");
            }
        }
    }
}
