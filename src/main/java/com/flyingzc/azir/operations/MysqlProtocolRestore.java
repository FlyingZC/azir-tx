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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import com.flyingzc.azir.control.MysqlDatabaseAuthority;
import com.flyingzc.azir.control.ProtocolMysqlDataSource;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * Re-enterable offline recovery tasks; independent logs and shard perform fence without participating in backup data replacement.
 */
public final class MysqlProtocolRestore {

    /**
     * Prohibits exemplifying recovery tools.
     */
    private MysqlProtocolRestore() {
    }

    /**
     * Regularly starts rejecting recovery that has not been completed before initializing any protocol table to prevent a write-back of the initialization default.
     *
     * @param control control
     * @param prefix prefix
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void requireNoImport(final DataSource control, final String prefix) {
        requirePrefix(prefix);
        try (var c = control.getConnection()) {
            if (!exists(c, prefix + "_restore")) {
                return;
            }
            Job job = read(c, prefix);
            if (job != null && job.state().equals("IMPORTING")) {
                throw new IllegalStateException("restore is incomplete; use resume-stage with the original archive");
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Even if running fence is being recovery, it will be possible to query a separate recovery task log.
     *
     * @param control control
     * @param prefix prefix
     * @return the operation result
     */
    public static Job status(final DataSource control, final String prefix) {
        requirePrefix(prefix);
        try (var c = control.getConnection()) {
            return exists(c, prefix + "_restore") ? read(c, prefix) : null;
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Freezing and clean-up is still done at the real backend before the creation of the task, before replacing data is allowed.
     *
     * @param admin admin
     * @param archive archive
     * @param requireEmpty require empty
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static Session start(final MysqlProtocolAdmin admin, final MysqlProtocolBackup.Archive archive, final Runnable requireEmpty) {
        var session = new Session(admin.state().prefix(), admin.state().control(), admin.state().shards());
        try {
            session.createJournal();
            if (read(session.gate, session.prefix) != null) {
                throw new IllegalStateException("restore job already exists; use resume-stage");
            }
            requireEmpty.run();
            var checkpoint = admin.freeze();
            admin.locked(() -> {
                admin.requireFrozen(checkpoint);
                requireEmpty.run();
                try {
                    session.requireHeld();
                    transaction(session.control, c -> {
                        try (
                                var q = c.prepareStatement("select namespace,generation,mode,checkpoint,freeze_ready from " + session.prefix + "_runtime where role='control' for update");
                                var r = q.executeQuery()) {
                            if (!r.next() || !checkpoint.namespace().equals(r.getString(1)) || checkpoint.generation() != r.getLong(2) || !r.getString(3).equals("FROZEN")
                                    || !checkpoint.id().equals(r.getString(4)) || !r.getBoolean(5)) {
                                throw new SQLException("restore target is no longer a complete frozen checkpoint");
                            }
                        }
                        try (
                                var q = c.prepareStatement("insert into " + session.prefix
                                        + "_restore(id,archive_sha,source_namespace,source_generation,checkpoint,target_namespace,target_generation,routes,epoch,state,progress_role,progress_table,updated_at) values(1,?,?,?,?,?,?,?,0,'IMPORTING',null,null,unix_timestamp(current_timestamp(3))*1000)")) {
                            q.setString(1, archive.sha256());
                            q.setString(2, archive.checkpoint().namespace());
                            q.setLong(3, archive.checkpoint().generation());
                            q.setString(4, archive.checkpoint().id());
                            q.setString(5, checkpoint.namespace());
                            q.setLong(6, Math.addExact(Math.max(checkpoint.generation(), archive.checkpoint().generation()), 1));
                            q.setString(7, session.routes);
                            q.executeUpdate();
                        }
                        // Logs and control decomposition atomic are published, which does not leave a gap that can be activated by normal resume.
                        try (var q = c.createStatement()) {
                            if (q.executeUpdate("update " + session.prefix + "_runtime set mode='RESTORING',freeze_ready=false where role='control'") != 1) {
                                throw new SQLException("restore control fence missing");
                            }
                        }
                    });
                    return null;
                } catch (final SQLException failure) {
                    throw storage(failure);
                }
            });
            session.claim(archive, () -> {
            });
            return session;
        } catch (final SQLException | RuntimeException | Error failure) {
            session.closeAfter(failure);
            if (failure instanceof SQLException sql) {
                throw storage(sql);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw (Error) failure;
        }
    }

    /**
     * The original target retry relies only on the task log, not on configuration and running fence that may have been partially replaced.
     *
     * @param prefix prefix
     * @param control control
     * @param shards shards
     * @param archive archive
     * @param preflight preflight
     * @return the operation result
     */
    static Session resume(final String prefix, final DataSource control, final Map<String, DataSource> shards, final MysqlProtocolBackup.Archive archive, final Runnable preflight) {
        var session = new Session(prefix, control, shards);
        try {
            session.claim(archive, preflight);
            return session;
        } catch (final SQLException | RuntimeException | Error failure) {
            session.closeAfter(failure);
            if (failure instanceof SQLException sql) {
                throw storage(sql);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw (Error) failure;
        }
    }

    /**
     * Fixed protocol name participating in the SQL identifier does not accept any external prefix.
     *
     * @param prefix prefix
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requirePrefix(final String prefix) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix)) {
            throw new IllegalArgumentException("invalid protocol");
        }
    }

    /**
     * The metadata existential query does not create a table and avoids read-only status command changing the target.
     *
     * @param c c
     * @param table table
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static boolean exists(final Connection c, final String table) throws SQLException {
        try (var q = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) {
            q.setString(1, table);
            try (var r = q.executeQuery()) {
                r.next();
                return r.getInt(1) != 0;
            }
        }
    }

    /**
     * The fixed single task log is independent of the backup table and no import can overwrite generation here.
     *
     * @param c c
     * @param prefix prefix
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static Job read(final Connection c, final String prefix) throws SQLException {
        try (
                var q = c.createStatement();
                var r = q.executeQuery("select archive_sha,source_namespace,source_generation,checkpoint,target_namespace,target_generation,epoch,state,progress_role,progress_table,updated_at from "
                        + prefix + "_restore where id=1")) {
            return r.next()
                    ? new Job(r.getString(1), r.getString(2), r.getLong(3), r.getString(4), r.getString(5), r.getLong(6), r.getLong(7), r.getString(8), r.getString(9), r.getString(10), r.getLong(11))
                    : null;
        }
    }

    /**
     * Visible rollback all failed, including cancellation; rollback failed to destroy connection.
     *
     * @param source database connection source
     * @param action action
     * @throws SQLException if the operation cannot complete
     */
    private static void transaction(final DataSource source, final SqlAction action) throws SQLException {
        try (var c = source.getConnection()) {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            try {
                action.run(c);
                c.commit();
            } catch (final SQLException | RuntimeException | Error failure) {
                try {
                    c.rollback();
                } catch (final SQLException | RuntimeException | Error rollback) {
                    if (failure != rollback) {
                        failure.addSuppressed(rollback);
                    }
                    com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
                }
                throw failure;
            }
        }
    }

    /**
     * Durable jobs that contain only identity and progress, do not record business content or database receipt.
     *
     * @param sha256 sha256
     * @param sourceNamespace source namespace
     * @param sourceGeneration source generation
     * @param checkpoint checkpoint
     * @param targetNamespace target namespace
     * @param targetGeneration target generation
     * @param epoch epoch
     * @param state state
     * @param role role
     * @param table table
     * @param updatedAt updated at
     */
    public record Job(String sha256, String sourceNamespace, long sourceGeneration, String checkpoint,
            String targetNamespace, long targetGeneration, long epoch, String state,
            String role, String table, long updatedAt) {

        /**
         * The damaged management log must not be used to reopen the target or guess generation.
         *
         * @param sha256 sha256
         * @param sourceNamespace source namespace
         * @param sourceGeneration source generation
         * @param checkpoint checkpoint
         * @param targetNamespace target namespace
         * @param targetGeneration target generation
         * @param epoch epoch
         * @param state state
         * @param role role
         * @param table table
         * @param updatedAt updated at
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Job {
            UUID.fromString(sourceNamespace);
            UUID.fromString(targetNamespace);
            UUID.fromString(checkpoint);
            if (sha256 == null || !sha256.matches("[0-9a-f]{64}") || sourceNamespace.equals(targetNamespace)
                    || sourceGeneration < 1 || targetGeneration <= sourceGeneration || epoch < 0 || updatedAt < 0
                    || !Set.of("IMPORTING", "STAGED").contains(state) || (state.equals("STAGED") && epoch == 0)
                    || (role == null) != (table == null)) {
                throw new IllegalArgumentException("invalid restore job");
            }
        }
    }

    /**
     * Locked session for one bounded execution, all late imports batch must be checked again through shard generation.
     */
    static final class Session implements AutoCloseable {

        final String prefix;

        final DataSource control;

        final Map<String, DataSource> roles;

        final Map<String, DataSource> shards;

        final String routes;

        private final Connection gate;

        private final LinkedHashMap<String, com.flyingzc.azir.control.MysqlNamedLock.Lease> locks = new LinkedHashMap<>();

        private Job job;

        /**
         * Staple the recovery lock with a physical control library, and namespace is imported to replace it without changing the lockname.
         *
         * @param prefix prefix
         * @param control control
         * @param shards shards
         * @throws IllegalArgumentException if the operation cannot complete
         */
        Session(final String prefix, final DataSource control, final Map<String, DataSource> shards) {
            requirePrefix(prefix);
            this.prefix = prefix;
            this.control = new ProtocolMysqlDataSource(control);
            var data = new TreeMap<String, DataSource>();
            var identities = new TreeMap<String, String>();
            identities.put("control", MysqlDatabaseAuthority.inspect(this.control).toString());
            var unique = new HashSet<MysqlDatabaseAuthority.Identity>();
            var physical = new HashSet<MysqlDatabaseAuthority.Identity>();
            shards.forEach((id, source) -> {
                if (!id.matches("[A-Za-z0-9_-]{1,64}")) {
                    throw new IllegalArgumentException("invalid shard id");
                }
                var safe = new ProtocolMysqlDataSource(source);
                var identity = MysqlDatabaseAuthority.inspect(safe);
                if (!unique.add(identity)) {
                    throw new IllegalArgumentException("restore shards alias the same database");
                }
                try (var connection = safe.getConnection()) {
                    if (!physical.add(MysqlDatabaseAuthority.inspectConnection(connection))) {
                        throw new IllegalArgumentException("restore shards currently alias the same physical database");
                    }
                } catch (final SQLException failure) {
                    throw storage(failure);
                }
                data.put(id, safe);
                identities.put("shard:" + id, identity.toString());
            });
            if (data.isEmpty()) {
                throw new IllegalArgumentException("restore requires shards");
            }
            this.shards = Collections.unmodifiableMap(data);
            routes = identities.toString();
            var ordered = new LinkedHashMap<String, DataSource>();
            ordered.put("control", this.control);
            data.forEach((id, source) -> ordered.put("shard:" + id, source));
            roles = Collections.unmodifiableMap(ordered);
            Connection acquired = null;
            try {
                acquired = this.control.getConnection();
                gate = acquired;
                acquire("azir:restore:" + prefix + ":" + UUID.nameUUIDFromBytes(identities.get("control").getBytes(StandardCharsets.UTF_8)));
            } catch (final SQLException | RuntimeException | Error failure) {
                if (acquired != null) {
                    try {
                        acquired.abort(Runnable::run);
                    } catch (final SQLException abort) {
                        failure.addSuppressed(abort);
                    }
                    try {
                        acquired.close();
                    } catch (final SQLException close) {
                        failure.addSuppressed(close);
                    }
                }
                if (failure instanceof SQLException sql) {
                    throw storage(sql);
                }
                if (failure instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw (Error) failure;
            }
        }

        /**
         * Fixed recovery log contains the original empty target identity and does not allow subsequent import to overwrite its route or task summary.
         *
         * @throws SQLException if the operation cannot complete
         */
        private void createJournal() throws SQLException {
            requireHeld();
            try (var q = gate.createStatement()) {
                q.execute("create table if not exists " + prefix
                        + "_restore(id int primary key,archive_sha char(64) character set ascii collate ascii_bin not null,source_namespace varchar(36) not null,source_generation bigint not null,checkpoint varchar(36) not null,target_namespace varchar(36) not null,target_generation bigint not null,routes text not null,epoch bigint not null,state varchar(20) not null,progress_role varchar(70) null,progress_table varchar(64) null,updated_at bigint not null) engine=InnoDB");
            }
            MysqlProtocolSchema.requireTable(gate, prefix + "_restore");
        }

        /**
         * Incremental generation and install all shard fence first. The old implementer cannot continue writing the replay table.
         *
         * @param archive archive
         * @param preflight preflight
         * @throws IllegalArgumentException if the operation cannot complete
         * @throws IllegalStateException if the operation cannot complete
         * @throws SQLException if the operation cannot complete
         */
        private void claim(final MysqlProtocolBackup.Archive archive, final Runnable preflight) throws SQLException {
            requireHeld();
            job = read(gate, prefix);
            MysqlProtocolSchema.requireTable(gate, prefix + "_restore");
            if (job == null || !archive.prefix().equals(prefix) || !job.sha256().equals(archive.sha256()) || !job.sourceNamespace().equals(archive.checkpoint().namespace())
                    || job.sourceGeneration() != archive.checkpoint().generation() || !job.checkpoint().equals(archive.checkpoint().id())) {
                throw new IllegalArgumentException("restore archive does not match the durable job");
            }
            try (var q = gate.createStatement(); var r = q.executeQuery("select routes from " + prefix + "_restore where id=1")) {
                if (!r.next() || !routes.equals(r.getString(1))) {
                    throw new IllegalArgumentException("restore physical routes changed");
                }
            }
            acquire("azir:" + prefix + ":" + job.targetNamespace());
            acquire("azir:" + prefix + ":" + job.sourceNamespace());
            if (job.state().equals("STAGED")) {
                return;
            }
            if (!job.state().equals("IMPORTING")) {
                throw new IllegalStateException("unknown restore state");
            }
            preflight.run();
            // The space from which external services or handoff have been started cannot in any case be covered by recovery retry ; the running line is replaced with atomic in each local transaction
            // .
            for (var entry : roles.entrySet()) {
                MysqlProtocolSchema.requireRole(entry.getValue(), prefix, entry.getKey());
                requireNonServing(entry.getValue(), entry.getKey());
            }
            requireHeld();
            try (
                    var q = gate.prepareStatement("update " + prefix
                            + "_restore set epoch=epoch+1,updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING' and epoch<9223372036854775807")) {
                q.setLong(1, job.epoch());
                if (q.executeUpdate() != 1) {
                    throw new SQLException("restore epoch changed");
                }
            }
            job = read(gate, prefix);
            for (var entry : roles.entrySet()) {
                installFence(entry.getValue(), entry.getKey());
            }
            for (var entry : roles.entrySet()) {
                local(entry.getValue(), entry.getKey(), c -> {
                    try (var q = c.prepareStatement("update " + prefix + "_runtime set mode='RESTORING',freeze_ready=false,pulse=pulse+1 where role=? and mode in ('FROZEN','RESTORING','STAGED')")) {
                        q.setString(1, entry.getKey());
                        if (q.executeUpdate() != 1) {
                            throw new SQLException("restore runtime fence is unavailable");
                        }
                    }
                });
            }
        }

        /**
         * When the recovery result is completed, retry returns to the original receipt only and does not touch any of the sheets.
         *
         * @return the operation result
         */
        boolean complete() {
            return job.state().equals("STAGED");
        }

        /**
         * Go back to receipt line durable identity.
         *
         * @return the operation result
         */
        Job job() {
            return job;
        }

        /**
         * Run fence exists and replay is allowed without service, missing fence cannot be supplemented by guesses from recovery tool.
         *
         * @param source database connection source
         * @param role role
         * @throws IllegalStateException if the operation cannot complete
         * @throws SQLException if the operation cannot complete
         */
        private void requireNonServing(final DataSource source, final String role) throws SQLException {
            try (var c = source.getConnection(); var q = c.prepareStatement("select namespace,mode from " + prefix + "_runtime where role=?")) {
                q.setString(1, role);
                try (var r = q.executeQuery()) {
                    if (!r.next() || !Set.of(job.targetNamespace(), job.sourceNamespace()).contains(r.getString(1)) || !Set.of("FROZEN", "RESTORING", "STAGED").contains(r.getString(2))) {
                        throw new IllegalStateException("restore target is serving or its runtime fence is missing");
                    }
                }
            }
        }

        /**
         * Placing a new generation and waiting for the end of the old batch; generation only adds up and does not cover the new fence by failing to link the old process.
         *
         * @param source database connection source
         * @param role role
         * @throws SQLException if the operation cannot complete
         */
        private void installFence(final DataSource source, final String role) throws SQLException {
            requireHeld();
            try (var c = source.getConnection(); var q = c.createStatement()) {
                q.execute("create table if not exists " + prefix
                        + "_restore_fence(role varchar(70) character set ascii collate ascii_bin primary key,archive_sha char(64) character set ascii collate ascii_bin not null,epoch bigint not null,pulse bigint not null default 0) engine=InnoDB");
                MysqlProtocolSchema.requireTable(c, prefix + "_restore_fence");
                try (var check = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='pulse'")) {
                    check.setString(1, prefix + "_restore_fence");
                    boolean present;
                    try (var r = check.executeQuery()) {
                        r.next();
                        present = r.getInt(1) != 0;
                    }
                    if (!present) {
                        q.execute("alter table " + prefix + "_restore_fence add column pulse bigint not null default 0");
                    }
                }
            }
            transaction(source, c -> {
                try (var q = c.prepareStatement("insert into " + prefix + "_restore_fence(role,archive_sha,epoch) values(?,?,0) on duplicate key update role=role")) {
                    q.setString(1, role);
                    q.setString(2, job.sha256());
                    q.executeUpdate();
                }
                try (var q = c.prepareStatement("select archive_sha,epoch from " + prefix + "_restore_fence where role=? for update")) {
                    q.setString(1, role);
                    try (var r = q.executeQuery()) {
                        if (!r.next() || !job.sha256().equals(r.getString(1)) || r.getLong(2) > job.epoch()) {
                            throw new SQLException("restore executor was superseded");
                        }
                    }
                }
                try (var q = c.prepareStatement("update " + prefix + "_restore_fence set epoch=? where role=?")) {
                    q.setLong(1, job.epoch());
                    q.setString(2, role);
                    q.executeUpdate();
                }
            });
        }

        /**
         * Each imported batch gets the shared fence first, and the receiver must wait for this batch commit or rollback.
         *
         * @param c c
         * @param role role
         * @throws SQLException if the operation cannot complete
         */
        void fence(final Connection c, final String role) throws SQLException {
            if (c.getAutoCommit()) {
                throw new SQLException("restore fence requires a transaction");
            }
            requireHeld();
            try (var q = c.prepareStatement("select archive_sha,epoch from " + prefix + "_restore_fence where role=? for share")) {
                q.setString(1, role);
                try (var r = q.executeQuery()) {
                    if (!r.next() || !job.sha256().equals(r.getString(1)) || r.getLong(2) != job.epoch()) {
                        throw new SQLException("restore batch belongs to an obsolete executor");
                    }
                }
            }
            if (MysqlProtocolState.grouped(roles.get(role))) {
                // Lets batch write to epoch update with the receiver compete for the same line in group replication authentication, instead of relying only on the old master local lock.
                try (var q = c.prepareStatement("update " + prefix + "_restore_fence set pulse=pulse+1 where role=? and archive_sha=? and epoch=? and pulse<9223372036854775807")) {
                    q.setString(1, role);
                    q.setString(2, job.sha256());
                    q.setLong(3, job.epoch());
                    if (q.executeUpdate() != 1) {
                        throw new SQLException("group restore fence changed or exhausted");
                    }
                }
            }
        }

        /**
         * Send a single step change with short local transaction to prevent an unexpected autocommit when the connection failed to return to the pool.
         *
         * @param source database connection source
         * @param role role
         * @param action action
         * @throws SQLException if the operation cannot complete
         */
        void local(final DataSource source, final String role, final SqlAction action) throws SQLException {
            transaction(source, c -> {
                fence(c, role);
                action.run(c);
            });
        }

        /**
         * Progress is diagnosed only, and recovery is always re-imported from a fixed tabular order and cannot rely on unreading table tags.
         *
         * @param role role
         * @param table table
         * @throws SQLException if the operation cannot complete
         */
        void progress(final String role, final String table) throws SQLException {
            requireHeld();
            try (
                    var q = gate.prepareStatement(
                            "update " + prefix + "_restore set progress_role=?,progress_table=?,updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING'")) {
                q.setString(1, role);
                q.setString(2, table);
                q.setLong(3, job.epoch());
                if (q.executeUpdate() != 1) {
                    throw new SQLException("restore task was superseded");
                }
            }
        }

        /**
         * The data character is temporarily stored and the control role and mission completion record are finally published in the same transaction.
         *
         * @throws SQLException if the operation cannot complete
         */
        void finish() throws SQLException {
            var ordered = new ArrayList<>(roles.entrySet());
            ordered.sort(Comparator.comparing(e -> e.getKey().equals("control")));
            for (var entry : ordered) {
                local(entry.getValue(), entry.getKey(), c -> {
                    try (
                            var q = c.prepareStatement("update " + prefix
                                    + "_runtime set generation=?,mode='STAGED',destination=?,freeze_ready=false where role=? and namespace=? and checkpoint=? and mode='RESTORING'")) {
                        q.setLong(1, job.targetGeneration());
                        q.setString(2, "archive:" + job.sha256());
                        q.setString(3, entry.getKey());
                        q.setString(4, job.sourceNamespace());
                        q.setString(5, job.checkpoint());
                        if (q.executeUpdate() != 1) {
                            throw new SQLException("restored runtime identity inconsistent");
                        }
                    }
                    if (entry.getKey().equals("control")) {
                        try (
                                var q = c.prepareStatement(
                                        "update " + prefix + "_restore set state='STAGED',updated_at=unix_timestamp(current_timestamp(3))*1000 where id=1 and epoch=? and state='IMPORTING'")) {
                            q.setLong(1, job.epoch());
                            if (q.executeUpdate() != 1) {
                                throw new SQLException("restore completion was superseded");
                            }
                        }
                    }
                });
            }
        }

        /**
         * The same control session must remain in possession of all locks and cannot be replaced with connection until the result is released.
         *
         * @throws SQLException if the operation cannot complete
         */
        void requireHeld() throws SQLException {
            MysqlProtocolState.checkInterrupted();
            ((ProtocolMysqlDataSource) control).verifySession(gate);
            for (String name : locks.keySet()) {
                try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) {
                    q.setString(1, name);
                    try (var r = q.executeQuery()) {
                        if (!r.next() || !r.getBoolean(1)) {
                            throw new SQLException("restore control session lost");
                        }
                    }
                }
            }
        }

        /**
         * The name lock cannot continue to be imported, but waits for up to 10 seconds.
         *
         * @param name name
         * @throws SQLException if the operation cannot complete
         */
        private void acquire(final String name) throws SQLException {
            if (locks.containsKey(name)) {
                return;
            }
            var lease = com.flyingzc.azir.control.MysqlNamedLock.lock(gate, name, 10);
            try {
                locks.put(name, lease);
            } catch (final RuntimeException | Error failure) {
                try {
                    lease.close();
                } catch (final SQLException close) {
                    failure.addSuppressed(close);
                }
                com.flyingzc.azir.control.MysqlNamedLock.discard(gate, failure);
                throw failure;
            }
        }

        /**
         * Keeps a closing error when there is a master anomaly and does not hide the true import failure.
         *
         * @param failure failure
         */
        private void closeAfter(final Throwable failure) {
            try {
                close();
            } catch (final RuntimeException | Error close) {
                if (failure != close) {
                    failure.addSuppressed(close);
                }
            }
        }

        /**
         * Release the session lock on a case-by-case basis, destroy the session for any anomaly and avoid leaving the namelock in connection pool.
         */
        @Override
        public void close() {
            Throwable failure = null;
            for (var lease : locks.reversed().values()) {
                try {
                    lease.close();
                } catch (final SQLException | RuntimeException | Error problem) {
                    if (failure == null) {
                        failure = problem;
                    } else if (failure != problem) {
                        failure.addSuppressed(problem);
                    }
                }
            }
            if (failure != null) {
                com.flyingzc.azir.control.MysqlNamedLock.discard(gate, failure);
            }
            try {
                gate.close();
            } catch (final SQLException | RuntimeException | Error close) {
                if (failure == null) {
                    failure = close;
                } else if (failure != close) {
                    failure.addSuppressed(close);
                }
            }
            if (failure instanceof SQLException sql) {
                throw storage(sql);
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error fatal) {
                throw fatal;
            }
        }

    }

    /**
     * Only the known local recovery SQL is executed, and SQL clips in files are not received.
     */
    @FunctionalInterface
    interface SqlAction {

        /**
         * Execute an local recovery step.
         *
         * @param c c
         * @throws SQLException if the operation cannot complete
         */
        void run(Connection c) throws SQLException;
    }
}
