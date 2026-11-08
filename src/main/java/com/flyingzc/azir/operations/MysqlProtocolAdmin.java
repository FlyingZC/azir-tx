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
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import java.util.function.IntFunction;
import java.util.function.Supplier;
import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * Visible production management portal; freeze, recovery, retirement and handoff leave durable fence, without ceasing to write an engagement within the process.
 */
public final class MysqlProtocolAdmin {

    private final MysqlProtocolState state;

    private final IntFunction<Integer> recovery;

    private final ThreadLocal<Connection> administrationGate = new ThreadLocal<>();

    private final String administrationName;

    private final boolean writable;

    private final FreezeOptions freezeOptions;

    /**
     * The recovery callback is provided by a real backend and management does not copy the commit protocol.
     *
     * @param state state
     * @param recovery recovery
     */
    public MysqlProtocolAdmin(final MysqlProtocolState state, final IntFunction<Integer> recovery) {
        this(state, recovery, true);
    }

    /**
     * The receipt diagnostic for some roles is only allowed to read and cannot be used to freeze or activate incomplete shard collections.
     *
     * @param state state
     * @param recovery recovery
     * @param writable writable
     */
    private MysqlProtocolAdmin(final MysqlProtocolState state, final IntFunction<Integer> recovery, final boolean writable) {
        this(state, recovery, writable, FreezeOptions.defaults());
    }

    /**
     * Inheritance management privileges and real recovery echoes only adjusts the freeze budget used for this management handle.
     *
     * @param state state
     * @param recovery recovery
     * @param writable writable
     * @param freezeOptions freeze options
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private MysqlProtocolAdmin(final MysqlProtocolState state, final IntFunction<Integer> recovery, final boolean writable, final FreezeOptions freezeOptions) {
        this.freezeOptions = Objects.requireNonNull(freezeOptions);
        this.writable = writable;
        if (writable && state.shards().isEmpty()) {
            throw new IllegalArgumentException("administration requires the full shard configuration");
        }
        this.state = Objects.requireNonNull(state);
        this.recovery = Objects.requireNonNull(recovery);
        var authority = com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control());
        administrationName = "azir:admin:" + state.prefix() + ":" + UUID.nameUUIDFromBytes(authority.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns to use the specified freeze budget management handle. Backup and initial suspense will also be used for budget.
     *
     * @param options options
     * @return the operation result
     */
    public MysqlProtocolAdmin withFreezeOptions(final FreezeOptions options) {
        return new MysqlProtocolAdmin(state, recovery, writable, options);
    }

    /**
     * No business client is created when diagnostics and repairs are deployed, and local recovery cannot automatically reset or watermark.
     *
     * @param prefix prefix
     * @param control control
     * @param shards shards
     * @return the operation result
     */
    public static MysqlProtocolAdmin openExisting(final String prefix, final DataSource control, final Map<String, DataSource> shards) {
        return open(prefix, control, shards, true);
    }

    /**
     * The read-only receipt entrance can only be configured with the required role; returned objects cannot execute any freeze, switch or retirement.
     *
     * @param prefix prefix
     * @param control control
     * @param shards shards
     * @return the operation result
     */
    public static MysqlProtocolAdmin openReceiptAuthority(final String prefix, final DataSource control, final Map<String, DataSource> shards) {
        return open(prefix, control, shards, false);
    }

    /**
     * General management must match the full durable route, and partial read-only diagnostics only contain clearly provided roles.
     *
     * @param prefix prefix
     * @param control control
     * @param shards shards
     * @param writable writable
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static MysqlProtocolAdmin open(final String prefix, final DataSource control, final Map<String, DataSource> shards, final boolean writable) {
        if (!Set.of("pc", "omid", "atlas").contains(prefix)) {
            throw new IllegalArgumentException("invalid protocol");
        }
        var safe = new com.flyingzc.azir.control.ProtocolMysqlDataSource(control);
        var data = new TreeMap<String, DataSource>();
        shards.forEach((id, source) -> data.put(id, new com.flyingzc.azir.control.ProtocolMysqlDataSource(source)));
        String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
        String namespace;
        try (var c = safe.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select namespace,operations_version,routes from " + config + " where id=1")) {
            if (!r.next() || r.getInt(2) != 2) {
                throw new SQLException("managed protocol schema not initialized");
            }
            namespace = r.getString(1);
            if (writable) {
                var routes = new TreeMap<String, String>();
                routes.put("control", safe.authorityIdentity().toString());
                data.forEach((id, source) -> routes.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
                if (!routes.toString().equals(r.getString(3))) {
                    throw new IllegalArgumentException("administration requires the full original physical routes");
                }
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
        if (writable) {
            MysqlProtocolSchema.requireRole(safe, prefix, "control");
            data.forEach((id, source) -> MysqlProtocolSchema.requireRole(source, prefix, "shard:" + id));
        }
        return new MysqlProtocolAdmin(MysqlProtocolState.existing(prefix, namespace, safe, data), ignored -> {
            throw new IllegalStateException("recovery needs the actual transaction backend");
        }, writable);
    }

    /**
     * Incomplete role sets can only check terminal state and no protocol data can be changed.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    void requireWritable() {
        if (!writable) {
            throw new IllegalStateException("receipt authority is read-only");
        }
    }

    /**
     * Returns protocol fence for a backup format to read the controlled layout.
     *
     * @return the operation result
     */
    public MysqlProtocolState state() {
        return state;
    }

    /**
     * Rebuild the original freeze receipt from the durable recovery program to support part recovery or recover the old generation when it is lost.
     *
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public Optional<Checkpoint> resumeCheckpoint() {
        var current = state.status();
        String plan = current.destination();
        if (plan == null || !plan.startsWith("resume:") || !Set.of("FROZEN", "ACTIVE").contains(current.mode())) {
            return Optional.empty();
        }
        try {
            String[] parts = plan.split(":", -1);
            if (parts.length != 4) {
                throw new IllegalArgumentException("invalid resume plan");
            }
            long before = Long.parseLong(parts[1]), after = Long.parseLong(parts[2]);
            if (after != Math.addExact(before, 1) || (current.generation() != before && current.generation() != after) || !Objects.equals(current.checkpoint(), parts[3])) {
                throw new IllegalArgumentException("inconsistent resume plan");
            }
            return Optional.of(new Checkpoint(current.namespace(), before, parts[3]));
        } catch (final RuntimeException failure) {
            throw new IllegalStateException("durable resume plan is invalid", failure);
        }
    }

    /**
     * Returns the state of all durable characters, which can also be diagnosed by local freezing or local switching.
     *
     * @return the operation result
     */
    public Map<String, MysqlProtocolState.Status> status() {
        var result = new TreeMap<String, MysqlProtocolState.Status>();
        result.put("control", state.status());
        state.shards().forEach((id, source) -> result.put("shard:" + id, state.read(source, "shard:" + id, false)));
        return Collections.unmodifiableMap(result);
    }

    /**
     * Freezes and suspends pending -defined operations transaction, recovery All real terminal state before confirming the checkpoint.
     *
     * @return the operation result
     */
    public Checkpoint freeze() {
        return freeze(freezeOptions);
    }

    /**
     * Time budget covers waiting, freezing, batching and recovery; SQL can only be cancelled with limited connection timeout collaboration.
     *
     * @param options options
     * @return the operation result
     */
    public Checkpoint freeze(final FreezeOptions options) {
        requireWritable();
        Objects.requireNonNull(options);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(options.timeoutMillis());
        Checkpoint token = locked(() -> {
            checkFreezeBudget(deadline);
            var control = state.status();
            if (!control.namespace().equals(state.namespace()) || control.generation() != state.generation()) {
                throw new IllegalStateException("freeze requires administration reopened for the current generation");
            }
            if (transitionPending(control)) {
                throw new IllegalStateException("administration transition pending; retry its original resume or handoff");
            }
            if (!Set.of("ACTIVE", "FROZEN").contains(control.mode())) {
                throw new IllegalStateException("cannot freeze " + control.mode());
            }
            if (status().values().stream().anyMatch(s -> s.generation() != control.generation())) {
                throw new IllegalStateException("incomplete generation transition; retry resume before freezing again");
            }
            if (status().values().stream().anyMatch(s -> s.mode().equals("HANDED_OFF"))) {
                throw new IllegalStateException("handoff is irreversible");
            }
            String checkpoint = control.mode().equals("FROZEN") ? control.checkpoint() : UUID.randomUUID().toString();
            var id = new Checkpoint(control.namespace(), control.generation(), checkpoint);
            String preserved = control.mode().equals("FROZEN") ? control.destination() : null;
            change(state.control(), "control", id.generation(), "FROZEN", checkpoint, preserved);
            mutateFrozen(state.control(), "control", id, "update " + state.prefix() + "_runtime set freeze_ready=false where role='control'", deadline);
            state.shards().forEach((shard, source) -> {
                checkFreezeBudget(deadline);
                change(source, "shard:" + shard, id.generation(), "FROZEN", checkpoint, preserved);
            });
            String limit = " limit " + options.batchSize();
            drainFrozen(state.control(), "control", id, state.prefix().equals("pc") ? "delete from pc_readers" + limit
                    : "update " + state.prefix() + "_txn set status='ABORTED',finished_at=if(finished_at=0,unix_timestamp(current_timestamp(3))*1000,finished_at) where status='ACTIVE'" + limit,
                    deadline);
            if (state.prefix().equals("pc")) {
                state.shards().forEach((shard, source) -> drainFrozen(source, "shard:" + shard, id, "update pc_lock set expires_at=0 where expires_at<>0" + limit, deadline));
            }
            return id;
        });
        // Freezing states have been durable, recovery call back to replace the normal control door, not locking between two connection.
        state.administrative(() -> {
            while (hasPending()) {
                checkFreezeBudget(deadline);
                recovery.apply(options.batchSize());
            }
            return null;
        });
        return locked(() -> {
            checkFreezeBudget(deadline);
            requireFrozenRoles(token);
            if (hasPending()) {
                throw new IllegalStateException("unstable frozen checkpoint");
            }
            new MysqlProtocolValidator(this).verify();
            mutateFrozen(state.control(), "control", token, "update " + state.prefix() + "_runtime set freeze_ready=true where role='control'", deadline);
            return token;
        });
    }

    /**
     * Resuscitating the original authority must add generation, and all clients created before freezing continue to fail.
     *
     * @param checkpoint checkpoint
     * @return the operation result
     */
    public long resume(final Checkpoint checkpoint) {
        requireWritable();
        return locked(() -> {
            requireToken(checkpoint);
            var current = state.status();
            long next = Math.addExact(checkpoint.generation(), 1);
            String plan = "resume:" + checkpoint.generation() + ":" + next + ":" + checkpoint.id();
            if (current.mode().equals("ACTIVE") && current.generation() == next) {
                if ((current.destination() != null && !current.destination().equals(plan)) || status().values().stream().anyMatch(value -> !value.mode().equals("ACTIVE") || value.generation() != next
                        || !value.namespace().equals(checkpoint.namespace()) || !Objects.equals(value.checkpoint(), checkpoint.id()) || !Objects.equals(value.destination(), current.destination()))) {
                    throw new IllegalStateException("active control has an incomplete role transition");
                }
                return current.generation();
            }
            if (!current.freezeReady() || hasPending()) {
                throw new IllegalStateException("freeze is incomplete; retry freeze recovery before resume");
            }
            for (var entry : status().entrySet()) {
                var value = entry.getValue();
                boolean frozen = value.mode().equals("FROZEN") && (value.generation() == checkpoint.generation() || value.generation() == next);
                boolean partialActivation =
                        !entry.getKey().equals("control") && current.mode().equals("FROZEN") && current.generation() == next && value.mode().equals("ACTIVE") && value.generation() == next;
                if (!value.namespace().equals(checkpoint.namespace()) || !Objects.equals(value.checkpoint(), checkpoint.id()) || !(frozen || partialActivation)
                        || (intent(value.destination()) && !plan.equals(value.destination()))) {
                    throw new IllegalStateException("resume requires this frozen transition or its partially activated data roles");
                }
            }
            change(state.control(), "control", current.generation(), "FROZEN", checkpoint.id(), plan);
            state.shards().forEach((id, source) -> change(source, "shard:" + id, next, "FROZEN", checkpoint.id(), plan));
            change(state.control(), "control", next, "FROZEN", checkpoint.id(), plan);
            state.shards().forEach((id, source) -> change(source, "shard:" + id, next, "ACTIVE", checkpoint.id(), plan));
            change(state.control(), "control", next, "ACTIVE", checkpoint.id(), plan);
            return next;
        });
    }

    /**
     * Visible settings query receipt retention time; watermark must still be protected by snapshot and shard fence.
     *
     * @param millis millis
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void receiptRetentionMillis(final long millis) {
        requireWritable();
        if (millis < 0 || millis > 3_153_600_000_000L) {
            throw new IllegalArgumentException("invalid receipt retention");
        }
        locked(() -> {
            if (!state.status().mode().equals("ACTIVE")) {
                throw new IllegalStateException("configure retention on an active deployment");
            }
            return local(state.control(), c -> {
                exclusive(c, "control");
                verifyGate();
                var current = state.read(c, "control", false);
                if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) {
                    throw new IllegalStateException("retention configuration belongs to an obsolete client");
                }
                try (var q = c.prepareStatement("update " + state.prefix() + "_runtime set retention_ms=? where role='control'")) {
                    q.setLong(1, millis);
                    q.executeUpdate();
                }
                verifyGate();
                return null;
            });
        });
    }

    /**
     * Automatically maintains the normal commit doorbar; advance fence at all shard first, and retirement without version reference identity.
     *
     * @param gate gate
     * @param floor floor
     * @param limit maximum number of items
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public int retireLocked(final Connection gate, final long floor, final int limit) {
        requireWritable();
        try {
            if (!gate.getAutoCommit()) {
                throw new IllegalStateException("retirement requires a committed safe point");
            }
            try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) {
                q.setString(1, state.gateName());
                try (var r = q.executeQuery()) {
                    if (!r.next() || !r.getBoolean(1)) {
                        throw new SQLException("retirement gate not owned");
                    }
                }
            }
            String sql = state.prefix().equals("pc") ? "select safe_point from pc_control where id=1" : "select safe_point from " + state.prefix() + "_gc where id=1";
            try (var q = gate.createStatement(); var r = q.executeQuery(sql)) {
                if (!r.next() || floor > r.getLong(1)) {
                    throw new IllegalArgumentException("retirement cannot force the safe point forward");
                }
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
        if (limit < 1 || limit > 4096 || floor < 0) {
            throw new IllegalArgumentException("invalid retirement budget");
        }
        var current = state.status();
        if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) {
            throw new IllegalStateException("retirement requires current active generation");
        }
        String prefix = state.prefix();
        for (var entry : state.shards().entrySet()) {
            MysqlProtocolState.checkInterrupted();
            retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                try (var q = c.prepareStatement("update " + prefix + "_runtime set retired_before=greatest(retired_before,?) where role=?")) {
                    q.setLong(1, floor);
                    q.setString(2, "shard:" + entry.getKey());
                    q.executeUpdate();
                }
                return null;
            });
        }
        retireLocal(gate, state.control(), "control", c -> {
            try (var q = c.prepareStatement("update " + prefix + "_runtime set retired_before=greatest(retired_before,?) where role='control'")) {
                q.setLong(1, floor);
                q.executeUpdate();
            }
            return null;
        });
        int removed = 0;
        if (prefix.equals("pc")) {
            for (var entry : state.shards().entrySet()) {
                removed += retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                    int count = 0;
                    for (String table : List.of("pc_write", "pc_rollback")) {
                        /**
                         * Primary key with original identity, cross-shard remnant locks still need a Primary decision.
                         *
                         * @param key business key
                         * @param start start
                         */
                        record Candidate(byte[] key, long start) {
                        }

                        List<Candidate> candidates = new ArrayList<>();
                        try (
                                var q = c.prepareStatement("select user_key,start_ts from " + table
                                        + " where start_ts<=? and finished_at<=unix_timestamp(current_timestamp(3))*1000-? and not exists(select 1 from pc_data d where d.user_key=" + table
                                        + ".user_key and d.start_ts=" + table + ".start_ts) order by user_key,start_ts limit ?")) {
                            q.setLong(1, floor);
                            q.setLong(2, current.receiptRetentionMillis());
                            q.setInt(3, limit);
                            try (var r = q.executeQuery()) {
                                while (r.next()) {
                                    candidates.add(new Candidate(r.getBytes(1), r.getLong(2)));
                                }
                            }
                        }
                        for (var candidate : candidates) {
                            boolean pending = false;
                            for (var shard : state.shards().values()) {
                                if (scalar(shard, "select count(*) from pc_lock where start_ts=" + candidate.start()) > 0) {
                                    pending = true;
                                    break;
                                }
                            }
                            if (!pending) {
                                try (var q = c.prepareStatement("delete from " + table + " where user_key=? and start_ts=?")) {
                                    q.setBytes(1, candidate.key());
                                    q.setLong(2, candidate.start());
                                    count += q.executeUpdate();
                                }
                            }
                        }
                    }
                    try (
                            var q = c.prepareStatement(
                                    "delete from pc_guard where not exists(select 1 from pc_data d where d.user_key=pc_guard.user_key) and not exists(select 1 from pc_lock l where l.user_key=pc_guard.user_key) and not exists(select 1 from pc_write w where w.user_key=pc_guard.user_key) and not exists(select 1 from pc_rollback r where r.user_key=pc_guard.user_key) order by user_key limit ?")) {
                        q.setInt(1, limit);
                        q.executeUpdate();
                    }
                    return count;
                });
            }
        } else {
            List<Long> candidates = new ArrayList<>();
            long cursor = scalar(state.control(), "select retire_cursor from " + prefix + "_runtime where role='control'");
            try (
                    var c = state.control().getConnection(); var q = c.prepareStatement("select start_ts from " + prefix
                            + "_txn where start_ts<=? and start_ts>? and status<>'ACTIVE' and finalized=1 and finished_at>0 and finished_at<=unix_timestamp(current_timestamp(3))*1000-? order by start_ts limit ?")) {
                q.setLong(1, floor);
                q.setLong(2, cursor);
                q.setLong(3, current.receiptRetentionMillis());
                q.setInt(4, limit);
                try (var r = q.executeQuery()) {
                    while (r.next()) {
                        candidates.add(r.getLong(1));
                    }
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
            for (long start : candidates) {
                MysqlProtocolState.checkInterrupted();
                boolean referenced = false;
                for (var source : state.shards().values()) {
                    if (scalar(source, "select count(*) from " + prefix + "_versions where stamp=" + start) != 0) {
                        referenced = true;
                        break;
                    }
                }
                if (!referenced) {
                    removed += retireLocal(gate, state.control(), "control", c -> {
                        try (var q = c.prepareStatement("delete from " + prefix + "_txn where start_ts=? and status<>'ACTIVE' and finalized=1")) {
                            q.setLong(1, start);
                            return q.executeUpdate();
                        }
                    });
                }
            }
            retireLocal(gate, state.control(), "control", c -> {
                try (var q = c.prepareStatement("update " + prefix + "_runtime set retire_cursor=? where role='control'")) {
                    q.setLong(1, candidates.isEmpty() ? 0 : candidates.getLast());
                    q.executeUpdate();
                }
                return null;
            });
            for (var entry : state.shards().entrySet()) {
                retireLocal(gate, entry.getValue(), "shard:" + entry.getKey(), c -> {
                    try (
                            var q = c.prepareStatement("delete from " + prefix + "_keys where not exists(select 1 from " + prefix + "_versions v where v.table_name=" + prefix
                                    + "_keys.table_name and v.row_key=" + prefix + "_keys.row_key) order by table_name,row_key limit ?")) {
                        q.setInt(1, limit);
                        q.executeUpdate();
                    }
                    return null;
                });
            }
            retireLocal(gate, state.control(), "control", c -> {
                try (var q = c.prepareStatement("delete from " + prefix + "_conflicts where commit_ts<=? order by cell_id limit ?")) {
                    q.setLong(1, floor);
                    q.setInt(2, limit);
                    q.executeUpdate();
                }
                return null;
            });
        }
        return removed;
    }

    /**
     * Retirement metadata must also wait for the old local and review the original control session and cannot be deleted after freezing.
     *
     * @param <T> value type
     * @param gate gate
     * @param source database connection source
     * @param role role
     * @param work work
     * @return the operation result
     */
    private <T> T retireLocal(final Connection gate, final DataSource source, final String role, final Work<T> work) {
        return local(source, c -> {
            exclusive(c, role);
            requireRetirementGate(gate);
            state.verifyOwner(gate);
            var current = state.read(c, role, false);
            if (!current.mode().equals("ACTIVE") || current.generation() != state.generation()) {
                throw new SQLException("retirement role is frozen or belongs to another generation");
            }
            state.certify(c, role, current);
            T result = work.run(c);
            requireRetirementGate(gate);
            state.verifyOwner(role.equals("control") ? c : gate);
            return result;
        });
    }

    /**
     * Confirm the name lock and authority with original maintenance session, not modify the control pulse line when another connection holds a runtime line lock.
     *
     * @param gate gate
     * @throws SQLException if the operation cannot complete
     */
    private void requireRetirementGate(final Connection gate) throws SQLException {
        MysqlProtocolState.checkInterrupted();
        if (state.control() instanceof com.flyingzc.azir.control.ProtocolMysqlDataSource protocol) {
            protocol.verifySession(gate);
        }
        try (var q = gate.prepareStatement("select is_used_lock(?)=connection_id()")) {
            q.setString(1, state.gateName());
            try (var r = q.executeQuery()) {
                if (!r.next() || !r.getBoolean(1)) {
                    throw new SQLException("retirement control session lost");
                }
            }
        }
        var current = state.read(gate, "control", false);
        if (!current.namespace().equals(state.namespace()) || current.generation() != state.generation() || !current.mode().equals("ACTIVE")) {
            throw new SQLException("retirement control generation is no longer active");
        }
    }

    /**
     * Pass validate STAGED dataspace to the single original authority; former authority is not activated until it has been permanently handed over.
     *
     * @param source database connection source
     * @param sourceCheckpoint source checkpoint
     * @param archiveSha archive sha
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void activateFrom(final MysqlProtocolAdmin source, final Checkpoint sourceCheckpoint, final String archiveSha) {
        requireWritable();
        source.requireWritable();
        if (!archiveSha.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("invalid archive digest");
        }
        if (com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).serverUuid().equals(com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source.state.control()).serverUuid())
                || routes().equals(source.routes()) || !state.namespace().equals(source.state.namespace())) {
            throw new IllegalArgumentException("handoff requires distinct routes and the same namespace");
        }
        source.locked(() -> locked(() -> {
            source.requireToken(sourceCheckpoint);
            var original = source.state.status();
            if (!original.freezeReady() || original.generation() != sourceCheckpoint.generation()) {
                throw new IllegalStateException("source freeze has not completed at this generation");
            }
            var target = state.status();
            String destination = routes() + "@" + archiveSha;
            String handoffPlan = "handoff:" + destination;
            if (original.mode().equals("FROZEN") && intent(original.destination()) && !handoffPlan.equals(original.destination())) {
                throw new IllegalStateException("source has a different administration intent");
            }
            if (!Objects.equals(target.checkpoint(), sourceCheckpoint.id()) || !Objects.equals(target.destination(), "archive:" + archiveSha)
                    || !Set.of("STAGED", "ACTIVE").contains(target.mode())) {
                throw new IllegalStateException("target is not this verified archive");
            }
            for (var role : source.status().values()) {
                if (!role.namespace().equals(sourceCheckpoint.namespace()) || role.generation() != sourceCheckpoint.generation() || !Objects.equals(role.checkpoint(), sourceCheckpoint.id())
                        || !Set.of("FROZEN", "HANDED_OFF").contains(role.mode()) || (role.mode().equals("HANDED_OFF") && !destination.equals(role.destination()))) {
                    throw new IllegalStateException("source already assigned to another destination");
                }
            }
            for (var role : status().values()) {
                if (!Set.of("STAGED", "ACTIVE").contains(role.mode()) || role.generation() != target.generation()
                        || !Objects.equals(role.checkpoint(), sourceCheckpoint.id()) || !Objects.equals(role.destination(), "archive:" + archiveSha)) {
                    throw new IllegalStateException("incomplete staged target");
                }
            }
            if (original.mode().equals("FROZEN")) {
                source.change(source.state.control(), "control", sourceCheckpoint.generation(), "FROZEN", sourceCheckpoint.id(), handoffPlan);
            }
            source.state.shards().forEach((id, pool) -> source.change(pool, "shard:" + id, sourceCheckpoint.generation(), "HANDED_OFF", sourceCheckpoint.id(), destination));
            source.change(source.state.control(), "control", sourceCheckpoint.generation(), "HANDED_OFF", sourceCheckpoint.id(), destination);
            state.shards().forEach((id, pool) -> change(pool, "shard:" + id, target.generation(), "ACTIVE", target.checkpoint(), target.destination()));
            change(state.control(), "control", target.generation(), "ACTIVE", target.checkpoint(), target.destination());
            return null;
        }));
    }

    /**
     * Returning the physical routing fingerprint does not allow the same checkpoint to be given to target group 2 database.
     *
     * @return the operation result
     */
    public String routes() {
        var values = new TreeMap<String, String>();
        values.put("control", com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).toString());
        state.shards().forEach((id, source) -> values.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
        return sha(values.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Construct a route string that fully corresponds to the durable configuration and is used for visible space recovery.
     *
     * @return the operation result
     */
    String routeConfiguration() {
        var values = new TreeMap<String, String>();
        values.put("control", com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(state.control()).toString());
        state.shards().forEach((id, source) -> values.put("shard:" + id, com.flyingzc.azir.control.MysqlDatabaseAuthority.inspect(source).toString()));
        return values.toString();
    }

    /**
     * Confirm that all players are still in the same complete freeze and that backup cannot be generated from a local freeze.
     *
     * @param checkpoint checkpoint
     * @throws IllegalStateException if the operation cannot complete
     */
    void requireFrozen(final Checkpoint checkpoint) {
        var current = state.status();
        if (!current.freezeReady() || transitionPending(current)) {
            throw new IllegalStateException("checkpoint has not completed recovery or already has a transition intent");
        }
        requireFrozenRoles(checkpoint);
    }

    /**
     * Freezing recovery internal check for consistency of roles before completing the validate concurrent -readiness tag.
     *
     * @param checkpoint checkpoint
     * @throws IllegalStateException if the operation cannot complete
     */
    private void requireFrozenRoles(final Checkpoint checkpoint) {
        requireToken(checkpoint);
        for (var value : status().values()) {
            if (!value.mode().equals("FROZEN") || value.generation() != checkpoint.generation()
                    || !checkpoint.id().equals(value.checkpoint())) {
                throw new IllegalStateException("not a complete frozen checkpoint");
            }
        }
    }

    /**
     * The management operation is also protected by the normal control face name lock and does not bypass transaction as it is commit.
     *
     * @param <T> value type
     * @param work work
     * @return the operation result
     */
    <T> T locked(final Supplier<T> work) {
        return MysqlProtocolState.admitted(administrationName, 10, () -> MysqlProtocolState.admitted(state.gateName(), 10, () -> {
            // Physical management locks were obtained before the namespace lock, and release abnormalities were added to the original operation.
            try (
                    var c = state.control().getConnection();
                    var physical = com.flyingzc.azir.control.MysqlNamedLock.lock(c, administrationName, 10);
                    var logical = com.flyingzc.azir.control.MysqlNamedLock.lock(c, state.gateName(), 10)) {
                Connection before = administrationGate.get();
                administrationGate.set(c);
                try {
                    return work.get();
                } finally {
                    if (before == null) {
                        administrationGate.remove();
                    } else {
                        administrationGate.set(before);
                    }
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
        }));
    }

    /**
     * A time-consuming management step to check the original lock session before publishing a file or state, and cannot read the freeze sign only with the new connection.
     *
     * @throws SQLException if the operation cannot complete
     */
    void verifyGate() throws SQLException {
        MysqlProtocolState.checkInterrupted();
        Connection c = administrationGate.get();
        if (c == null) {
            throw new SQLException("administration gate is not bound");
        }
        if (state.control() instanceof com.flyingzc.azir.control.ProtocolMysqlDataSource protocol) {
            protocol.verifySession(c);
        }
        for (String name : List.of(administrationName, state.gateName())) {
            try (var q = c.prepareStatement("select is_used_lock(?)=connection_id()")) {
                q.setString(1, name);
                try (var r = q.executeQuery()) {
                    if (!r.next() || !r.getBoolean(1)) {
                        throw new SQLException("administration gate lost");
                    }
                }
            }
        }
    }

    /**
     * Waits for all old scripts that have entered this shard to be completed with a local exclusion lock before you release a new generation or freeze.
     *
     * @param source database connection source
     * @param role role
     * @param generation generation
     * @param mode mode
     * @param checkpoint checkpoint
     * @param destination destination
     */
    void change(final DataSource source, final String role, final long generation, final String mode, final String checkpoint, final String destination) {
        requireWritable();
        local(source, c -> {
            exclusive(c, role);
            verifyGate();
            var current = state.read(c, role, false);
            if (generation < current.generation()) {
                throw new SQLException("administration cannot decrease runtime generation");
            }
            requireTransition(role, current, generation, mode, checkpoint, destination);
            // Resume retry can only fill unenabled roles and cannot be reduced back to FROZEN by the same program already ACTIVE.
            if (mode.equals("FROZEN") && destination != null && destination.startsWith("resume:") && current.mode().equals("ACTIVE") && generation == current.generation()
                    && Objects.equals(checkpoint, current.checkpoint())) {
                return null;
            }
            if (current.mode().equals("HANDED_OFF")
                    && !(mode.equals("HANDED_OFF") && generation == current.generation() && Objects.equals(checkpoint, current.checkpoint()) && Objects.equals(destination, current.destination()))) {
                throw new SQLException("handoff is irreversible");
            }
            if (mode.equals("FROZEN") && !Set.of("ACTIVE", "FROZEN").contains(current.mode())) {
                throw new SQLException("cannot refreeze restoring, staged or handed-off data");
            }
            if (mode.equals("ACTIVE") && (generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()) || !Set.of("ACTIVE", "FROZEN", "STAGED").contains(current.mode()))) {
                throw new SQLException("activation must preserve the prepared generation and checkpoint");
            }
            if (mode.equals("HANDED_OFF") && (generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()) || !Set.of("FROZEN", "HANDED_OFF").contains(current.mode()))) {
                throw new SQLException("handoff requires its original frozen generation");
            }
            if (mode.equals("STAGED") && current.destination() != null && current.destination().startsWith("new-namespace:") && !Objects.equals(destination, current.destination())) {
                throw new SQLException("namespace activation plan already assigned");
            }
            if (mode.equals("STAGED") && (!current.mode().equals("STAGED") || generation != current.generation() || !Objects.equals(checkpoint, current.checkpoint()))) {
                throw new SQLException("namespace activation plan requires its original staged generation");
            }
            state.certify(c, role, current);
            boolean resetReady = mode.equals("ACTIVE") || (mode.equals("FROZEN") && (current.mode().equals("ACTIVE") || !Objects.equals(checkpoint, current.checkpoint())));
            try (var q = c.prepareStatement("update " + state.prefix() + "_runtime set generation=?,mode=?,checkpoint=?,destination=?" + (resetReady ? ",freeze_ready=false" : "") + " where role=?")) {
                q.setLong(1, generation);
                q.setString(2, mode);
                q.setString(3, checkpoint);
                q.setString(4, destination);
                q.setString(5, role);
                q.executeUpdate();
            }
            verifyGate();
            return null;
        });
    }

    /**
     * The management intent is validate with the checkpoint, and the re-election of the node does not allow the implementer who maintains the old lock to retreat from the new process.
     *
     * @param role role
     * @param current current
     * @param generation generation
     * @param mode mode
     * @param checkpoint checkpoint
     * @param destination destination
     * @throws SQLException if the operation cannot complete
     */
    private void requireTransition(final String role, final MysqlProtocolState.Status current, final long generation,
                                   final String mode, final String checkpoint, final String destination) throws SQLException {
        var authority = role.equals("control") ? current : state.read(administrationGate.get(), "control", false);
        if (!authority.namespace().equals(state.namespace())) {
            throw new SQLException("administration control namespace changed");
        }
        if (destination != null && destination.startsWith("resume:")) {
            boolean sameIntent = destination.equals(authority.destination());
            boolean initial = role.equals("control") && authority.mode().equals("FROZEN") && authority.freezeReady() && !intent(authority.destination());
            if (!Objects.equals(checkpoint, authority.checkpoint()) || !Set.of("FROZEN", "ACTIVE").contains(authority.mode()) || !(sameIntent || initial)) {
                throw new SQLException("resume intent was superseded");
            }
        } else if (destination != null && destination.startsWith("handoff:")) {
            if (!role.equals("control") || !mode.equals("FROZEN") || !authority.mode().equals("FROZEN") || !authority.freezeReady() || generation != authority.generation()
                    || !Objects.equals(checkpoint, authority.checkpoint()) || (intent(authority.destination()) && !destination.equals(authority.destination()))) {
                throw new SQLException("handoff intent already assigned or superseded");
            }
        } else if (mode.equals("HANDED_OFF")) {
            boolean prepared = authority.mode().equals("FROZEN") && Objects.equals(authority.destination(), "handoff:" + destination);
            boolean completed = authority.mode().equals("HANDED_OFF") && Objects.equals(authority.destination(), destination);
            if (generation != authority.generation() || !Objects.equals(checkpoint, authority.checkpoint()) || !(prepared || completed)) {
                throw new SQLException("handoff requires its exact persisted intent");
            }
        } else if (mode.equals("FROZEN") && transitionPending(authority)) {
            throw new SQLException("freeze cannot replace an unfinished administration intent");
        }
    }

    /**
     * Only internal recovery and handoff intents are identified, original backup, old version upgrades and namespace plans to retain their semantics.
     *
     * @param destination destination
     * @return the operation result
     */
    private static boolean intent(final String destination) {
        return destination != null && (destination.startsWith("resume:") || destination.startsWith("handoff:"));
    }

    /**
     * Durable The management intent in the freezing state must be completed first and cannot be reinterpretation as another freeze.
     *
     * @param status status
     * @return the operation result
     */
    static boolean transitionPending(final MysqlProtocolState.Status status) {
        return status.mode().equals("FROZEN") && intent(status.destination());
    }

    /**
     * The same frozen checkpoint is cleared in batch by commit batch reserved, and timeout followed by retry continues to process remaining records.
     *
     * @param source database connection source
     * @param role role
     * @param token token
     * @param sql sql
     * @param deadline deadline
     */
    private void drainFrozen(final DataSource source, final String role, final Checkpoint token, final String sql, final long deadline) {
        while (true) {
            checkFreezeBudget(deadline);
            if (mutateFrozen(source, role, token, sql, deadline) == 0) {
                return;
            }
        }
    }

    /**
     * Freezing to clear the same locked role and recheck the original session. Late SQL cannot abort the new transaction after recovery.
     *
     * @param source database connection source
     * @param role role
     * @param token token
     * @param sql sql
     * @param deadline deadline
     * @return the operation result
     */
    private int mutateFrozen(final DataSource source, final String role, final Checkpoint token, final String sql, final long deadline) {
        checkFreezeBudget(deadline);
        return local(source, c -> {
            exclusive(c, role);
            verifyGate();
            checkFreezeBudget(deadline);
            var current = state.read(c, role, false);
            var authority = role.equals("control") ? current : state.read(administrationGate.get(), "control", false);
            if (transitionPending(authority)) {
                throw new SQLException("freeze cleanup cannot overwrite an administration intent");
            }
            if (!current.mode().equals("FROZEN") || current.generation() != token.generation() || !Objects.equals(current.checkpoint(), token.id())) {
                throw new SQLException("frozen mutation belongs to an obsolete checkpoint");
            }
            state.certify(c, role, current);
            int affected;
            try (var q = c.createStatement()) {
                q.setQueryTimeout(freezeSecondsRemaining(deadline));
                affected = q.executeUpdate(sql);
            }
            verifyGate();
            checkFreezeBudget(deadline);
            return affected;
        });
    }

    /**
     * Use X-0 timing without increasing the time allowed for management operations due to system proofing.
     *
     * @param deadline deadline
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void checkFreezeBudget(final long deadline) {
        MysqlProtocolState.checkInterrupted();
        if (deadline - System.nanoTime() <= 0) {
            throw new IllegalStateException("freeze budget exhausted; inspect status and retry freeze");
        }
    }

    /**
     * JDBC cancels SQL in seconds and has to recheck millisecond budget before the commit after taking it up.
     *
     * @param deadline deadline
     * @return the operation result
     */
    private static int freezeSecondsRemaining(final long deadline) {
        checkFreezeBudget(deadline);
        long remaining = deadline - System.nanoTime();
        return (int) Math.max(1, (remaining + 999_999_999L) / 1_000_000_000L);
    }

    /**
     * Validate Stabilizes the checkpoint without any string as an recovery authorization.
     *
     * @param token token
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void requireToken(final Checkpoint token) {
        var value = state.status();
        if (!state.namespace().equals(token.namespace()) || !Objects.equals(value.checkpoint(), token.id())
                || (value.generation() != token.generation() && value.generation() != token.generation() + 1)) {
            throw new IllegalArgumentException("checkpoint mismatch");
        }
    }

    /**
     * Only to determine whether recovery records are still pending and not the exact number of complete tables accumulated for the management cycle.
     *
     * @return the operation result
     */
    private boolean hasPending() {
        if (!state.prefix().equals("pc") && scalar(state.control(), "select exists(select 1 from " + state.prefix() + "_txn where status='ACTIVE' or finalized=0 limit 1)") != 0) {
            return true;
        }
        for (var source : state.shards().values()) {
            if (scalar(source, state.prefix().equals("pc") ? "select exists(select 1 from pc_lock limit 1)"
                    : "select exists(select 1 from " + state.prefix() + "_versions where shadow_ts is null limit 1)") != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get local fence Quicklock, caller must have turned on short local transaction.
     *
     * @param c c
     * @param role role
     * @throws SQLException if the operation cannot complete
     */
    private void exclusive(final Connection c, final String role) throws SQLException {
        try (var q = c.prepareStatement("select namespace from " + state.prefix() + "_runtime where role=? for update")) {
            q.setString(1, role);
            try (var r = q.executeQuery()) {
                if (!r.next() || !state.namespace().equals(r.getString(1))) {
                    throw new SQLException("runtime fence missing or replaced");
                }
            }
        }
    }

    /**
     * Local transaction failed to destroy connection instead of autocommit when rollback is unknown.
     *
     * @param <T> value type
     * @param source database connection source
     * @param work work
     * @return the operation result
     */
    <T> T local(final DataSource source, final Work<T> work) {
        MysqlProtocolState.checkInterrupted();
        try (var c = source.getConnection()) {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            try {
                T value = work.run(c);
                c.commit();
                return value;
            } catch (final SQLException | RuntimeException | Error failure) {
                state.invalidateControl();
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
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Reads the fixed management aggregation value, without loading a full set of transaction or version.
     *
     * @param source database connection source
     * @param sql sql
     * @return the operation result
     */
    static long scalar(final DataSource source, final String sql) {
        try (var c = source.getConnection(); var q = c.createStatement(); var r = q.executeQuery(sql)) {
            r.next();
            return r.getLong(1);
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Adds the terminal state time bar, where the old records at the source backend are filled in by bounded recovery batch and start calculating retention periods.
     *
     * @param source database connection source
     * @param table table
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void ensureFinishedColumn(final DataSource source, final String table) {
        if (!table.matches("(pc_(write|rollback)|(omid|atlas)_txn)")) {
            throw new IllegalArgumentException("invalid finished table");
        }
        try (
                var c = source.getConnection();
                var q = c.prepareStatement("select count(*) from information_schema.columns where table_schema=database() and table_name=? and column_name='finished_at'")) {
            q.setString(1, table);
            boolean exists;
            try (var r = q.executeQuery()) {
                r.next();
                exists = r.getInt(1) != 0;
            }
            if (!exists) {
                try (var ddl = c.createStatement()) {
                    ddl.execute("alter table " + table + " add column finished_at bigint not null default "
                            + (table.startsWith("pc_") ? "(cast(unix_timestamp(current_timestamp(3))*1000 as signed))" : "0"));
                }
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }

    /**
     * Use a fixed summary for the file and route identity.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    static String sha(final byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (final java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * The total time of the freeze budget and individual clean-up, recovery batch caps apply to a large transaction backlog of controlled shutdowns.
     *
     * @param timeoutMillis timeout millis
     * @param batchSize batch size
     */
    public record FreezeOptions(long timeoutMillis, int batchSize) {

        /**
         * It is prohibited to freeze for an indefinite period, Zero budget busy cycle or unlimited single times local transaction.
         *
         * @param timeoutMillis timeout millis
         * @param batchSize batch size
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public FreezeOptions {
            if (timeoutMillis < 1 || timeoutMillis > 3_600_000 || batchSize < 1 || batchSize > 4096) {
                throw new IllegalArgumentException("invalid freeze options");
            }
        }

        /**
         * Default freeze budget 60 seconds, each local clean batch up to a thousand twenty-four rows.
         *
         * @return the operation result
         */
        public static FreezeOptions defaults() {
            return new FreezeOptions(60_000, 1024);
        }
    }

    /**
     * Freezes receipt while binding namespace, running generation and this management checkpoint.
     *
     * @param namespace persistent protocol namespace
     * @param generation generation
     * @param id id
     */
    public record Checkpoint(String namespace, long generation, String id) {

        /**
         * Manage receipt cannot miss or cross-space guess.
         *
         * @param namespace persistent protocol namespace
         * @param generation generation
         * @param id id
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Checkpoint {
            UUID.fromString(namespace);
            UUID.fromString(id);
            if (generation < 1) {
                throw new IllegalArgumentException("invalid generation");
            }
        }
    }

    /**
     * Only SQL jobs in a fixed table layout are accepted, and the user input of SQL is not accepted.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    interface Work<T> {

        /**
         * Execute a short local transaction.
         *
         * @param c c
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        T run(Connection c) throws SQLException;
    }
}
