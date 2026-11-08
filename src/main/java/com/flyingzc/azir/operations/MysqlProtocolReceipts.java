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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import com.flyingzc.azir.backend.BackendDecision;
import com.flyingzc.azir.backend.BackendId;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.storage.Utf8KeyOrder;

import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * The read-only receipt query can be checked for terminal state after freezing or handoff and does not execute expired rollback or write decision.
 */
public final class MysqlProtocolReceipts {

    /**
     * Prohibits exemplifying receipt reading tools.
     */
    private MysqlProtocolReceipts() {
    }

    /**
     * Ask for Omid or AtlasDB original control authority, missing records can only return pending or retirement.
     *
     * @param admin admin
     * @param id id
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static BackendDecision lookup(final MysqlProtocolAdmin admin, final BackendId id) {
        Objects.requireNonNull(id);
        String prefix = admin.state().prefix();
        String implementation = switch (prefix) {
            case "omid" -> "omid";
            case "atlas" -> "atlasdb";
            default -> throw new IllegalArgumentException("source backend receipt required");
        };
        if (!implementation.equals(id.implementation())) {
            throw new IllegalArgumentException("receipt implementation mismatch");
        }
        return read(admin, id.namespace(), "control", id.startTs(), c -> {
            try (var q = c.prepareStatement("select status,commit_ts from " + prefix + "_txn where start_ts=?")) {
                q.setLong(1, id.startTs());
                try (var r = q.executeQuery()) {
                    if (!r.next()) {
                        return null;
                    }
                    return switch (r.getString(1)) {
                        case "ACTIVE" -> {
                            if (r.getLong(2) != 0) {
                                throw new SQLException("pending receipt has commit timestamp");
                            }
                            yield new BackendDecision(com.flyingzc.azir.backend.BackendDecision.State.PENDING, 0);
                        }
                        case "ABORTED" -> {
                            if (r.getLong(2) != 0) {
                                throw new SQLException("aborted receipt has commit timestamp");
                            }
                            yield new BackendDecision(com.flyingzc.azir.backend.BackendDecision.State.ABORTED, 0);
                        }
                        case "COMMITTED" -> committed(id.startTs(), r.getLong(2));
                        default -> throw new SQLException("invalid durable transaction state");
                    };
                }
            }
        });
    }

    /**
     * Query for Percolator old Primary, expired and missing locks cannot be interpreted as rollback by the read-only command.
     *
     * @param admin admin
     * @param handle handle
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static BackendDecision lookup(final MysqlProtocolAdmin admin, final PercolatorRuntime.Handle handle) {
        Objects.requireNonNull(handle);
        if (!admin.state().prefix().equals("pc") || handle.namespace() == null) {
            throw new IllegalArgumentException("managed Percolator receipt required");
        }
        byte[] key = Utf8KeyOrder.encode(handle.primary().key());
        return read(admin, handle.namespace(), "shard:" + handle.primary().shard(), handle.startTs(), c -> {
            try (var q = c.createStatement(); var r = q.executeQuery("select namespace,shard from pc_binding where id=1")) {
                if (!r.next() || !handle.namespace().equals(r.getString(1)) || !handle.primary().shard().equals(r.getString(2))) {
                    throw new SQLException("receipt primary shard binding differs");
                }
            }
            Long timestamp = null;
            boolean rollback;
            try (var q = c.prepareStatement("select commit_ts from pc_write where user_key=? and start_ts=?")) {
                q.setBytes(1, key);
                q.setLong(2, handle.startTs());
                try (var r = q.executeQuery()) {
                    if (r.next()) {
                        timestamp = r.getLong(1);
                    }
                }
            }
            try (var q = c.prepareStatement("select exists(select 1 from pc_rollback where user_key=? and start_ts=?)")) {
                q.setBytes(1, key);
                q.setLong(2, handle.startTs());
                try (var r = q.executeQuery()) {
                    r.next();
                    rollback = r.getBoolean(1);
                }
            }
            if (timestamp != null && rollback) {
                throw new SQLException("Percolator receipt has contradictory terminal proofs");
            }
            if (timestamp != null) {
                return committed(handle.startTs(), timestamp);
            }
            return rollback ? new BackendDecision(com.flyingzc.azir.backend.BackendDecision.State.ABORTED, 0) : null;
        });
    }

    /**
     * An physical database in which local snapshot reads watermark and decision with the same name lock is not allowed to return successful results after missing.
     *
     * @param admin admin
     * @param namespace persistent protocol namespace
     * @param role role
     * @param startTs transaction start timestamp
     * @param lookup lookup
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static BackendDecision read(final MysqlProtocolAdmin admin, final String namespace, final String role, final long startTs, final Lookup lookup) {
        UUID.fromString(namespace);
        if (startTs <= 0) {
            throw new IllegalArgumentException("positive startTs required");
        }
        if (!namespace.equals(admin.state().namespace())) {
            throw new IllegalArgumentException("receipt namespace differs from the opened authority");
        }
        return admin.locked(() -> {
            try {
                admin.verifyGate();
                requireAuthority(admin.state().status(), namespace);
                var source = role.equals("control") ? admin.state().control() : admin.state().shards().get(role.substring(6));
                if (source == null) {
                    throw new IllegalArgumentException("receipt shard is not configured");
                }
                BackendDecision result;
                try (var c = source.getConnection()) {
                    MysqlProtocolSchema.requireTable(c, admin.state().prefix() + "_runtime");
                    if (role.equals("control")) {
                        MysqlProtocolSchema.requireTable(c, admin.state().prefix() + "_txn");
                    } else {
                        for (String table : List.of("pc_binding", "pc_write", "pc_rollback")) {
                            MysqlProtocolSchema.requireTable(c, table);
                        }
                    }
                    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    c.setAutoCommit(false);
                    try {
                        var local = admin.state().read(c, role, false);
                        requireAuthority(local, namespace);
                        var decision = lookup.read(c);
                        result = decision != null ? decision
                                : new BackendDecision(
                                        startTs <= local.retiredBefore() ? com.flyingzc.azir.backend.BackendDecision.State.RETIRED : com.flyingzc.azir.backend.BackendDecision.State.PENDING, 0);
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
                admin.verifyGate();
                requireAuthority(admin.state().status(), namespace);
                return result;
            } catch (final SQLException failure) {
                throw storage(failure);
            }
        });
    }

    /**
     * Only the original authority and its freeze replica can be checked, temporary import data cannot impersonate the authority already taken over.
     *
     * @param status status
     * @param namespace persistent protocol namespace
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void requireAuthority(final MysqlProtocolState.Status status, final String namespace) {
        if (!namespace.equals(status.namespace()) || !Set.of("ACTIVE", "FROZEN", "HANDED_OFF").contains(status.mode())) {
            throw new IllegalStateException("receipt authority is restoring, staged or belongs to another namespace");
        }
    }

    /**
     * The damaged timestamp cannot be packaged as normal commit receipt.
     *
     * @param startTs transaction start timestamp
     * @param commitTs durable commit timestamp
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static BackendDecision committed(final long startTs, final long commitTs) throws SQLException {
        if (commitTs <= startTs) {
            throw new SQLException("receipt commit timestamp must exceed start timestamp");
        }
        return new BackendDecision(com.flyingzc.azir.backend.BackendDecision.State.COMMITTED, commitTs);
    }

    /**
     * Read-only takes fixed decision SQL and does not accept operations SQL or changes transaction state.
     */
    @FunctionalInterface
    private interface Lookup {

        /**
         * Read terminal state in the same local snapshot and return empty values when missing.
         *
         * @param c c
         * @return the operation result
         * @throws SQLException if the operation cannot complete
         */
        BackendDecision read(Connection c) throws SQLException;
    }
}
