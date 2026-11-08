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

import com.flyingzc.azir.percolator.MysqlPercolatorLifecycle;
import com.flyingzc.azir.percolator.MysqlPercolatorStore;
import com.flyingzc.azir.percolator.PercolatorRuntime;
import com.flyingzc.azir.percolator.PercolatorStore;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.flyingzc.azir.tso.mysql.MysqlTimestampOracle;
import javax.sql.DataSource;

import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * Old version independent Percolator 's visible offline upgrade; Back window closes before any new business timestamp allocation.
 */
public final class MysqlProtocolUpgrade {

    /**
     * Prohibits the example of static upgrade tool.
     */
    private MysqlProtocolUpgrade() {
    }

    /**
     * Verify Create binding after you have stopped writing the old space, keep all business values and original timestamp and return to freeze receipt.
     *
     * @param control control
     * @param shards shards
     * @param offline offline
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static MysqlProtocolAdmin.Checkpoint adoptOffline(final DataSource control, final Map<String, DataSource> shards, final boolean offline) {
        if (!offline) {
            throw new IllegalArgumentException("legacy upgrade requires explicit offline mode");
        }
        var safeControl = new com.flyingzc.azir.control.ProtocolMysqlDataSource(control);
        var safeShards = new TreeMap<String, DataSource>();
        shards.forEach((id, source) -> safeShards.put(id, new com.flyingzc.azir.control.ProtocolMysqlDataSource(source)));
        var lifecycle = MysqlPercolatorLifecycle.adoptLegacyOffline(safeControl, safeShards, 60_000);
        var stores = new TreeMap<String, PercolatorStore>();
        safeShards.forEach((id, source) -> {
            var store = new MysqlPercolatorStore(id, source, lifecycle.operations());
            store.initialize();
            stores.put(id, store);
        });
        try (var runtime = new PercolatorRuntime(new MysqlTimestampOracle(safeControl), stores, PercolatorRuntime.Options.defaults(), lifecycle)) {
            var admin = runtime.administration();
            var token = admin.freeze();
            long sequence = MysqlProtocolAdmin.scalar(safeControl, "select current_value from mvcc_tso where sequence_name='global'");
            admin.locked(() -> {
                for (var role : MysqlProtocolBackup.roles(admin).entrySet()) {
                    admin.change(role.getValue(), role.getKey(), token.generation(), "FROZEN", token.id(), "legacy-sequence:" + sequence);
                }
                return null;
            });
            return token;
        }
    }

    /**
     * Unable to remove new bindings when no new transaction, GC or timestamp allocation is enabled; old business data and TSO are not deleted.
     *
     * @param admin admin
     * @param offline offline
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void rollbackOffline(final MysqlProtocolAdmin admin, final boolean offline) {
        admin.requireWritable();
        if (!offline || !admin.state().prefix().equals("pc")) {
            throw new IllegalArgumentException("offline Percolator rollback required");
        }
        admin.locked(() -> {
            var status = admin.state().status();
            String plan = status.destination();
            if (!status.mode().equals("FROZEN") || plan == null || !plan.startsWith("legacy-sequence:")) {
                throw new IllegalStateException("legacy rollback window closed");
            }
            long expected = Long.parseLong(plan.substring("legacy-sequence:".length()));
            if (MysqlProtocolAdmin.scalar(admin.state().control(), "select current_value from mvcc_tso where sequence_name='global'") != expected) {
                throw new IllegalStateException("new timestamps allocated; restore a verified backup instead");
            }
            for (var role : admin.status().values()) {
                if (!role.mode().equals("FROZEN") || role.retiredBefore() != 0 || !Objects.equals(role.destination(), plan)) {
                    throw new IllegalStateException("not a pristine frozen upgrade");
                }
            }
            for (var source : admin.state().shards().values()) {
                try (var c = source.getConnection(); var q = c.createStatement()) {
                    q.execute("drop table if exists pc_binding");
                    q.execute("drop table if exists pc_runtime");
                } catch (final SQLException failure) {
                    throw storage(failure);
                }
            }
            try (var c = admin.state().control().getConnection(); var q = c.createStatement()) {
                q.execute("drop table if exists pc_readers");
                q.execute("drop table if exists pc_control");
                q.execute("drop table if exists pc_runtime");
            } catch (final SQLException failure) {
                throw storage(failure);
            }
            return null;
        });
    }

    /**
     * Offline takeovers must be free of locks, isolated payload, bound to identity or regressive time lines.
     *
     * @param control control
     * @param shards shards
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    public static void validateLegacy(final DataSource control, final Map<String, DataSource> shards) {
        long sequence;
        try (var c = control.getConnection(); var q = c.createStatement(); var r = q.executeQuery("select current_value from mvcc_tso where sequence_name='global'")) {
            MysqlProtocolSchema.requireLegacyTable(c, "mvcc_tso");
            if (!r.next() || r.getLong(1) <= 0) {
                throw new SQLException("legacy timestamp missing");
            }
            sequence = r.getLong(1);
        } catch (final SQLException failure) {
            throw storage(failure);
        }
        for (var source : shards.values()) {
            try (var connection = source.getConnection()) {
                for (String table : List.of("pc_guard", "pc_data", "pc_lock", "pc_write", "pc_rollback")) {
                    MysqlProtocolSchema.requireLegacyTable(connection, table);
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
            if (exists(source, "pc_binding") && MysqlProtocolAdmin.scalar(source, "select count(*) from pc_binding") != 0) {
                throw new IllegalArgumentException("legacy data already bound");
            }
            if (exists(source, "pc_runtime") && MysqlProtocolAdmin.scalar(source, "select count(*) from pc_runtime") != 0) {
                throw new IllegalArgumentException("legacy data has runtime fencing; refuse adoption");
            }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_lock") != 0) {
                throw new IllegalArgumentException("drain legacy locks before upgrade");
            }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_data d where not exists(select 1 from pc_write w where w.user_key=d.user_key and w.start_ts=d.start_ts)") != 0) {
                throw new IllegalArgumentException("orphan legacy prewrites");
            }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_write where commit_ts<=start_ts or start_ts<=0") != 0) {
                throw new IllegalArgumentException("invalid legacy decision timestamps");
            }
            if (MysqlProtocolAdmin.scalar(source, "select count(*) from pc_data d where not exists(select 1 from pc_guard g where g.user_key=d.user_key)") != 0) {
                throw new IllegalArgumentException("legacy candidate index incomplete");
            }
            for (String sql : List.of("select coalesce(max(commit_ts),0) from pc_write", "select coalesce(max(start_ts),0) from pc_data", "select coalesce(max(start_ts),0) from pc_rollback")) {
                if (MysqlProtocolAdmin.scalar(source, sql) > sequence) {
                    throw new IllegalArgumentException("legacy TSO is behind persisted versions");
                }
            }
        }
    }

    /**
     * Only search for a known protocol table name and do not accept external SQL clips.
     *
     * @param source database connection source
     * @param table table
     * @return the operation result
     */
    private static boolean exists(final DataSource source, final String table) {
        try (var c = source.getConnection(); var q = c.prepareStatement("select count(*) from information_schema.tables where table_schema=database() and table_name=?")) {
            q.setString(1, table);
            try (var r = q.executeQuery()) {
                r.next();
                return r.getInt(1) > 0;
            }
        } catch (final SQLException failure) {
            throw storage(failure);
        }
    }
}
