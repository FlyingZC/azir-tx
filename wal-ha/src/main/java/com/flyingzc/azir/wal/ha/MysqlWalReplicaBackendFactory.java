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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.wal.MysqlWalMaterializedStore;
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Distribution of MySQL database to stand-alone replica generation, control pool owned by caller and operations pool owned by lease.
 */
public final class MysqlWalReplicaBackendFactory implements WalReplicaBackendFactory {

    private final Map<String, HikariDataSource> controls;

    private final Options options;

    private final Hook hook;

    private final RetirementHook retirementHook;

    private final Map<String, String> controlIds;

    /**
     * Control database must be clearly provided by the caller and the account must create database of the namespace plant.
     *
     * @param controls controls
     * @param options options
     */
    public MysqlWalReplicaBackendFactory(final Map<String, HikariDataSource> controls, final Options options) {
        this(controls, options, stage -> {
        });
    }

    /**
     * The failure injection in the package is used only for verify real registration, DDL, tags and connection pool life cycles.
     *
     * @param controls controls
     * @param options options
     * @param hook hook
     */
    MysqlWalReplicaBackendFactory(final Map<String, HikariDataSource> controls, final Options options, final Hook hook) {
        this(controls, options, hook, stage -> {
        });
    }

    /**
     * Two types of failure boundary are separate and do not merge the retirement border into initialize interface.
     *
     * @param controls controls
     * @param options options
     * @param hook hook
     * @param retirementHook retirement hook
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    MysqlWalReplicaBackendFactory(final Map<String, HikariDataSource> controls, final Options options, final Hook hook, final RetirementHook retirementHook) {
        this.controls = Map.copyOf(controls);
        this.options = java.util.Objects.requireNonNull(options);
        this.hook = java.util.Objects.requireNonNull(hook);
        this.retirementHook = java.util.Objects.requireNonNull(retirementHook);
        var ids = new HashMap<String, String>();
        if (controls.isEmpty() || controls.size() > 1024) {
            throw new IllegalArgumentException("invalid MySQL generation routes");
        }
        for (var entry : this.controls.entrySet()) {
            new WalMutation(entry.getKey(), "", null);
            if (entry.getKey().contains("\u0000")) {
                throw new IllegalArgumentException("invalid MySQL generation shard");
            }
            new MysqlWalMaterializedStore(entry.getValue()).validateProduction();
            try (var connection = entry.getValue().getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select @@server_uuid,database()")) {
                if (!row.next() || row.getString(2) == null) {
                    throw new IllegalStateException("explicit namespace control database required");
                }
                ids.put(entry.getKey(), hash(row.getString(1) + "\u0000" + row.getString(2)));
            } catch (final java.sql.SQLException unavailable) {
                throw new IllegalStateException("cannot bind namespace control endpoint", unavailable);
            }
        }
        controlIds = Map.copyOf(ids);
    }

    /**
     * Opens the same generation reuse of the same space, the new generation uses an independent space and failed to close all created own pools.
     *
     * @param identity identity
     * @param generation generation
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    @Override
    public Lease open(final WalReplicaIdentity identity, final UUID generation) throws IOException {
        var pools = new ArrayList<HikariDataSource>();
        var stores = new HashMap<String, WalMaterializedStore>();
        var guard = MysqlNamespaceLeases.open(leaseKeys(identity, generation));
        var cleanup = new RatisCloseSequence(() -> closePools(pools), guard::close);
        try {
            for (String shard : controls.keySet().stream().sorted().toList()) {
                var control = controls.get(shard);
                String schema = databaseName(identity, generation, shard);
                new MysqlReplicaNamespaces(control, options, hook).provision(identity, generation, shard, schema);
                var config = new HikariConfig();
                control.copyStateTo(config);
                config.setCatalog(schema);
                config.setSchema(null);
                config.setPoolName("azir-generation-" + hash(schema).substring(0, 16));
                config.setMinimumIdle(0);
                config.setMaximumPoolSize(options.connectionsPerShard());
                config.setAutoCommit(true);
                var pool = new HikariDataSource(config);
                pools.add(pool);
                try (var connection = pool.getConnection()) {
                    try (var query = connection.createStatement(); var row = query.executeQuery("select database()")) {
                        if (!row.next() || !schema.equals(row.getString(1))) {
                            throw new IOException("driver did not select owned generation database");
                        }
                    }
                    MysqlReplicaNamespaces.verifyOpened(connection, schema, owner(identity, generation, shard, schema));
                }
                var store = new MysqlWalMaterializedStore(pool);
                store.validateProduction();
                stores.put(shard, store);
                hook.hit(Stage.AFTER_POOL_OPEN);
            }
            return new Lease(stores, cleanup::close);
        } catch (final Throwable failure) {
            throw RatisStartupException.afterFailure(failure, cleanup);
        }
    }

    /**
     * MySQL achieves durable tombstone and idempotent level releases.
     *
     * @return the operation result
     */
    @Override
    public boolean supportsRetirement() {
        return true;
    }

    /**
     * Cursor is accompanied by routed and physical control endpoints, and the configuration changes cannot continue to delete another group of spaces.
     *
     * @param permit permit
     * @param cursor cursor
     * @param shardLimit shard limit
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @Override
    public RetirementProgress retire(final WalGenerationRetirement permit, final String cursor, final int shardLimit) throws IOException {
        permit.verify();
        if (shardLimit < 1 || shardLimit > 1024) {
            throw new IllegalArgumentException("invalid retirement shard budget");
        }
        var shards = controls.keySet().stream().sorted().toList();
        String layout = hash(shards.stream().map(shard -> shard + "\u0000" + controlIds.get(shard)).collect(java.util.stream.Collectors.joining("\u0000")));
        int offset = 0;
        if (!cursor.isEmpty()) {
            String[] parts = cursor.split(":", -1);
            if (parts.length != 2 || !layout.equals(parts[0])) {
                throw new IOException("retirement layout changed");
            }
            try {
                offset = Integer.parseInt(parts[1]);
            } catch (final NumberFormatException invalid) {
                throw new IOException("invalid retirement cursor", invalid);
            }
        }
        if (offset < 0 || offset > shards.size()) {
            throw new IOException("retirement cursor outside layout");
        }
        int inspected = 0, dropped = 0;
        try (var ignored = MysqlNamespaceLeases.retire(leaseKeys(permit.identity(), permit.generation()))) {
            while (offset < shards.size() && inspected < shardLimit) {
                permit.verify();
                String shard = shards.get(offset);
                if (new MysqlReplicaNamespaces(controls.get(shard), options, hook).retire(permit, shard, databaseName(permit.identity(), permit.generation(), shard), retirementHook)) {
                    dropped++;
                }
                offset++;
                inspected++;
            }
        }
        return new RetirementProgress(layout + ":" + offset, offset == shards.size(), inspected, dropped);
    }

    /**
     * The registration key only contains a physical endpoint summary, which neither leaks receipt nor wrongly identifies a space by reference to the data source object.
     *
     * @param identity identity
     * @param generation generation
     * @return the operation result
     */
    private java.util.List<String> leaseKeys(final WalReplicaIdentity identity, final UUID generation) {
        return controls.keySet().stream().map(shard -> hash(controlIds.get(shard) + "\u0000" + databaseName(identity, generation, shard))).toList();
    }

    /**
     * The derivative name contains nodes, complete generation UUID and shard digests, while the actual ownership also has to pass the full fingerprint verify.
     *
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public String databaseName(final WalReplicaIdentity identity, final UUID generation, final String shard) {
        java.util.Objects.requireNonNull(generation);
        if (!controls.containsKey(shard)) {
            throw new IllegalArgumentException("unknown MySQL generation shard");
        }
        return options.prefix() + "_" + nodeOwner(identity).substring(0, 8) + "_" + generation.toString().replace("-", "") + "_" + hash(shard).substring(0, 8);
    }

    /**
     * Node ownership uses all identity fields and does not use a short database summary as the final authorization.
     *
     * @param identity identity
     * @return the operation result
     */
    static String nodeOwner(final WalReplicaIdentity identity) {
        return hash(identity.engineId() + "\u0000" + identity.groupId() + "\u0000" + identity.nodeId());
    }

    /**
     * Full space identity binds the nodes, generation, shard and physical database names.
     *
     * @param identity identity
     * @param generation generation
     * @param shard logical shard identifier
     * @param schema schema
     * @return the operation result
     */
    static String owner(final WalReplicaIdentity identity, final UUID generation, final String shard, final String schema) {
        return hash(nodeOwner(identity) + "\u0000" + generation + "\u0000" + shard + "\u0000" + schema);
    }

    /**
     * Fixed UTF-8 Summary contains only identity fields and does not contain database receipt.
     *
     * @param value value to write
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private static String hash(final String value) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (final java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * Only the business pool created at this plant was released, and the control ponds and database were managed by other defined life cycles.
     *
     * @param pools pools
     * @throws IOException if the operation cannot complete
     */
    private static void closePools(final java.util.List<HikariDataSource> pools) throws IOException {
        RuntimeException failure = null;
        for (var iterator = pools.iterator(); iterator.hasNext();) {
            try {
                iterator.next().close();
                iterator.remove();
            } catch (final RuntimeException close) {
                if (failure == null) {
                    failure = close;
                } else {
                    failure.addSuppressed(close);
                }
            }
        }
        if (failure != null) {
            throw new IOException("generation connection pools did not close", failure);
        }
    }

    /**
     * Namespace and connection are limited in number budget, and the old generation retirement did not automatically release the allocation.
     *
     * @param prefix prefix
     * @param maxGenerationsPerShard max generations per shard
     * @param maxSchemasPerControl max schemas per control
     * @param connectionsPerShard connections per shard
     */
    public record Options(String prefix, int maxGenerationsPerShard, int maxSchemasPerControl, int connectionsPerShard) {

        /**
         * Prefixes only allow short ASCII identifiers, with database names not exceeding the sixty-four byte limit of MySQL.
         *
         * @param prefix prefix
         * @param maxGenerationsPerShard max generations per shard
         * @param maxSchemasPerControl max schemas per control
         * @param connectionsPerShard connections per shard
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Options {
            if (prefix == null || !prefix.matches("[a-z][a-z0-9_]{0,7}") || maxGenerationsPerShard < 1 || maxGenerationsPerShard > 128
                    || maxSchemasPerControl < 1 || maxSchemasPerControl > 4096 || connectionsPerShard < 1 || connectionsPerShard > 16) {
                throw new IllegalArgumentException("invalid MySQL generation limits");
            }
        }

        /**
         * Default for one hundred and twenty-eight database and two operations per shard for eight generations.
         *
         * @return the operation result
         */
        public static Options defaults() {
            return new Options("azw", 8, 128, 2);
        }
    }

    /**
     * The actual failure boundary of the initialization of the new space is not exposed to normal operations.
     */
    enum Stage {
        AFTER_RESERVATION, AFTER_CREATE_DATABASE, AFTER_MARKER, AFTER_READY, AFTER_POOL_OPEN
    }

    /**
     * Real retirement log, DDL and the failure boundary released by a line.
     */
    enum RetirementStage {
        AFTER_RETIRING, AFTER_DATABASE_DROP, AFTER_RELEASE
    }

    /**
     * The package verify retirement is interrupted to continue and the formal construction default is not injected.
     */
    @FunctionalInterface
    interface RetirementHook {

        /**
         * Failed to inject after the real durable border.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        void hit(RetirementStage stage) throws IOException;
    }

    /**
     * Each border failed to retain the already durable storage distribution identity.
     */
    @FunctionalInterface
    interface Hook {

        /**
         * The injection failed after the actual operation was completed.
         *
         * @param stage stage
         * @throws IOException if the operation cannot complete
         */
        void hit(Stage stage) throws IOException;
    }
}
