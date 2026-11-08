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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalOptions;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * Strict configuration of a stand-alone node, receipt only refers to environmental variables and does not provide explicit or unauthenticated regression mode.
 */
public final class RatisNodeConfiguration {

    final Path directory;

    final RaftPeerId node;

    final RaftGroup group;

    final RatisTlsSecurity security;

    final List<Store> stores;

    final Map<String, String> routes;

    final WalOptions transactions;

    final WalBackupOptions backups;

    final MysqlWalReplicaBackendFactory.Options namespaces;

    final RatisTransportLimits transport;

    final RatisManagedNode.Maintenance maintenance;

    final String healthAddress;

    final int healthPort;

    final String applicationClass;

    /**
     * Formats, budget, environment variables and certificates validate are completed before any connection or thread startup.
     *
     * @param file file
     * @return the operation result
     * @throws ConfigurationException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    public static RatisNodeConfiguration load(final Path file) throws IOException {
        var path = Objects.requireNonNull(file).toAbsolutePath().normalize();
        Values values = null;
        try {
            values = new Values(path);
            return new RatisNodeConfiguration(values);
        } catch (final IOException | IllegalArgumentException invalid) {
            // External configuration errors may contain sensitive originals, and public anomalies only provide a fixed description.
            throw new ConfigurationException(values == null ? Phase.FILE : values.phase, invalid);
        }
    }

    /**
     * Only version one configuration is accepted, and unknown keys and repeat keys are rejected to avoid spelling errors that will quietly enable the default value.
     *
     * @param values values
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private RatisNodeConfiguration(final Values values) throws IOException {
        if (!values.required("format.version").equals("1")) {
            throw new IllegalArgumentException("unsupported configuration version");
        }
        values.phase = Phase.MEMBERSHIP;
        String id = values.identifier("node.id");
        node = RaftPeerId.valueOf(id);
        String groupText = values.required("group.id");
        UUID groupId = UUID.fromString(groupText);
        if (!groupId.toString().equals(groupText)) {
            throw new IllegalArgumentException("canonical group UUID required");
        }
        directory = values.path("node.directory");
        if (directory.getParent() == null) {
            throw new IllegalArgumentException("node directory cannot be filesystem root");
        }
        var voters = values.list("group.voters", 3, 3);
        var learners = values.optionalList("group.learners", 0, 6);
        var trusted = values.list("tls.peers", 3, 32);
        if (!Collections.disjoint(voters, learners) || !trusted.containsAll(voters) || !trusted.containsAll(learners)) {
            throw new IllegalArgumentException("voters and learners must have distinct authorized identities");
        }
        var peers = new ArrayList<RaftPeer>();
        var catalog = new ArrayList<RatisTlsSecurity.PeerTrust>();
        for (var peer : trusted) {
            Values.checkIdentifier(peer);
            String address = values.required("peer." + peer + ".address");
            RatisEndpoint.parse(address);
            catalog.add(new RatisTlsSecurity.PeerTrust(peer, address, Set.copyOf(values.list("peer." + peer + ".sha256", 1, 2))));
            if (voters.contains(peer) || learners.contains(peer)) {
                peers.add(RaftPeer.newBuilder().setId(peer).setAddress(address)
                        .setStartupRole(voters.contains(peer) ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build());
            }
        }
        group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        if (group.getPeer(node) == null) {
            throw new IllegalArgumentException("local identity missing from group");
        }
        values.phase = Phase.RAFT_TLS;
        Path identity = values.path("tls.identity");
        Path roots = values.path("tls.truststore");
        char[] identityPassword = values.secret("tls.identity-password-env").toCharArray();
        char[] rootsPassword = null;
        try {
            rootsPassword = values.secret("tls.trust-password-env").toCharArray();
            security = RatisTlsSecurity.load(groupId, id, identity, identityPassword, roots, rootsPassword, catalog);
            security.validate(group, node);
        } finally {
            Arrays.fill(identityPassword, '\0');
            if (rootsPassword != null) {
                Arrays.fill(rootsPassword, '\0');
            }
        }
        values.phase = Phase.MYSQL;
        var configuredStores = new ArrayList<Store>();
        var names = values.list("stores", 1, 16);
        for (var name : names) {
            Values.checkIdentifier(name);
            String prefix = "store." + name + ".";
            String url = values.required(prefix + "jdbc-url");
            validateJdbc(url);
            var trust = values.path(prefix + "truststore");
            String trustPassword = values.secret(prefix + "trust-password-env");
            validateMysqlTrust(trust, trustPassword);
            configuredStores.add(new Store(name, url, values.secret(prefix + "username-env"), values.secret(prefix + "password-env"), trust, trustPassword));
        }
        stores = List.copyOf(configuredStores);
        var configuredRoutes = new LinkedHashMap<String, String>();
        for (var shard : values.list("shards", 1, 64)) {
            Values.checkIdentifier(shard);
            String store = values.required("route." + shard);
            if (!names.contains(store)) {
                throw new IllegalArgumentException("shard route references unknown control pool");
            }
            configuredRoutes.put(shard, store);
        }
        if (!new HashSet<>(configuredRoutes.values()).containsAll(names)) {
            throw new IllegalArgumentException("unused control pool");
        }
        routes = Map.copyOf(configuredRoutes);

        values.phase = Phase.BUDGETS;
        var tx = WalOptions.defaults();
        transactions = new WalOptions(values.integer("tx.max-active", tx.maxActiveTransactions(), 1, 1_000_000),
                values.integer("tx.max-write-keys", tx.maxWriteKeys(), 1, 100_000), values.number("tx.max-write-bytes", tx.maxWriteBytes(), 1, 8L << 20),
                values.integer("tx.queue-capacity", tx.queueCapacity(), 1, 1_000_000), values.integer("tx.commit-batch-size", tx.commitBatchSize(), 1, 1024),
                values.number("tx.batch-delay-ms", tx.batchDelayMillis(), 0, 1000), values.number("tx.max-wal-bytes", tx.maxWalBytes(), 1024, 1L << 50),
                values.number("tx.max-memory-bytes", tx.maxMemoryBytes(), 1, 1L << 40), values.number("tx.timeout-ms", tx.transactionTimeoutMillis(), 1, 86_400_000),
                values.integer("tx.flush-batch-size", tx.flushBatchSize(), 1, 1024), values.number("tx.flush-interval-ms", tx.flushIntervalMillis(), 1, 60_000),
                values.number("tx.hot-cache-bytes", tx.maxHotCacheBytes(), 0, 1L << 40), values.number("tx.max-materialization-lag", tx.maxMaterializationLag(), 1, 1L << 40));
        var backup = WalBackupOptions.defaults();
        backups = new WalBackupOptions(values.number("backup.max-bytes", backup.maxBytes(), 1024, 1L << 50),
                values.integer("backup.batch-keys", backup.batchKeys(), 1, 1024), values.number("backup.batch-bytes", backup.batchBytes(), 1, 16L << 20),
                values.number("backup.timeout-ms", backup.timeoutMillis(), 1, 86_400_000));
        var ns = MysqlWalReplicaBackendFactory.Options.defaults();
        namespaces = new MysqlWalReplicaBackendFactory.Options(values.optional("namespace.prefix", ns.prefix()),
                values.integer("namespace.max-generations", ns.maxGenerationsPerShard(), 1, 128),
                values.integer("namespace.max-schemas", ns.maxSchemasPerControl(), 1, 4096), values.integer("namespace.connections-per-shard", ns.connectionsPerShard(), 1, 16));
        var wire = RatisTransportLimits.defaults();
        transport = new RatisTransportLimits(values.integer("transport.connections", wire.connections(), 1, 4096),
                values.integer("transport.calls", wire.calls(), 1, 4096), values.integer("transport.streams", wire.streamsPerConnection(), 1, 1024),
                values.integer("transport.metadata-bytes", wire.metadataBytes(), 1024, 65536), values.integer("transport.backlog", wire.listenBacklog(), 16, 4096),
                values.number("transport.handshake-ms", wire.handshakeMillis(), 100, 30_000), values.number("transport.idle-ms", wire.idleMillis(), 1000, 3_600_000),
                values.number("transport.connection-age-ms", wire.connectionAgeMillis(), 1000, 86_400_000));
        // Maintenance policy session durable storage; sample visible list of all fields and upgrades should not inadvertently change the existing strategy.
        var h = RatisHealthOptions.defaults();
        var health = new RatisHealthOptions(values.number("maintenance.health.interval-ms", h.intervalMillis(), 100, 60_000),
                values.integer("maintenance.health.failures", h.failures(), 1, 100), values.integer("maintenance.health.recoveries", h.recoveries(), 1, 100),
                values.number("maintenance.health.probe-ms", h.probeMillis(), 100, 10_000), values.number("maintenance.health.transfer-ms", h.transferMillis(), 600, 120_000));
        var g = RatisGcOptions.defaults();
        var gc = new RatisGcOptions(values.number("maintenance.gc.interval-ms", g.intervalMillis(), 100, 60_000),
                values.integer("maintenance.gc.key-limit", g.keyLimit(), 1, 4096), values.integer("maintenance.gc.row-limit", g.rowLimit(), 1, 4096),
                values.number("maintenance.gc.work-ms", g.workMillis(), 1, 5000), values.number("maintenance.gc.stale-ms", g.staleAfterMillis(), 101, 86_400_000));
        var s = RatisSnapshotOptions.defaults();
        var snapshot = new RatisSnapshotOptions(values.number("maintenance.snapshot.interval-ms", s.intervalMillis(), 100, 60_000),
                values.number("maintenance.snapshot.entry-gap", s.entryGap(), 1, 10_000_000), values.number("maintenance.snapshot.max-interval-ms", s.maxIntervalMillis(), 100, 86_400_000),
                values.number("maintenance.snapshot.log-high-water-bytes", s.logHighWaterBytes(), 32L << 20, 1L << 50),
                values.number("maintenance.snapshot.preserve-entries", s.preserveEntries(), 0, 10_000_000));
        maintenance = new RatisManagedNode.Maintenance(health, gc, snapshot,
                values.number("maintenance.sample-ms", 1000, 100, 60_000), values.number("maintenance.stale-ms", 15_000, 101, 86_400_000),
                values.number("maintenance.probe-ms", 1000, 1, 5000));
        values.phase = Phase.HEALTH;
        healthAddress = values.optional("health.address", "127.0.0.1");
        if (!Set.of("127.0.0.1", "::1").contains(healthAddress)) {
            throw new IllegalArgumentException("health endpoint must use a numeric loopback address");
        }
        healthPort = values.integer("health.port", 18080, 1, 65535);
        applicationClass = values.optional("application.class", "");
        if (!applicationClass.isEmpty() && !applicationClass.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+")) {
            throw new IllegalArgumentException("invalid application class name");
        }
        values.phase = Phase.FILE;
        values.finish();
    }

    /**
     * Generates a hosting node configuration, and the data source remains under the responsibility of the outer host.
     *
     * @param pools pools
     * @return the operation result
     */
    RatisManagedNode.Config managed(final Map<String, HikariDataSource> pools) {
        return new RatisManagedNode.Config(directory, node, group, pools, security, transactions, backups, namespaces, transport, maintenance);
    }

    /**
     * The control library must be a single and clear address that prevents URL query parameters from bypassing TLS, timeout or receipt policies.
     *
     * @param url url
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void validateJdbc(final String url) {
        if (!url.startsWith("jdbc:mysql://") || url.length() > 1024) {
            throw new IllegalArgumentException("explicit MySQL JDBC endpoint required");
        }
        URI uri = URI.create(url.substring(5));
        if (uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535 || uri.getRawUserInfo() != null || uri.getRawQuery() != null
                || uri.getRawFragment() != null || !uri.getRawPath().matches("/[A-Za-z0-9_]{1,64}")) {
            throw new IllegalArgumentException("single MySQL endpoint and explicit control database required");
        }
        RatisEndpoint.of(uri.getHost(), uri.getPort());
        if (Set.of("mysql", "sys", "information_schema", "performance_schema").contains(uri.getRawPath().substring(1).toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("system database cannot be a control database");
        }
    }

    /**
     * Offline validate MySQL exclusive trust library, Web hostname verify is still executed by Contractor/J at connection.
     *
     * @param path path
     * @param password password
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void validateMysqlTrust(final Path path, final String password) throws IOException {
        char[] chars = password.toCharArray();
        try {
            var store = KeyStore.getInstance("PKCS12");
            byte[] bytes = boundedFile(path, 1_048_576);
            try {
                store.load(new java.io.ByteArrayInputStream(bytes), chars);
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
            if (store.size() < 1 || store.size() > 16) {
                throw new IllegalArgumentException("bounded MySQL CA trust required");
            }
            for (var alias : Collections.list(store.aliases())) {
                if (store.isKeyEntry(alias) || !(store.getCertificate(alias) instanceof X509Certificate ca) || ca.getBasicConstraints() < 0) {
                    throw new IllegalArgumentException("MySQL trust store must contain CA certificates only");
                }
                ca.checkValidity();
            }
        } catch (final java.security.GeneralSecurityException invalid) {
            throw new IOException("invalid MySQL CA trust store", invalid);
        } finally {
            Arrays.fill(chars, '\0');
        }
    }

    /**
     * Normal files are unconnected, bounded readable and file growth cannot break memory budget.
     *
     * @param path path
     * @param limit maximum number of items
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static byte[] boundedFile(final Path path, final int limit) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > limit) {
            throw new IOException("configuration resource missing or exceeds budget");
        }
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length > limit) {
                throw new IOException("configuration resource grew beyond budget");
            }
            return bytes;
        }
    }

    /**
     * Public diagnostics do not contain path, data source configuration and receipt objects.
     *
     * @return the operation result
     */
    @Override
    public String toString() {
        return "NodeConfiguration[node=" + node + ", group=" + group.getGroupId() + ", credentials=redacted]";
    }

    /**
     * Fixed diagnostic stages contain only code-defined names and do not carry configuration originals or passwords.
     */
    public enum Phase {
        FILE, MEMBERSHIP, RAFT_TLS, MYSQL, BUDGETS, HEALTH
    }

    /**
     * Commencing the rejected diagnostic retention security phase for bottom reasons only for controlled local, which do not directly output logs.
     */
    public static final class ConfigurationException extends IOException {

        private final Phase phase;

        /**
         * Use a fixed-stage description and do not spell file contents, URLs or receipt in outer messages.
         *
         * @param phase phase
         * @param cause cause
         */
        private ConfigurationException(final Phase phase, final Exception cause) {
            super("node configuration rejected in phase " + phase, cause);
            this.phase = phase;
        }

        /**
         * The operator can use a stage location problem and cannot rely on an abnormal message to carry sensitive connection details.
         *
         * @return the operation result
         */
        public Phase phase() {
            return phase;
        }
    }

    /**
     * The private assembly data of the single control pool is forbidden by default record output password and JDBC address.
     *
     * @param name name
     * @param url url
     * @param username username
     * @param password password
     * @param trust trust
     * @param trustPassword trust password
     */
    record Store(String name, String url, String username, String password, Path trust, String trustPassword) {

        /**
         * Build inert connection pool, database connection and durable checks to start the process at the official node.
         *
         * @return the operation result
         */
        HikariDataSource pool() {
            var pool = new HikariDataSource();
            try {
                pool.setPoolName("azir-control-" + name);
                pool.setJdbcUrl(url);
                pool.setUsername(username);
                pool.setPassword(password);
                pool.setMinimumIdle(0);
                pool.setMaximumPoolSize(2);
                pool.setConnectionTimeout(5000);
                pool.setValidationTimeout(1000);
                pool.setAutoCommit(true);
                pool.addDataSourceProperty("connectTimeout", "3000");
                pool.addDataSourceProperty("socketTimeout", "5000");
                pool.addDataSourceProperty("sslMode", "VERIFY_IDENTITY");
                pool.addDataSourceProperty("trustCertificateKeyStoreUrl", trust.toUri().toASCIIString());
                pool.addDataSourceProperty("trustCertificateKeyStoreType", "PKCS12");
                pool.addDataSourceProperty("trustCertificateKeyStorePassword", trustPassword);
                pool.addDataSourceProperty("fallbackToSystemTrustStore", "false");
                pool.addDataSourceProperty("allowPublicKeyRetrieval", "false");
                pool.addDataSourceProperty("rewriteBatchedStatements", "true");
                return pool;
            } catch (final RuntimeException | Error failure) {
                pool.close();
                throw failure;
            }
        }

        /**
         * Diagnosis can only identify the logical control pool and cannot output connection parameters.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return "Store[name=" + name + ", credentials=redacted]";
        }
    }

    /**
     * Bounded Properties Reader, used keys are removed so that the unknown configuration is finally found.
     */
    private static final class Values {

        private final Path parent;

        private final Properties properties;

        private Phase phase = Phase.FILE;

        /**
         * UTF-8 is strictly decoded and repeat keys, including those with the same name after conversion, are rejected.
         *
         * @param file file
         * @throws IOException if the operation cannot complete
         */
        Values(final Path file) throws IOException {
            parent = file.getParent();
            properties = new UniqueProperties();
            var decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
            properties.load(new StringReader(decoder.decode(ByteBuffer.wrap(boundedFile(file, 65_536))).toString()));
        }

        /**
         * Text will not accept empty string and control characters.
         *
         * @param key business key
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        String required(final String key) {
            String value = (String) properties.remove(key);
            if (value == null || value.isBlank() || value.length() > 4096 || value.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("missing or invalid configuration field: " + key);
            }
            return value.strip();
        }

        /**
         * Default values only apply to configuration keys that are not declared at all.
         *
         * @param key business key
         * @param fallback fallback
         * @return the operation result
         */
        String optional(final String key, final String fallback) {
            return properties.containsKey(key) ? required(key) : fallback;
        }

        /**
         * Paths do not execute variable replacement, command or wildcards relative to configuration file resolution.
         *
         * @param key business key
         * @return the operation result
         */
        Path path(final String key) {
            return parent.resolve(required(key)).normalize().toAbsolutePath();
        }

        /**
         * The identifier is limited to the ASCII range that can be shown safely.
         *
         * @param key business key
         * @return the operation result
         */
        String identifier(final String key) {
            String value = required(key);
            checkIdentifier(value);
            return value;
        }

        /**
         * Node, shard and control pool names reject space, separator and super long input.
         *
         * @param value value to write
         * @throws IllegalArgumentException if the operation cannot complete
         */
        static void checkIdentifier(final String value) {
            if (!value.matches("[A-Za-z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("invalid configuration identity");
            }
        }

        /**
         * The environment variable name is publicly available, but the password value does not spell abnormal information.
         *
         * @param key business key
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        String secret(final String key) {
            String name = required(key);
            if (!name.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")) {
                throw new IllegalArgumentException("invalid environment reference: " + key);
            }
            String value = System.getenv(name);
            if (value == null || value.isEmpty() || value.length() > 4096 || value.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("missing or invalid environment value for: " + key);
            }
            return value;
        }

        /**
         * Bounded Integers prohibit spills, hidden units and unlimited values.
         *
         * @param key business key
         * @param fallback fallback
         * @param min min
         * @param max max
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        long number(final String key, final long fallback, final long min, final long max) {
            long value = properties.containsKey(key) ? Long.parseLong(required(key)) : fallback;
            if (value < min || value > max) {
                throw new IllegalArgumentException("configuration budget outside supported range: " + key);
            }
            return value;
        }

        /**
         * Zoom in to int after the scope check has been completed.
         *
         * @param key business key
         * @param fallback fallback
         * @param min min
         * @param max max
         * @return the operation result
         */
        int integer(final String key, final int fallback, final int min, final int max) {
            return Math.toIntExact(number(key, fallback, min, max));
        }

        /**
         * The list of empty elements, duplicate values or more than budget failed.
         *
         * @param key business key
         * @param min min
         * @param max max
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        List<String> list(final String key, final int min, final int max) {
            var values = Arrays.stream(required(key).split(",", -1)).map(String::strip).toList();
            if (values.size() < min || values.size() > max || values.stream().anyMatch(String::isEmpty) || new HashSet<>(values).size() != values.size()) {
                throw new IllegalArgumentException("invalid bounded configuration list: " + key);
            }
            return values;
        }

        /**
         * The optional list is shown as empty by missing keys and does not mask the error configuration with an empty value.
         *
         * @param key business key
         * @param min min
         * @param max max
         * @return the operation result
         */
        List<String> optionalList(final String key, final int min, final int max) {
            return properties.containsKey(key) ? list(key, min, max) : List.of();
        }

        /**
         * Do not output the original unknown key, so that the wrong password is not used to cause the diagnosis leak.
         *
         * @throws IllegalArgumentException if the operation cannot complete
         */
        void finish() {
            if (!properties.isEmpty()) {
                throw new IllegalArgumentException("unknown node configuration fields");
            }
        }
    }

    /**
     * Propertiers has duplicate key protection for original solvers, retaining standard transliteration and continuation syntax.
     */
    private static final class UniqueProperties extends Properties {

        /**
         * Prohibits after-writing to overwrite, so that the configuration audit is not consistent with actual start value.
         *
         * @param key business key
         * @param value value to write
         * @return the operation result
         * @throws IllegalArgumentException if the operation cannot complete
         */
        @Override
        public synchronized Object put(final Object key, final Object value) {
            if (containsKey(key)) {
                throw new IllegalArgumentException("duplicate node configuration field");
            }
            return super.put(key, value);
        }
    }
}
