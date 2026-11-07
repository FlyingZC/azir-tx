package com.flyingzc.azir.wal.ha;

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
import java.util.*;

/** 独立节点的严格配置，凭据仅引用环境变量，不提供明文或不认证的回退模式。 */
public final class RatisNodeConfiguration {
    /** 固定诊断阶段只含代码定义的名称，不携带配置原文或密码。 */
    public enum Phase { FILE, MEMBERSHIP, RAFT_TLS, MYSQL, BUDGETS, HEALTH }

    /** 启动拒绝的诊断保留安全阶段，底层原因仅供受控本地排查，不直接输出日志。 */
    public static final class ConfigurationException extends IOException {
        private final Phase phase;

        /** 使用固定阶段描述，不把文件内容、URL 或凭据拼进外层消息。 */
        private ConfigurationException(Phase phase, Exception cause) {
            super("node configuration rejected in phase " + phase, cause);
            this.phase = phase;
        }

        /** 操作者可用阶段定位问题，不能依赖异常消息携带敏感连接细节。 */
        public Phase phase() { return phase; }
    }

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

    /** 单一控制池的私有装配数据，禁止通过默认 record 输出密码和 JDBC 地址。 */
    record Store(String name, String url, String username, String password, Path trust, String trustPassword) {
        /** 构建惰性连接池，数据库连接和持久性检查留给正式节点启动流程。 */
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
            } catch (RuntimeException | Error failure) {
                pool.close();
                throw failure;
            }
        }

        /** 诊断只能辨认逻辑控制池，不能输出连接参数。 */
        @Override public String toString() { return "Store[name=" + name + ", credentials=redacted]"; }
    }

    /** 在任何连接或线程启动之前，完成格式、预算、环境变量及证书校验。 */
    public static RatisNodeConfiguration load(Path file) throws IOException {
        var path = Objects.requireNonNull(file).toAbsolutePath().normalize();
        Values values = null;
        try {
            values = new Values(path);
            return new RatisNodeConfiguration(values);
        } catch (IOException | IllegalArgumentException invalid) {
            // 外部配置错误可能包含敏感原文，公共异常只提供固定说明。
            throw new ConfigurationException(values == null ? Phase.FILE : values.phase, invalid);
        }
    }

    /** 只接受版本一配置，未知键与重复键一律拒绝，避免拼写错误悄悄启用默认值。 */
    private RatisNodeConfiguration(Values values) throws IOException {
        if (!values.required("format.version").equals("1")) { throw new IllegalArgumentException("unsupported configuration version"); }
        values.phase = Phase.MEMBERSHIP;
        String id = values.identifier("node.id");
        node = RaftPeerId.valueOf(id);
        String groupText = values.required("group.id");
        UUID groupId = UUID.fromString(groupText);
        if (!groupId.toString().equals(groupText)) { throw new IllegalArgumentException("canonical group UUID required"); }
        directory = values.path("node.directory");
        if (directory.getParent() == null) { throw new IllegalArgumentException("node directory cannot be filesystem root"); }
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
        if (group.getPeer(node) == null) { throw new IllegalArgumentException("local identity missing from group"); }
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
            if (rootsPassword != null) { Arrays.fill(rootsPassword, '\0'); }
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
            if (!names.contains(store)) { throw new IllegalArgumentException("shard route references unknown control pool"); }
            configuredRoutes.put(shard, store);
        }
        if (!new HashSet<>(configuredRoutes.values()).containsAll(names)) { throw new IllegalArgumentException("unused control pool"); }
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
        // 维护策略会持久化；样例显式列出所有字段，升级时不能无意改变已存在的策略。
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
        if (!Set.of("127.0.0.1", "::1").contains(healthAddress)) { throw new IllegalArgumentException("health endpoint must use a numeric loopback address"); }
        healthPort = values.integer("health.port", 18080, 1, 65535);
        applicationClass = values.optional("application.class", "");
        if (!applicationClass.isEmpty() && !applicationClass.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+")) {
            throw new IllegalArgumentException("invalid application class name");
        }
        values.phase = Phase.FILE;
        values.finish();
    }

    /** 生成托管节点配置，数据源仍由外层宿主负责关闭。 */
    RatisManagedNode.Config managed(Map<String, HikariDataSource> pools) {
        return new RatisManagedNode.Config(directory, node, group, pools, security, transactions, backups, namespaces, transport, maintenance);
    }

    /** 控制库必须是单一明确地址，禁止 URL 查询参数绕过 TLS、超时或凭据策略。 */
    private static void validateJdbc(String url) {
        if (!url.startsWith("jdbc:mysql://") || url.length() > 1024) { throw new IllegalArgumentException("explicit MySQL JDBC endpoint required"); }
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

    /** 离线校验 MySQL 专属信任库，网络主机名验证仍由 Connector/J 在连接时执行。 */
    private static void validateMysqlTrust(Path path, String password) throws IOException {
        char[] chars = password.toCharArray();
        try {
            var store = KeyStore.getInstance("PKCS12");
            byte[] bytes = boundedFile(path, 1_048_576);
            try { store.load(new java.io.ByteArrayInputStream(bytes), chars); }
            finally { Arrays.fill(bytes, (byte) 0); }
            if (store.size() < 1 || store.size() > 16) { throw new IllegalArgumentException("bounded MySQL CA trust required"); }
            for (var alias : Collections.list(store.aliases())) {
                if (store.isKeyEntry(alias) || !(store.getCertificate(alias) instanceof X509Certificate ca) || ca.getBasicConstraints() < 0) {
                    throw new IllegalArgumentException("MySQL trust store must contain CA certificates only");
                }
                ca.checkValidity();
            }
        } catch (java.security.GeneralSecurityException invalid) {
            throw new IOException("invalid MySQL CA trust store", invalid);
        } finally { Arrays.fill(chars, '\0'); }
    }

    /** 普通文件采用无链接、有界读取，文件增长也不能突破内存预算。 */
    private static byte[] boundedFile(Path path, int limit) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > limit) { throw new IOException("configuration resource missing or exceeds budget"); }
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length > limit) { throw new IOException("configuration resource grew beyond budget"); }
            return bytes;
        }
    }

    /** 公共诊断不包含路径、数据源配置和凭据对象。 */
    @Override public String toString() { return "NodeConfiguration[node=" + node + ", group=" + group.getGroupId() + ", credentials=redacted]"; }

    /** 有界属性读取器，消耗过的键被移除，以便最终发现未知配置。 */
    private static final class Values {
        private final Path parent;
        private final Properties properties;
        private Phase phase = Phase.FILE;

        /** UTF-8 严格解码，重复键包括转义后同名的键也会拒绝。 */
        Values(Path file) throws IOException {
            parent = file.getParent();
            properties = new UniqueProperties();
            var decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
            properties.load(new StringReader(decoder.decode(ByteBuffer.wrap(boundedFile(file, 65_536))).toString()));
        }

        /** 必填文本不接受空串与控制字符。 */
        String required(String key) {
            String value = (String) properties.remove(key);
            if (value == null || value.isBlank() || value.length() > 4096 || value.chars().anyMatch(Character::isISOControl)) { throw new IllegalArgumentException("missing or invalid configuration field: " + key); }
            return value.strip();
        }

        /** 缺省值仅适用于完全没有声明的配置键。 */
        String optional(String key, String fallback) { return properties.containsKey(key) ? required(key) : fallback; }

        /** 路径相对于配置文件解析，不执行变量替换、命令或通配符。 */
        Path path(String key) { return parent.resolve(required(key)).normalize().toAbsolutePath(); }

        /** 标识符限制为可安全显示的 ASCII 范围。 */
        String identifier(String key) { String value = required(key); checkIdentifier(value); return value; }

        /** 节点、分片和控制池名称均拒绝空格、分隔符及超长输入。 */
        static void checkIdentifier(String value) { if (!value.matches("[A-Za-z0-9._-]{1,64}")) { throw new IllegalArgumentException("invalid configuration identity"); } }

        /** 环境变量名可公开，但密码值绝不拼进异常信息。 */
        String secret(String key) {
            String name = required(key);
            if (!name.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")) { throw new IllegalArgumentException("invalid environment reference: " + key); }
            String value = System.getenv(name);
            if (value == null || value.isEmpty() || value.length() > 4096 || value.indexOf('\0') >= 0) { throw new IllegalArgumentException("missing or invalid environment value for: " + key); }
            return value;
        }

        /** 有界整数禁止溢出、隐式单位和无限值。 */
        long number(String key, long fallback, long min, long max) {
            long value = properties.containsKey(key) ? Long.parseLong(required(key)) : fallback;
            if (value < min || value > max) { throw new IllegalArgumentException("configuration budget outside supported range: " + key); }
            return value;
        }

        /** 在完成范围检查后才缩窄为 int。 */
        int integer(String key, int fallback, int min, int max) { return Math.toIntExact(number(key, fallback, min, max)); }

        /** 列表中空元素、重复值或超过预算都会失败。 */
        List<String> list(String key, int min, int max) {
            var values = Arrays.stream(required(key).split(",", -1)).map(String::strip).toList();
            if (values.size() < min || values.size() > max || values.stream().anyMatch(String::isEmpty) || new HashSet<>(values).size() != values.size()) {
                throw new IllegalArgumentException("invalid bounded configuration list: " + key);
            }
            return values;
        }

        /** 可选列表通过缺少键表示为空，不用空值掩盖错误配置。 */
        List<String> optionalList(String key, int min, int max) { return properties.containsKey(key) ? list(key, min, max) : List.of(); }

        /** 不输出未知键原文，避免误填密码造成诊断泄露。 */
        void finish() { if (!properties.isEmpty()) { throw new IllegalArgumentException("unknown node configuration fields"); } }
    }

    /** Properties 原生解析器的重复键保护，保留标准转义与续行语法。 */
    private static final class UniqueProperties extends Properties {
        /** 禁止后写覆盖先写，避免配置审计与实际启动值不一致。 */
        @Override public synchronized Object put(Object key, Object value) {
            if (containsKey(key)) { throw new IllegalArgumentException("duplicate node configuration field"); }
            return super.put(key, value);
        }
    }
}
