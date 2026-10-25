package mvcc.wal.ha;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.wal.MysqlWalMaterializedStore;
import mvcc.wal.WalMaterializedStore;
import mvcc.wal.WalMutation;
import mvcc.wal.WalReplicaIdentity;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** 为独立副本代次分配专属 MySQL 数据库，控制池由调用方拥有，业务池由租约拥有。 */
public final class MysqlWalReplicaBackendFactory implements WalReplicaBackendFactory {
    private final Map<String, HikariDataSource> controls;
    private final Options options;
    private final Hook hook;
    private final RetirementHook retirementHook;
    private final Map<String, String> controlIds;

    /** 控制数据库必须由调用方明确提供，账户须可创建本工厂命名空间的数据库。 */
    public MysqlWalReplicaBackendFactory(Map<String, HikariDataSource> controls, Options options) {
        this(controls, options, stage -> { });
    }

    /** 包内故障注入仅用于验证真实登记、DDL、标记和连接池生命周期。 */
    MysqlWalReplicaBackendFactory(Map<String, HikariDataSource> controls, Options options, Hook hook) {
        this(controls, options, hook, stage -> { });
    }

    /** 两类故障边界各自独立，不把退休边界混入初始化接口。 */
    MysqlWalReplicaBackendFactory(Map<String, HikariDataSource> controls, Options options, Hook hook, RetirementHook retirementHook) {
        this.controls = Map.copyOf(controls); this.options = java.util.Objects.requireNonNull(options); this.hook = java.util.Objects.requireNonNull(hook);
        this.retirementHook = java.util.Objects.requireNonNull(retirementHook); var ids = new HashMap<String, String>();
        if (controls.isEmpty() || controls.size() > 1024) { throw new IllegalArgumentException("invalid MySQL generation routes"); }
        for (var entry : this.controls.entrySet()) {
            new WalMutation(entry.getKey(), "", null); if (entry.getKey().contains("\u0000")) { throw new IllegalArgumentException("invalid MySQL generation shard"); }
            new MysqlWalMaterializedStore(entry.getValue()).validateProduction();
            try (var connection = entry.getValue().getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select @@server_uuid,database()")) {
                if (!row.next() || row.getString(2) == null) { throw new IllegalStateException("explicit namespace control database required"); }
                ids.put(entry.getKey(), hash(row.getString(1) + "\u0000" + row.getString(2)));
            } catch (java.sql.SQLException unavailable) { throw new IllegalStateException("cannot bind namespace control endpoint", unavailable); }
        }
        controlIds = Map.copyOf(ids);
    }

    /** 命名空间与连接数都有有限预算，旧代次退休以前不会自动释放分配额度。 */
    public record Options(String prefix, int maxGenerationsPerShard, int maxSchemasPerControl, int connectionsPerShard) {
        /** 前缀只允许短 ASCII 标识符，派生数据库名不超过 MySQL 六十四字节限制。 */
        public Options {
            if (prefix == null || !prefix.matches("[a-z][a-z0-9_]{0,7}") || maxGenerationsPerShard < 1 || maxGenerationsPerShard > 128
                    || maxSchemasPerControl < 1 || maxSchemasPerControl > 4096 || connectionsPerShard < 1 || connectionsPerShard > 16) { throw new IllegalArgumentException("invalid MySQL generation limits"); }
        }
        /** 默认每分片八代、每控制库一百二十八个数据库、每分片两个业务连接。 */
        public static Options defaults() { return new Options("azw", 8, 128, 2); }
    }

    /** 新空间初始化的实际故障边界，不暴露给普通业务。 */
    enum Stage { AFTER_RESERVATION, AFTER_CREATE_DATABASE, AFTER_MARKER, AFTER_READY, AFTER_POOL_OPEN }

    /** 真实退休日志、DDL 和额度释放的故障边界。 */
    enum RetirementStage { AFTER_RETIRING, AFTER_DATABASE_DROP, AFTER_RELEASE }
    /** 包内验证退休中断续做，正式构造默认不注入。 */
    @FunctionalInterface interface RetirementHook {
        /** 在真实持久边界之后注入失败。 */
        void hit(RetirementStage stage) throws IOException;
    }

    /** 每个边界失败均保留已经持久化的分配身份。 */
    @FunctionalInterface interface Hook {
        /** 在实际操作完成之后注入失败。 */
        void hit(Stage stage) throws IOException;
    }

    /** 打开相同代次复用同一空间，新代次使用独立空间，失败关闭全部已创建的自有池。 */
    @Override public Lease open(WalReplicaIdentity identity, UUID generation) throws IOException {
        var pools = new ArrayList<HikariDataSource>(); var stores = new HashMap<String, WalMaterializedStore>();
        var guard = MysqlNamespaceLeases.open(leaseKeys(identity, generation));
        var cleanup = new RatisCloseSequence(() -> closePools(pools), guard::close);
        try {
            for (String shard : controls.keySet().stream().sorted().toList()) {
                var control = controls.get(shard); String schema = databaseName(identity, generation, shard);
                new MysqlReplicaNamespaces(control, options, hook).provision(identity, generation, shard, schema);
                var config = new HikariConfig(); control.copyStateTo(config); config.setCatalog(schema); config.setSchema(null);
                config.setPoolName("azir-generation-" + hash(schema).substring(0, 16)); config.setMinimumIdle(0); config.setMaximumPoolSize(options.connectionsPerShard()); config.setAutoCommit(true);
                var pool = new HikariDataSource(config); pools.add(pool);
                try (var connection = pool.getConnection()) {
                    try (var query = connection.createStatement(); var row = query.executeQuery("select database()")) {
                        if (!row.next() || !schema.equals(row.getString(1))) { throw new IOException("driver did not select owned generation database"); }
                    }
                    MysqlReplicaNamespaces.verifyOpened(connection, schema, owner(identity, generation, shard, schema));
                }
                var store = new MysqlWalMaterializedStore(pool); store.validateProduction(); stores.put(shard, store); hook.hit(Stage.AFTER_POOL_OPEN);
            }
            return new Lease(stores, cleanup::close);
        } catch (Throwable failure) {
            throw RatisStartupException.afterFailure(failure, cleanup);
        }
    }

    /** MySQL 实现持久墓碑和幂等额度释放。 */
    @Override public boolean supportsRetirement() { return true; }

    /** 游标同时绑定路由和物理控制端点，配置变化不能续删另一组空间。 */
    @Override public RetirementProgress retire(WalGenerationRetirement permit, String cursor, int shardLimit) throws IOException {
        permit.verify(); if (shardLimit < 1 || shardLimit > 1024) { throw new IllegalArgumentException("invalid retirement shard budget"); }
        var shards = controls.keySet().stream().sorted().toList(); String layout = hash(shards.stream().map(shard -> shard + "\u0000" + controlIds.get(shard)).collect(java.util.stream.Collectors.joining("\u0000")));
        int offset = 0;
        if (!cursor.isEmpty()) { String[] parts = cursor.split(":", -1); if (parts.length != 2 || !layout.equals(parts[0])) { throw new IOException("retirement layout changed"); } try { offset = Integer.parseInt(parts[1]); } catch (NumberFormatException invalid) { throw new IOException("invalid retirement cursor", invalid); } }
        if (offset < 0 || offset > shards.size()) { throw new IOException("retirement cursor outside layout"); }
        int inspected = 0, dropped = 0;
        try (var ignored = MysqlNamespaceLeases.retire(leaseKeys(permit.identity(), permit.generation()))) {
            while (offset < shards.size() && inspected < shardLimit) {
                permit.verify(); String shard = shards.get(offset);
                if (new MysqlReplicaNamespaces(controls.get(shard), options, hook).retire(permit, shard, databaseName(permit.identity(), permit.generation(), shard), retirementHook)) { dropped++; }
                offset++; inspected++;
            }
        }
        return new RetirementProgress(layout + ":" + offset, offset == shards.size(), inspected, dropped);
    }

    /** 登记键仅存物理端点摘要，既不泄漏凭据，也不按数据源对象引用误认同一空间。 */
    private java.util.List<String> leaseKeys(WalReplicaIdentity identity, UUID generation) { return controls.keySet().stream().map(shard -> hash(controlIds.get(shard) + "\u0000" + databaseName(identity, generation, shard))).toList(); }

    /** 派生名字包含节点、完整代次 UUID 和分片摘要，实际归属还要通过完整指纹验证。 */
    public String databaseName(WalReplicaIdentity identity, UUID generation, String shard) {
        java.util.Objects.requireNonNull(generation); if (!controls.containsKey(shard)) { throw new IllegalArgumentException("unknown MySQL generation shard"); }
        return options.prefix() + "_" + nodeOwner(identity).substring(0, 8) + "_" + generation.toString().replace("-", "") + "_" + hash(shard).substring(0, 8);
    }

    /** 节点所有权使用全部身份字段，不以短数据库名摘要作为最终授权。 */
    static String nodeOwner(WalReplicaIdentity identity) { return hash(identity.engineId() + "\u0000" + identity.groupId() + "\u0000" + identity.nodeId()); }

    /** 完整空间身份同时绑定节点、代次、分片和物理数据库名。 */
    static String owner(WalReplicaIdentity identity, UUID generation, String shard, String schema) { return hash(nodeOwner(identity) + "\u0000" + generation + "\u0000" + shard + "\u0000" + schema); }

    /** 固定 UTF-8 摘要仅包含身份字段，不包含数据库凭据。 */
    private static String hash(String value) {
        try { return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }

    /** 只释放本次工厂创建的业务池，控制池和数据库均由其他明确生命周期管理。 */
    private static void closePools(java.util.List<HikariDataSource> pools) throws IOException {
        RuntimeException failure = null;
        for (var iterator = pools.iterator(); iterator.hasNext();) {
            try { iterator.next().close(); iterator.remove(); }
            catch (RuntimeException close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } }
        }
        if (failure != null) { throw new IOException("generation connection pools did not close", failure); }
    }
}
