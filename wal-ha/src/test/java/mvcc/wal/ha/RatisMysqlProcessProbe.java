package mvcc.wal.ha;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/** 三 JVM/MySQL 安装强杀探针，stdin 仅用于父进程测试调度，不是产品接入协议。 */
public final class RatisMysqlProcessProbe {
    /** 工具不允许实例化。 */
    private RatisMysqlProcessProbe() { }

    /** 启动一个真实副本，凭据仅从自有测试环境读取，不写入命令行或日志。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String id = args[1]; var groupId = RaftGroupId.valueOf(UUID.fromString(args[2])); var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) { peers.add(RaftPeer.newBuilder().setId("p" + i).setAddress("127.0.0.1:" + args[3 + i]).build()); }
        String release = System.getenv("AZIR_WAL_PROBE_RELEASE_FILE");
        if (release != null && !Path.of(release).toAbsolutePath().normalize().equals(root.toAbsolutePath().normalize().resolve(".auto-release"))) { throw new IllegalArgumentException("release marker must belong to this owned probe root"); }
        var group = RaftGroup.valueOf(groupId, peers); var fault = new Fault(System.getenv("AZIR_WAL_PROBE_FAULT")); result("BOOT " + ProcessHandle.current().pid());
        try (var control = control()) {
            var base = new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults());
            WalReplicaBackendFactory factory = new WalReplicaBackendFactory() {
                /** 备份与恢复仍完整委托实际 MySQL，暂停点仅用于父进程的故障调度。 */
                @Override public Lease open(WalReplicaIdentity identity, UUID generation) throws IOException {
                    var lease = base.open(identity, generation); var stores = new HashMap<String, WalMaterializedStore>();
                    lease.stores().forEach((shard, store) -> stores.put(shard, new RestoreBoundaryStore(store, shard.equals("a") ? fault : new Fault(null))));
                    return new Lease(stores, lease::close);
                }
                /** 自动生命周期使用正式工厂的有界退休能力。 */
                @Override public boolean supportsRetirement() { return base.supportsRetirement(); }
                /** 不通过测试包装器绕过实际所有权许可。 */
                @Override public RetirementProgress retire(WalGenerationRetirement permit, String cursor, int limit) throws IOException { return base.retire(permit, cursor, limit); }
            };
            try (var node = RatisWalNode.openGenerationalForTest(root, RaftPeerId.valueOf(id), group, Set.of("a", "b"), factory,
                    WalOptions.defaults().withTimeout(30_000).withHotCacheBytes(0), new WalBackupOptions(128_000_000, 2, 4_000_000, 120_000), new RatisWalState.Hook() {
                        /** 安装和镜像真实边界共用同一个一次性故障调度。 */
                        @Override public void hit(RatisWalState.Stage stage) { fault.hit(stage.name()); }
                        /** 证明记录先于暂停事件发送，父进程才可在实际边界强杀。 */
                        @Override public void hit(RatisWalState.Stage stage, long first, long covered) { fault.proof(stage.name(), first, covered); fault.hit(stage.name()); }
                    });
                 var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                result("STARTED " + ProcessHandle.current().pid());
                for (String command; (command = input.readLine()) != null;) {
                    String[] fields = command.split(" ", 2);
                    try {
                        switch (fields[0]) {
                            case "ARM" -> { fault.arm(fields[1]); result("ARMED " + fields[1]); }
                            case "LIFECYCLE" -> {
                                node.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000));
                                node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 1, 1000, 64L * 1024 * 1024, 0)); result("LIFECYCLE true true");
                            }
                            case "LOG" -> result("LOG " + node.firstLogIndexForTest());
                            case "POLICY" -> result("POLICY " + node.gcStats().enabled() + " " + node.snapshotStats().enabled());
                            case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + (node.ready() ? node.appliedSequence() : -1) + " " + node.ready() + " " + node.snapshotIndex());
                            case "PING" -> { var tx = node.begin(IsolationLevel.REPEATABLE_READ); long sequence = tx.startSequence(); node.rollback(tx); result("PING " + sequence); }
                            case "INIT" -> {
                                var tx = node.begin(IsolationLevel.REPEATABLE_READ); result("INTENT " + tx.id());
                                for (String shard : List.of("a", "b")) { for (int i = 0; i < 32; i++) { node.put(tx, shard, "cold" + i, bytes(shard + ":" + i)); } node.put(tx, shard, "blob", new byte[]{0}); node.put(tx, shard, "live", bytes("0")); }
                                committed(node.commit(tx));
                            }
                            case "WRITE" -> {
                                int value = Integer.parseInt(fields[1]); var tx = node.begin(IsolationLevel.REPEATABLE_READ); result("INTENT " + tx.id());
                                for (String shard : List.of("a", "b")) { var payload = new byte[480 * 1024]; Arrays.fill(payload, (byte) (value + (shard.equals("a") ? 0 : 100))); node.put(tx, shard, "blob", payload); }
                                committed(node.commit(tx));
                            }
                            case "DELETE" -> { var tx = node.begin(IsolationLevel.REPEATABLE_READ); result("INTENT " + tx.id()); for (String shard : List.of("a", "b")) { node.delete(tx, shard, "cold0"); } committed(node.commit(tx)); }
                            case "LIVE" -> { int value = Integer.parseInt(fields[1]); var tx = node.begin(IsolationLevel.REPEATABLE_READ); result("INTENT " + tx.id()); node.put(tx, "a", "live", bytes(Integer.toString(value))); node.put(tx, "b", "live", bytes(Integer.toString(-value))); committed(node.commit(tx)); }
                            case "COLD" -> {
                                long sequence = Long.parseLong(fields[1]); var stats = node.localStats();
                                if (stats.materializedSequences().values().stream().anyMatch(value -> value < sequence)) { result("NOT_COLD"); }
                                else { node.checkpointLocal(); stats = node.localStats(); result("COLD " + stats.publishedSequence() + " " + stats.retainedBytes() + " " + stats.walBytes()); }
                            }
                            case "SNAPSHOT" -> result("SNAPSHOT " + node.snapshot());
                            case "PURGE" -> { long index = Long.parseLong(fields[1]); node.purgeForTest(index); result("PURGED " + node.firstLogIndexForTest()); }
                            case "LOOKUP" -> { var lookup = node.lookupCommit(UUID.fromString(fields[1])); result("LOOKUP " + lookup.status() + (lookup.receipt() == null ? "" : " " + lookup.receipt().commitSequence() + " " + lookup.receipt().digest())); }
                            case "READ" -> result(read(node));
                            case "TRANSFER" -> {
                                try (var client = RaftClient.newBuilder().setRaftGroup(group).setLeaderId(RaftPeerId.valueOf(id)).setProperties(new org.apache.ratis.conf.RaftProperties()).build()) { result("TRANSFER " + client.admin().transferLeadership(RaftPeerId.valueOf(fields[1]), 10_000).isSuccess()); }
                            }
                            case "STOP" -> { result("STOPPING"); return; }
                            default -> throw new IllegalArgumentException("unknown owned probe command");
                        }
                    } catch (Exception failure) { result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : "")); }
                }
            }
        }
    }

    /** 只连接父进程创建的回环实例，控制库名必须来自固定测试格式。 */
    private static HikariDataSource control() {
        String url = System.getenv("AZIR_WAL_PROBE_JDBC_URL"), schema = System.getenv("AZIR_WAL_PROBE_CONTROL_SCHEMA");
        if (url == null || !url.matches("jdbc:mysql://(localhost|127\\.0\\.0\\.1):[0-9]+/ha_control(?:\\?.*)?") || schema == null || !schema.matches("c_[a-f0-9]{32}")) { throw new IllegalArgumentException("probe requires owned loopback MySQL namespace"); }
        var config = new HikariConfig(); config.setJdbcUrl(url); config.setCatalog(schema); config.setUsername("root"); config.setPassword(System.getenv("AZIR_WAL_PROBE_PASSWORD"));
        config.setMinimumIdle(0); config.setMaximumPoolSize(2); config.setConnectionTimeout(5000); config.setValidationTimeout(1000); return new HikariDataSource(config);
    }

    /** 相同 RR 语句快照遍历全部键和墓碑，按长度编码计算可独立重算的完整内容摘要。 */
    private static String read(RatisWalNode node) throws Exception {
        var hash = java.security.MessageDigest.getInstance("SHA-256"); var output = new DataOutputStream(new java.security.DigestOutputStream(OutputStream.nullOutputStream(), hash));
        var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
        for (String shard : List.of("a", "b")) {
            for (int i = 0; i < 32; i++) { entry(output, shard + "/cold" + i, node.get(tx, shard, "cold" + i, snapshot)); }
            for (String key : List.of("blob", "live")) { entry(output, shard + "/" + key, node.get(tx, shard, key, snapshot)); }
        }
        node.rollback(tx); output.flush(); return "DATA " + snapshot.sequence() + " " + java.util.HexFormat.of().formatHex(hash.digest());
    }

    /** 空值明确编码为负长度，不能与空字节值或拼接边界混淆。 */
    private static void entry(DataOutputStream output, String key, Optional<byte[]> value) throws IOException { byte[] name = bytes(key); output.writeInt(name.length); output.write(name); if (value.isEmpty()) { output.writeInt(-1); } else { output.writeInt(value.get().length); output.write(value.get()); } }

    /** 固定 UTF-8 业务编码。 */
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    /** 只有实际返回的提交凭据才发送 COMMITTED，INTENT 不表示成功。 */
    private static void committed(WalReceipt receipt) { result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence() + " " + receipt.digest()); }

    /** 事件可能来自安装线程和调度线程，每行完整输出并立即刷新。 */
    private static synchronized void result(String value) { System.out.println("AZIR " + value); System.out.flush(); }

    /** 父进程观察到真实边界后强杀，探针不主动 halt 或用超时冒充崩溃。 */
    private static final class Fault {
        volatile String wanted; final AtomicBoolean armed = new AtomicBoolean(true);
        /** 没有配置的进程不注入故障。 */
        Fault(String wanted) { this.wanted = wanted; }
        /** 仅由当前测试 stdin 在正常启动后选择一次实际故障边界。 */
        void arm(String stage) { wanted = stage; armed.set(true); }
        /** 回收边界还需保留原生日志实际首索引和完整镜像覆盖上界。 */
        void proof(String stage, long first, long covered) { if (stage.equals(wanted) && armed.get()) { result("PROOF " + stage + " " + first + " " + covered); } }
        /** 只冻结一次命中的实际边界，恢复期间是否再次强杀由父进程另行启动决定。 */
        void hit(String stage) {
            if (wanted == null || !wanted.equals(stage) || !armed.compareAndSet(true, false)) { return; }
            result("BOUNDARY " + stage + " " + ProcessHandle.current().pid());
            try {
                String configured = System.getenv("AZIR_WAL_PROBE_RELEASE_FILE");
                if (configured == null) { new CountDownLatch(1).await(); }
                else {
                    // 仅测试恢复检查点允许父进程通过其自有临时文件继续，产品代码没有此入口。
                    Path release = Path.of(configured);
                    if (!release.getFileName().toString().equals(".auto-release")) { throw new IllegalArgumentException("invalid owned release marker"); }
                    while (!java.nio.file.Files.isRegularFile(release, java.nio.file.LinkOption.NOFOLLOW_LINKS)) { Thread.sleep(10); }
                    if (!java.nio.file.Files.readString(release).equals("resume")) { throw new IllegalArgumentException("invalid owned release command"); }
                    java.nio.file.Files.delete(release);
                }
            } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IllegalStateException("owned fault probe interrupted", failure); }
            catch (IOException failure) { throw new IllegalStateException("owned fault release failed", failure); }
        }
    }

    /** 仅在真实 MySQL restoreBatch 已提交后冻结，其他操作完整委托正式适配器。 */
    private record RestoreBoundaryStore(WalMaterializedStore delegate, Fault fault) implements WalMaterializedStore {
        /** 保留正式后端容量校验。 */
        @Override public void validateMutation(WalMutation mutation) { delegate.validateMutation(mutation); }
        /** 保留普通绑定拒绝规则。 */
        @Override public void initialize(UUID engine, String shard) { delegate.initialize(engine, shard); }
        /** 保留复制能力声明。 */
        @Override public boolean supportsReplicaOwnership() { return delegate.supportsReplicaOwnership(); }
        /** 保留接收副本所有权。 */
        @Override public void initializeReplica(WalReplicaIdentity identity, String shard) { delegate.initializeReplica(identity, shard); }
        /** 直接查询真实物化前缀。 */
        @Override public WalCheckpoint checkpoint() { return delegate.checkpoint(); }
        /** 直接应用真实事务。 */
        @Override public WalCheckpoint apply(List<WalCommit> commits) { return delegate.apply(commits); }
        /** 不引入测试数据缓存。 */
        @Override public Optional<WalValue> get(String key, long sequence) { return delegate.get(key, sequence); }
        /** 保留真实写冲突查询。 */
        @Override public long latestSequence(String key) { return delegate.latestSequence(key); }
        /** 保留真实范围查询。 */
        @Override public NavigableMap<String, WalValue> scan(String from, String to, long sequence) { return delegate.scan(from, to, sequence); }
        /** 保留真实后端回收。 */
        @Override public int gc(long safe, int keys) { return delegate.gc(safe, keys); }
        /** 保留有界维护能力。 */
        @Override public boolean supportsBoundedMaintenance() { return delegate.supportsBoundedMaintenance(); }
        /** 保留真实有界回收。 */
        @Override public GcProgress gcBounded(long safe, int keys, int rows, long millis) { return delegate.gcBounded(safe, keys, rows, millis); }
        /** 连接和持久性校验仍由正式适配器执行。 */
        @Override public void validateProduction() { delegate.validateProduction(); }
        /** 导出仍使用真实流式实现。 */
        @Override public boolean supportsBackup() { return delegate.supportsBackup(); }
        /** 并发快照能力由实际后端提供。 */
        @Override public boolean supportsPinnedSnapshot() { return delegate.supportsPinnedSnapshot(); }
        /** 固定真实后端的只读事务，不用包装器模拟快照。 */
        @Override public SnapshotView openSnapshot(WalCheckpoint boundary) { return delegate.openSnapshot(boundary); }
        /** 恢复所有权能力不改变。 */
        @Override public boolean supportsReplicaRestore() { return delegate.supportsReplicaRestore(); }
        /** 导出真实冷数据。 */
        @Override public void exportSnapshot(WalCheckpoint boundary, BackupSink sink) throws IOException { delegate.exportSnapshot(boundary, sink); }
        /** 恢复隔离标记由正式实现持久化。 */
        @Override public boolean beginRestore(WalRestoreSpec spec) { return delegate.beginRestore(spec); }
        /** 明确在真实批次提交完成之后停住。 */
        @Override public void restoreBatch(WalRestoreSpec spec, List<WalBackupRow> rows) { delegate.restoreBatch(spec, rows); fault.hit("MID_RESTORE_BATCH"); }
        /** 完整数据摘要与前缀发布仍由正式实现核对。 */
        @Override public void finishRestore(WalRestoreSpec spec, long rows, String hash) { delegate.finishRestore(spec, rows, hash); }
    }
}
