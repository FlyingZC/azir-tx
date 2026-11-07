package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.wal.*;
import org.apache.ratis.protocol.*;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** 自动维护、真实后端与认证网络组合故障探针，结果由父进程接收才进入确认前缀。 */
public final class RatisCombinationProcessProbe {
    /** 探针不允许实例化。 */
    private RatisCombinationProcessProbe() { }
    /** 一个容器一个节点，组、策略和自己的凭据均由父测试明确提供。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String id = args[1]; UUID groupId = UUID.fromString(args[2]); var peers = new ArrayList<RaftPeer>(); var trusted = new ArrayList<RatisTlsSecurity.PeerTrust>(); var policy = new Properties();
        try (var input = Files.newInputStream(Path.of(args[3]))) { policy.load(input); }
        for (int i = 0; i < 3; i++) { String peer = "p" + i, address = policy.getProperty(peer + ".address"); peers.add(RaftPeer.newBuilder().setId(peer).setAddress(address).build()); trusted.add(new RatisTlsSecurity.PeerTrust(peer, address, Set.of(policy.getProperty(peer + ".pin")))); }
        result("BOOT " + ProcessHandle.current().pid()); var security = RatisTlsSecurity.load(groupId, id, Path.of(args[4]), RatisTlsTestCertificates.PASSWORD, Path.of(args[5]), RatisTlsTestCertificates.PASSWORD, trusted);
        var fault = new Fault();
        try (var control = control(policy, id); var node = RatisWalNode.openAuthenticatedForTest(root, RaftPeerId.valueOf(id), RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers), Set.of("a", "b"), new MysqlWalReplicaBackendFactory(Map.of("a", control, "b", control), MysqlWalReplicaBackendFactory.Options.defaults()),
                WalOptions.defaults().withTimeout(5000).withHotCacheBytes(0), new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000), security, stage -> fault.hit(stage.name()));
             var worker = Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().name("owned-combination-commit").factory());
             var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            result("STARTED " + ProcessHandle.current().pid()); WalTransaction buffered = null; CompletableFuture<WalReceipt> submitted = null; UUID submittedId = null;
            for (String command; (command = input.readLine()) != null;) {
                String[] fields = command.split(" ", 2);
                try {
                    switch (fields[0]) {
                        case "ARM" -> { fault.arm(fields[1]); result("ARMED " + fields[1]); }
                        case "RELEASE" -> { fault.release(); result("RELEASED"); }
                        case "LIFECYCLE" -> { node.enableHealthManagement(new RatisHealthOptions(200, 2, 2, 750, 6000)); node.enableVersionGc(new RatisGcOptions(200, 64, 32, 100, 15_000)); node.enableSnapshotMaintenance(new RatisSnapshotOptions(100, 8, 1000, 64L * 1024 * 1024, 0)); result("LIFECYCLE true true true"); }
                        case "MAINT" -> result("MAINT " + node.gcStats().rounds() + " " + node.snapshotStats().snapshots() + " " + node.snapshotStats().firstLogIndex() + " " + node.healthStats().quarantined() + " " + node.healthStats().campaignBlocked() + " " + node.gcStats().removedRows());
                        case "COLD" -> { long needed = Long.parseLong(fields[1]); var stats = node.localStats(); if (!node.ready() || stats.materializedSequences().values().stream().anyMatch(value -> value < needed)) { result("NOT_COLD"); } else { node.checkpointLocal(); result("COLD " + node.appliedSequence()); } }
                        case "FINISH_ASYNC" -> { if (buffered == null || submitted != null) { throw new IllegalStateException("missing buffer or commit already submitted"); } var tx = buffered; buffered = null; submittedId = tx.id(); submitted = CompletableFuture.supplyAsync(() -> node.commit(tx), worker); result("SUBMITTED " + submittedId); }
                        case "RESULT" -> { if (submitted == null) { throw new IllegalStateException("no submitted commit"); } if (!submitted.isDone()) { result("PENDING " + submittedId); } else { try { committed(submitted.join()); } catch (CompletionException failed) { Throwable cause = failed.getCause(); result("ERROR " + cause.getClass().getSimpleName() + (cause instanceof WalMvccException wal ? " " + wal.errorCode() : "") + " " + submittedId); } } }
                        case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + (node.ready() ? node.appliedSequence() : -1) + " " + node.appliedRaftIndex() + " " + node.snapshotIndex() + " " + node.durableSnapshotAckIndex() + " " + node.ready());
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
                        case "BUFFER" -> { if (buffered != null) { throw new IllegalStateException("buffer already exists"); } buffered = node.begin(IsolationLevel.REPEATABLE_READ); node.put(buffered, "a", "live", bytes("999")); node.put(buffered, "b", "live", bytes("-999")); result("BUFFERED " + buffered.id()); }
                        case "FINISH" -> { if (buffered == null) { throw new IllegalStateException("buffer missing"); } var tx = buffered; buffered = null; committed(node.commit(tx)); }
                        case "SNAPSHOT" -> result("SNAPSHOT " + node.snapshot());
                        case "PURGE" -> { node.purgeForTest(Long.parseLong(fields[1])); result("PURGED " + node.firstLogIndexForTest()); }
                        case "LOOKUP" -> { var value = node.lookupCommit(UUID.fromString(fields[1])); result("LOOKUP " + value.status() + (value.receipt() == null ? "" : " " + value.receipt().commitSequence() + " " + value.receipt().digest())); }
                        case "READ" -> result(read(node));
                        case "TRANSFER" -> { node.transferLeadership(RaftPeerId.valueOf(fields[1]), 30_000); result("TRANSFERRED"); }
                        case "STOP" -> { fault.release(); result("STOPPING"); return; }
                        default -> throw new IllegalArgumentException("unknown probe command");
                    }
                } catch (Exception failure) { result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : "")); }
            }
        }
    }

    /** 数据连接只使用父进程的专属后端网络和控制库，凭据不进入命令行或历史。 */
    private static com.zaxxer.hikari.HikariDataSource control(Properties policy, String id) {
        String address = policy.getProperty(id + ".backend"), schema = System.getenv("AZIR_WAL_COMBINATION_SCHEMA");
        if (address == null || !address.matches("[0-9.]+:3306") || schema == null || !schema.matches("c_[a-f0-9]{32}")) { throw new IllegalArgumentException("owned combination backend required"); }
        var config = new com.zaxxer.hikari.HikariConfig(); config.setJdbcUrl("jdbc:mysql://" + address + "/ha_control?connectTimeout=2000&socketTimeout=2000&rewriteBatchedStatements=true"); config.setCatalog(schema); config.setUsername("root"); config.setPassword(System.getenv("AZIR_WAL_COMBINATION_PASSWORD")); config.setMinimumIdle(0); config.setMaximumPoolSize(2); config.setConnectionTimeout(5000); config.setValidationTimeout(1000); return new com.zaxxer.hikari.HikariDataSource(config);
    }
    /** 在真实串行追加以前或多数派与本地 WAL 应用以后暂停，控制输入仍独立可用。 */
    private static final class Fault {
        volatile String wanted; final AtomicBoolean armed = new AtomicBoolean(); volatile CountDownLatch released = new CountDownLatch(0);
        /** 一次只允许一个父进程选择的故障边界。 */
        void arm(String stage) { released = new CountDownLatch(1); wanted = stage; armed.set(true); }
        /** 故障注入只发生在真实协议位置，不 halt JVM 或伪造完成。 */
        void hit(String stage) throws IOException { if (!stage.equals(wanted) || !armed.compareAndSet(true, false)) { return; } result("BOUNDARY " + stage); try { released.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); } }
        /** 父进程释放或正常清理时继续原操作，真正强杀由 Docker 控制。 */
        void release() { released.countDown(); }
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

}
