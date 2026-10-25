package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** 容器内独立 JVM 的认证网络故障探针，stdin 调度不作为产品网络入口。 */
public final class RatisNetworkProcessProbe {
    /** 探针不允许实例化。 */
    private RatisNetworkProcessProbe() { }
    /** 一个容器一个节点，组、策略和自己的凭据均由父测试明确提供。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String id = args[1]; UUID groupId = UUID.fromString(args[2]); var peers = new ArrayList<RaftPeer>(); var trusted = new ArrayList<RatisTlsSecurity.PeerTrust>(); var policy = new Properties();
        try (var input = Files.newInputStream(Path.of(args[3]))) { policy.load(input); }
        for (int i = 0; i < 3; i++) { String peer = "p" + i, address = policy.getProperty(peer + ".address"); peers.add(RaftPeer.newBuilder().setId(peer).setAddress(address).build()); trusted.add(new RatisTlsSecurity.PeerTrust(peer, address, Set.of(policy.getProperty(peer + ".pin")))); }
        result("BOOT " + ProcessHandle.current().pid()); var security = RatisTlsSecurity.load(groupId, id, Path.of(args[4]), RatisTlsTestCertificates.PASSWORD, Path.of(args[5]), RatisTlsTestCertificates.PASSWORD, trusted);
        String wanted = args[6]; var armed = new AtomicBoolean(true);
        try (var node = RatisWalNode.openAuthenticatedForTest(root, RaftPeerId.valueOf(id), RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers), Set.of("a", "b"), WalReplicaBackendFactory.localOnly(),
                WalOptions.defaults().withTimeout(3000), new WalBackupOptions(128_000_000, 8, 4_000_000, 120_000), security, stage -> {
                    if (stage.name().equals(wanted) && armed.compareAndSet(true, false)) {
                        result("BOUNDARY " + wanted); try { new CountDownLatch(1).await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    }
                }); var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            result("STARTED " + ProcessHandle.current().pid()); WalTransaction buffered = null;
            for (String command; (command = input.readLine()) != null;) {
                String[] fields = command.split(" ", 2);
                try {
                    switch (fields[0]) {
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
                        case "STOP" -> { result("STOPPING"); return; }
                        default -> throw new IllegalArgumentException("unknown probe command");
                    }
                } catch (Exception failure) { result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : "")); }
            }
        }
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
