package mvcc.wal.ha;

import mvcc.common.IsolationLevel;
import mvcc.wal.*;
import org.apache.ratis.protocol.*;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 独立 JVM 故障探针，stdin 只用于测试调度，不作为产品 RPC 或认证协议。 */
public final class RatisWalProcessProbe {
    /** 工具不允许实例化。 */
    private RatisWalProcessProbe() { }

    /** 每个 JVM 运行一个实际 Raft/MVCC 节点，结果由父进程记录后才注入强杀。 */
    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]); String id = args[1]; UUID groupId = UUID.fromString(args[2]); var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) { peers.add(RaftPeer.newBuilder().setId("p" + i).setAddress("127.0.0.1:" + args[3 + i]).build()); }
        var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        try (var node = RatisWalNode.openLoopback(directory, RaftPeerId.valueOf(id), group, Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000));
             var input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            result("STARTED " + ProcessHandle.current().pid());
            WalTransaction buffered = null;
            for (String command; (command = input.readLine()) != null;) {
                String[] fields = command.split(" ", 2);
                try {
                    switch (fields[0]) {
                        case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + node.appliedSequence());
                        case "PING" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ); long start = tx.startSequence(); node.commit(tx); result("PING " + start);
                        }
                        case "WRITE" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ); result("INTENT " + tx.id()); byte[] value = Base64.getUrlDecoder().decode(fields[1]);
                            node.put(tx, "a", "k", value); node.put(tx, "b", "k", value); var receipt = node.commit(tx);
                            result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence());
                        }
                        case "BUFFER" -> {
                            if (buffered != null) { throw new IllegalStateException("buffered transaction already exists"); }
                            buffered = node.begin(IsolationLevel.REPEATABLE_READ); byte[] value = Base64.getUrlDecoder().decode(fields[1]);
                            node.put(buffered, "a", "k", value); node.put(buffered, "b", "k", value); result("BUFFERED " + buffered.id());
                        }
                        case "FINISH" -> {
                            if (buffered == null) { throw new IllegalStateException("no buffered transaction"); }
                            var tx = buffered; buffered = null; var receipt = node.commit(tx);
                            result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence());
                        }
                        case "READ" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ); var snapshot = node.beginReadStatement(tx);
                            String a = encode(node.get(tx, "a", "k", snapshot)); String b = encode(node.get(tx, "b", "k", snapshot)); node.rollback(tx); result("VALUES " + a + " " + b);
                        }
                        case "LOOKUP" -> result("LOOKUP " + node.lookupCommit(UUID.fromString(fields[1])).status());
                        case "STOP" -> { result("STOPPING"); return; }
                        default -> throw new IllegalArgumentException("unknown probe command");
                    }
                } catch (Exception failure) {
                    result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : ""));
                }
            }
        }
    }

    /** 明确区分空值和实际二进制载荷。 */
    private static String encode(java.util.Optional<byte[]> value) { return value.map(bytes -> Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)).orElse("-"); }

    /** 每条业务结果立即 flush，父进程收到成功后才算进入确认前缀。 */
    private static void result(String value) { System.out.println("AZIR " + value); System.out.flush(); }
}
