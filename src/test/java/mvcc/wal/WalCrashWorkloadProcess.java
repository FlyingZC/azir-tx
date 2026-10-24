package mvcc.wal;

import mvcc.common.IsolationLevel;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/** 为随机时刻强杀提供持续跨片写入及可独立核对的尝试与确认记录。 */
public final class WalCrashWorkloadProcess {
    /** 独立进程探针不允许实例化。 */
    private WalCrashWorkloadProcess() { }

    /** 前三笔确认后通知父进程，其余写入持续到父进程强杀。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        try (var events = new PrintWriter(Files.newBufferedWriter(root.resolve("events.tsv"), StandardCharsets.UTF_8), true);
             var runtime = WalMvccRuntime.open(root, Set.of("a", "b"), WalOptions.defaults())) {
            for (int ordinal = 0; ; ordinal++) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                String key = "k" + ordinal % 7; String value = ordinal % 9 == 0 ? "<NULL>" : "v" + ordinal;
                events.println("ATTEMPT\t" + tx.id() + "\t0\t" + key + "\t" + value);
                for (String shard : Set.of("a", "b")) {
                    if (value.equals("<NULL>")) { runtime.delete(tx, shard, key); }
                    else { runtime.put(tx, shard, key, value.getBytes(StandardCharsets.UTF_8)); }
                }
                var receipt = runtime.commit(tx);
                events.println("ACK\t" + tx.id() + "\t" + receipt.commitSequence() + "\t" + key + "\t" + value);
                if (ordinal == 2) { Files.writeString(root.resolve("ready"), "ready"); }
            }
        }
    }
}
