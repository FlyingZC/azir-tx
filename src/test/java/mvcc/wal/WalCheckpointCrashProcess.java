package mvcc.wal;

import mvcc.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/** 在真实检查点发布或部分文件写入处停住，供父进程强杀恢复。 */
public final class WalCheckpointCrashProcess {
    /** 独立进程探针不允许实例化。 */
    private WalCheckpointCrashProcess() { }

    /** 先确认两笔跨片事务，再停在指定检查点边界，不能用正常关闭代替崩溃。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String target = args[1];
        var io = new WalLog.IoHook() {
            /** 把对应文件真实写入切到指定位置，保证父进程杀到部分正文。 */
            @Override public int checkpointWriteLimit(String name, long position, int remaining) {
                return matches(target, name) ? Math.min(remaining, 32) : remaining;
            }
            /** 部分内容实际写出后再通知父进程。 */
            @Override public void afterCheckpointWrite(String name, long position) {
                if (matches(target, name)) { signal(root); }
            }
        };
        try (var runtime = WalMvccRuntime.openForTest(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(),
                stage -> { if (stage.name().equals(target)) { signal(root); } }, io, System::nanoTime)) {
            for (String value : new String[]{"old", "new"}) {
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                runtime.put(tx, "a", "k", value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                runtime.put(tx, "b", "k", value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                runtime.commit(tx); Files.writeString(root.resolve(value + "-id"), tx.id().toString());
            }
            runtime.collectGarbage(32); runtime.checkpoint(); throw new AssertionError("checkpoint barrier not reached");
        }
    }

    /** 注入只匹配本次指定的检查点文件，不干扰此前已经确认的事务日志。 */
    private static boolean matches(String target, String file) {
        return target.equals("PARTIAL_IMAGE") && file.equals("snapshot.bin")
                || target.equals("PARTIAL_RECEIPT") && file.startsWith("receipts-")
                || target.equals("PARTIAL_POINTER") && file.startsWith("CURRENT-");
    }

    /** 持续等待强杀，不正常退出释放文件锁或补齐未完成写入。 */
    private static void signal(Path root) {
        try { Files.writeString(root.resolve("ready"), "ready"); new CountDownLatch(1).await(); }
        catch (Exception failure) { throw new IllegalStateException(failure); }
    }
}
