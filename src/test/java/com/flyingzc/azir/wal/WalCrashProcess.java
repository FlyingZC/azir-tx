package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/** 在真实子 JVM 中停在提交边界，父进程使用强杀验证未执行关闭的恢复。 */
public final class WalCrashProcess {
    /** 禁止实例化独立进程探针。 */
    private WalCrashProcess() { }

    /** 写入一笔跨片事务，在指定持久边界通知父进程后阻塞。 */
    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        String stageName = args[1];
        var hook = (WalMvccRuntime.FaultHook) stage -> {
            if (stage.name().equals(stageName)) { signalAndWait(directory); }
        };
        try (var runtime = WalMvccRuntime.openForTest(directory, Set.of("a", "b"), Map.of(), WalOptions.defaults(), hook)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            Files.writeString(directory.resolve("transaction-id"), tx.id().toString());
            runtime.put(tx, "a", "balance", new byte[]{90});
            runtime.put(tx, "b", "balance", new byte[]{110});
            runtime.commit(tx);
            if (stageName.equals("ACK")) { signalAndWait(directory); }
        }
    }

    /** 通知父进程边界已到达，阻塞期间不正常退出或释放文件锁。 */
    private static void signalAndWait(Path directory) {
        try {
            Files.writeString(directory.resolve("ready"), "ready");
            new CountDownLatch(1).await();
        } catch (Exception ex) { throw new IllegalStateException(ex); }
    }
}
