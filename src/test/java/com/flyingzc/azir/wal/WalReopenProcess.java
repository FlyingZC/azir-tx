package com.flyingzc.azir.wal;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/** 在真实重启恢复安装第一个分片版本后停住，以验证恢复期间再次崩溃。 */
public final class WalReopenProcess {
    /** 独立子进程入口不允许实例化。 */
    private WalReopenProcess() { }

    /** 只有命中实际恢复版本安装边界才通知父进程，不能用启动前暂停代替。 */
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        try (var runtime = WalMvccRuntime.openForTest(root, Set.of("a", "b"), Map.of(), WalOptions.defaults(), stage -> {
            if (stage == WalMvccRuntime.Stage.DURING_RECOVERY) {
                try {
                    Files.writeString(root.resolve("recovery-ready"), "ready");
                    new CountDownLatch(1).await();
                } catch (Exception failure) { throw new IllegalStateException(failure); }
            }
        })) { throw new AssertionError("recovery barrier was not reached"); }
    }
}
