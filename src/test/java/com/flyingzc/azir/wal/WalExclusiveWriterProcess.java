package com.flyingzc.azir.wal;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/** 用另一个 JVM 验证系统级文件锁，避免只验证 JVM 内部重叠锁表。 */
public final class WalExclusiveWriterProcess {
    /** 禁止实例化独占锁探针。 */
    private WalExclusiveWriterProcess() { }

    /** 返回 7 表示获得锁，返回 0 表示被已有写者拒绝。 */
    public static void main(String[] args) throws Exception {
        try (var log = new WalLog(Path.of(args[0]), Set.of("a"), 1_000_000)) {
            System.out.println("ACQUIRED"); System.exit(7);
        } catch (IOException locked) { System.out.println("LOCKED"); }
    }
}
