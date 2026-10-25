package mvcc.wal.ha;

import java.io.IOException;
import java.util.*;

/** 同 JVM 的所有工厂共享空间租约，Node 目录锁负责跨进程的同节点互斥。 */
final class MysqlNamespaceLeases {
    private static final Map<String, Entry> ENTRIES = new HashMap<>();
    /** 工具不允许实例化。 */
    private MysqlNamespaceLeases() { }
    /** 有活动租约时不能退休，退休进行时不能开始新租约。 */
    private static final class Entry { int leases; boolean retiring; }
    /** 一次原子登记全部分片，不能只保护已打开的第一片。 */
    static synchronized Guard open(Collection<String> names) throws IOException {
        var keys = Set.copyOf(names); for (String key : keys) { var entry = ENTRIES.get(key); if (entry != null && entry.retiring) { throw new IOException("namespace retirement in progress"); } }
        for (String key : keys) { ENTRIES.computeIfAbsent(key, ignored -> new Entry()).leases++; } return new Guard(keys, false);
    }
    /** 删除任何一片前先确认全部片都没有活动租约。 */
    static synchronized Guard retire(Collection<String> names) throws IOException {
        var keys = Set.copyOf(names); for (String key : keys) { var entry = ENTRIES.get(key); if (entry != null && (entry.leases > 0 || entry.retiring)) { throw new IOException("cannot retire a leased namespace"); } }
        for (String key : keys) { ENTRIES.computeIfAbsent(key, ignored -> new Entry()).retiring = true; } return new Guard(keys, true);
    }
    /** 全部关闭后移除内存登记，不把历史墓碑留在堆中。 */
    static final class Guard implements AutoCloseable {
        final Set<String> keys; final boolean retiring; boolean closed;
        /** Guard 只能由上述原子登记创建。 */
        Guard(Set<String> keys, boolean retiring) { this.keys = keys; this.retiring = retiring; }
        /** 幂等释放全部分片登记。 */
        @Override public void close() { synchronized (MysqlNamespaceLeases.class) {
            if (closed) { return; } closed = true;
            for (String key : keys) { var entry = ENTRIES.get(key); if (retiring) { entry.retiring = false; } else { entry.leases--; } if (entry.leases == 0 && !entry.retiring) { ENTRIES.remove(key); } }
        } }
    }
}
