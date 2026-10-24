package mvcc.wal;

import mvcc.storage.Utf8KeyOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/** 确定性物化测试替身，用来控制故障和检查读取、回收交错。 */
final class TestMaterializedStore implements WalMaterializedStore {
    final Map<String, NavigableMap<Long, WalValue>> versions = new HashMap<>();
    volatile boolean failApply;
    volatile boolean loseApplyResponse;
    volatile Runnable onRead = () -> { };
    volatile Runnable onApply = () -> { };
    private UUID engine;
    private String shard;
    private WalCheckpoint checkpoint = new WalCheckpoint(0, "");

    /** 绑定测试后端，拒绝复用不同日志身份。 */
    @Override public synchronized void initialize(UUID engineId, String shard) {
        if (engine != null && (!engine.equals(engineId) || !this.shard.equals(shard))) {
            throw new IllegalStateException("test store binding mismatch");
        }
        engine = engineId;
        this.shard = shard;
    }

    /** 返回已应用前缀。 */
    @Override public synchronized WalCheckpoint checkpoint() { return checkpoint; }

    /** 应用完整日志并可模拟应用后响应丢失。 */
    @Override public synchronized WalCheckpoint apply(List<WalCommit> commits) {
        onApply.run();
        if (failApply) { throw new IllegalStateException("paused materializer"); }
        for (WalCommit commit : commits) {
            if (commit.sequence() <= checkpoint.sequence()) { continue; }
            if (commit.sequence() != checkpoint.sequence() + 1) { throw new IllegalStateException("log gap"); }
            for (WalMutation mutation : commit.mutations()) {
                if (mutation.shard().equals(shard)) {
                    versions.computeIfAbsent(mutation.key(), ignored -> new TreeMap<>())
                            .put(commit.sequence(), new WalValue(commit.sequence(), mutation.value()));
                }
            }
            checkpoint = new WalCheckpoint(commit.sequence(), commit.digest());
        }
        if (loseApplyResponse) { loseApplyResponse = false; throw new IllegalStateException("lost apply response"); }
        return checkpoint;
    }

    /** 返回快照版本并允许注入读取延迟。 */
    @Override public synchronized Optional<WalValue> get(String key, long readSequence) {
        var chain = versions.get(key);
        var entry = chain == null ? null : chain.floorEntry(readSequence);
        onRead.run();
        return entry == null ? Optional.empty() : Optional.of(entry.getValue());
    }

    /** 返回半开区间内包含墓碑的候选行。 */
    @Override public synchronized NavigableMap<String, WalValue> scan(String from, String to, long readSequence) {
        var result = new TreeMap<String, WalValue>(Utf8KeyOrder::compare);
        versions.forEach((key, chain) -> {
            if ((from == null || Utf8KeyOrder.compare(key, from) >= 0)
                    && (to == null || Utf8KeyOrder.compare(key, to) < 0)) {
                var entry = chain.floorEntry(readSequence);
                if (entry != null) { result.put(key, entry.getValue()); }
            }
        });
        onRead.run();
        return result;
    }

    /** 保留活值安全点锚点，墓碑锚点及旧历史可退休，重放仍受检查点保护。 */
    @Override public synchronized int gc(long safePoint, int keyLimit) {
        int removed = 0;
        int keys = 0;
        var iterator = versions.values().iterator();
        while (iterator.hasNext()) {
            var chain = iterator.next();
            if (keys++ >= keyLimit) { break; }
            Long anchor = chain.floorKey(safePoint);
            if (anchor != null) {
                boolean retired = chain.get(anchor).deleted();
                removed += chain.headMap(anchor, retired).size();
                chain.headMap(anchor, retired).clear();
                if (chain.isEmpty()) { iterator.remove(); }
            }
        }
        return removed;
    }
}
