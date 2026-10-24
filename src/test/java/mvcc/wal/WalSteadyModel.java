package mvcc.wal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;
import static org.junit.jupiter.api.Assertions.*;

/** 独立有界版本模型，参考数据只来自调用方成功凭据和计划写集，不读取引擎版本链。 */
final class WalSteadyModel implements AutoCloseable {
    /** 前台已经确认但可能尚未按全局顺序进入模型的写事务。 */
    private record Commit(WalReceipt receipt, long start, String key, String value) { }
    private final Map<String, TreeMap<Long, String>> chains = new HashMap<>();
    private final Map<String, Long> pins = new HashMap<>();
    private final TreeMap<Long, Commit> pending = new TreeMap<>();
    private final BufferedWriter trace;
    private long known;
    private long observations;
    private long samples;
    private WalReceipt firstReceipt;
    private WalReceipt lastReceipt;

    /** 流式保留证据，长稳不能把全部累计历史装进 JVM。 */
    WalSteadyModel(Path path) throws IOException {
        trace = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path,
                java.nio.file.StandardOpenOption.CREATE_NEW)), StandardCharsets.UTF_8)); trace.write("H\t2\n");
    }

    /** 在引擎 begin 之前保护较早的已知边界，避免模型回收与读者登记竞争。 */
    synchronized long pin(String token) throws IOException {
        assertNull(pins.put(token, known)); trace.write("B\t" + token + "\t" + known + "\n"); return known;
    }

    /** 事务结束后释放模型保护，并清理不再需要的历史锚点。 */
    synchronized void unpin(String token) throws IOException {
        assertNotNull(pins.remove(token)); trace.write("E\t" + token + "\n"); prune();
    }

    /** 成功结果可能乱序返回，必须先补齐连续提交前缀再开放参考读取。 */
    synchronized void committed(WalReceipt receipt, long start, String key, String value) throws IOException {
        assertTrue(receipt.commitSequence() > known); assertNull(pending.put(receipt.commitSequence(), new Commit(receipt, start, key, value)));
        while (pending.containsKey(known + 1)) {
            var commit = pending.remove(++known); var chain = chains.computeIfAbsent(commit.key, ignored -> new TreeMap<>());
            assertTrue(commit.start < known); if (!chain.isEmpty()) { assertTrue(chain.lastKey() <= commit.start, "stale writer accepted"); }
            chain.put(known, commit.value);
            if (firstReceipt == null) { firstReceipt = commit.receipt; } lastReceipt = commit.receipt;
            trace.write("C\t" + known + "\t" + commit.start + "\t" + commit.receipt.transactionId() + "\t" + commit.key + "\t" + scalar(commit.value) + "\n");
        }
        prune(); notifyAll();
    }

    /** 每次观测都精确核对可见版本，只抽样写出读取证据以限制长期磁盘占用。 */
    synchronized void observed(String token, long snapshot, String key, String left, String right,
                               mvcc.common.IsolationLevel isolation, long start, long minimum, boolean sample) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (known < snapshot) {
            if (System.nanoTime() > deadline) { throw new AssertionError("reference model could not catch up"); } wait(10);
        }
        assertTrue(pins.containsKey(token)); assertTrue(snapshot >= pins.get(token));
        var chain = chains.get(key); var entry = chain == null ? null : chain.floorEntry(snapshot);
        String expected = entry == null ? null : entry.getValue();
        assertEquals(expected, left, "invalid left snapshot version"); assertEquals(expected, right, "invalid right snapshot version");
        observations++;
        if (sample) { samples++; trace.write("R\t" + token + "\t" + snapshot + "\t" + key + "\t" + scalar(left)
                + "\t" + scalar(right) + "\t" + isolation + "\t" + start + "\t" + minimum + "\n"); }
    }

    /** 返回目前由调用方成功结果确认的连续水位，用于独立检查 RC 的实时下界。 */
    synchronized long known() { return known; }

    /** 返回已核对的读取数。 */
    synchronized long observations() { return observations; }

    /** 返回已保存原始证据的读取样本数。 */
    synchronized long samples() { return samples; }

    /** 返回最早确认身份，以验证短测试窗口确实发生凭据回收。 */
    synchronized WalReceipt firstReceipt() { return firstReceipt; }

    /** 返回最后一个连续确认身份，以验证重开后仍能查询最新结果。 */
    synchronized WalReceipt lastReceipt() { return lastReceipt; }

    /** 返回最终参考状态的副本，供绕过引擎缓存核对真实 MySQL。 */
    synchronized Map<String, WalValue> finalValues() {
        var result = new HashMap<String, WalValue>();
        chains.forEach((key, chain) -> { var last = chain.lastEntry(); result.put(key,
                new WalValue(last.getKey(), last.getValue() == null ? null : last.getValue().getBytes(StandardCharsets.UTF_8))); });
        return result;
    }

    /** 周期刷新证据缓冲，业务计数与容量压测无关。 */
    synchronized void flush() throws IOException { trace.flush(); }

    /** 固定活跃读者数量时只保留其快照锚点和更新版本，模型自身也不能无限增长。 */
    private void prune() {
        long safe = pins.values().stream().mapToLong(Long::longValue).min().orElse(known);
        for (var chain : chains.values()) { Long anchor = chain.floorKey(safe); if (anchor != null) { chain.headMap(anchor, false).clear(); } }
    }

    /** 合成业务值不会与这个墓碑标记重名。 */
    private static String scalar(String value) { return value == null ? "-" : value; }

    /** 完成证据必须包含最终计数和 gzip 尾部，未完成的文件不能作为通过凭证。 */
    @Override public synchronized void close() throws IOException {
        assertTrue(pins.isEmpty()); assertTrue(pending.isEmpty());
        trace.write("Z\t" + known + "\t" + observations + "\t" + samples + "\n"); trace.close();
    }
}
