package mvcc.wal;

import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 三写三读限速交错分页、回收及检查点，独立模型逐键核对扫描结果而不是只比较两片相等。 */
class WalCheckpointConcurrentScanTest {
    @TempDir Path directory;

    /** 每笔语句保持同一快照完成全部页面，检查点及 GC 不得导致漏行、重行或错误版本。 */
    @Test void pagedSnapshotsRemainExactDuringRepeatedCheckpoints() throws Exception {
        String saved = System.getProperty("azir.wal.rangeEvidence");
        Path evidence = saved == null ? directory.resolve("history.tsv.gz") : Path.of(saved);
        if (saved != null) { Files.createDirectories(evidence.getParent()); }
        var running = new AtomicBoolean(true); var keys = new ArrayList<String>();
        for (int worker = 0; worker < 3; worker++) { for (int k = 0; k < 8; k++) { keys.add("w" + worker + ":k" + k); } }
        var a = new TestMaterializedStore(); var b = new TestMaterializedStore();
        var policy = new WalRetentionOptions(200, 3_600_000, 4_000_000, 1_000_000, 1000);
        try (var model = new WalSteadyModel(evidence);
             var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults().withHotCacheBytes(0), policy);
             var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            runtime.put(seed, "a", keys.getFirst(), bytes("seed")); runtime.put(seed, "b", keys.getFirst(), bytes("seed"));
            model.committed(runtime.commit(seed), seed.startSequence(), keys.getFirst(), "seed");
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            try {
                for (int worker = 0; worker < 3; worker++) {
                    int id = worker;
                    futures.add(tasks.submit(() -> {
                        int ordinal = 0;
                        try {
                            while (running.get()) {
                                java.util.concurrent.locks.LockSupport.parkNanos(30_000_000);
                                String key = "w" + id + ":k" + ordinal % 8; String value = ordinal % 7 == 0 ? null : id + ":" + ordinal;
                                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                                for (String shard : new String[]{"a", "b"}) {
                                    if (value == null) { runtime.delete(tx, shard, key); } else { runtime.put(tx, shard, key, bytes(value)); }
                                }
                                model.committed(runtime.commit(tx), tx.startSequence(), key, value); ordinal++;
                            }
                        } catch (Exception failure) { throw new IllegalStateException(failure); }
                    }));
                }
                for (int worker = 0; worker < 3; worker++) {
                    int id = worker;
                    futures.add(tasks.submit(() -> {
                        long ordinal = 0;
                        try {
                            while (running.get()) {
                                String token = "scan:" + id + ":" + ordinal;
                                long lower = model.pin(token); var level = ordinal++ % 2 == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED;
                                WalTransaction tx = null;
                                try {
                                    tx = runtime.begin(level); assertTrue(tx.startSequence() >= lower);
                                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                    while (running.get() && System.nanoTime() < end) {
                                        java.util.concurrent.locks.LockSupport.parkNanos(100_000_000);
                                        long minimum = model.known(); var snapshot = runtime.beginReadStatement(tx);
                                        var left = pages(runtime, tx, "a", snapshot); var right = pages(runtime, tx, "b", snapshot);
                                        assertTrue(keys.containsAll(left.keySet())); assertTrue(keys.containsAll(right.keySet()));
                                        for (String key : keys) { model.observed(token, snapshot.sequence(), key, left.get(key), right.get(key), level, tx.startSequence(), minimum, true); }
                                    }
                                } finally { if (tx != null) { runtime.rollback(tx); } model.unpin(token); }
                            }
                        } catch (Exception failure) { throw new IllegalStateException(failure); }
                    }));
                }
                for (int cycle = 0; cycle < 100; cycle++) {
                    Thread.sleep(50); runtime.collectGarbage(64); assertTrue(runtime.checkpoint().created());
                }
            } finally { running.set(false); }
            for (var future : futures) { future.get(30, TimeUnit.SECONDS); }
            assertEquals(model.known(), runtime.stats().publishedSequence()); assertTrue(model.observations() >= 240);
        }
    }

    /** 同一语句的全部页面使用同一令牌，并显式检测重复键和不终止的游标。 */
    private static Map<String, String> pages(WalMvccRuntime runtime, WalTransaction tx, String shard, WalReadSnapshot snapshot) {
        var result = new LinkedHashMap<String, String>(); String cursor = null; int count = 0;
        do {
            var page = runtime.scanPage(tx, shard, null, null, cursor, 3, snapshot);
            page.rows().forEach((key, value) -> assertNull(result.put(key, new String(value, StandardCharsets.UTF_8))));
            cursor = page.nextAfterKey(); assertTrue(++count < 64);
        } while (cursor != null);
        var ordered = result.keySet().stream().sorted(mvcc.storage.Utf8KeyOrder::compare).toList(); assertEquals(ordered, new ArrayList<>(result.keySet()));
        return result;
    }
}
