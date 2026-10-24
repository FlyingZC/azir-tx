package mvcc.wal;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 穷举两个写者和一个两次读取者的所有有界 API 交错，物化与 GC 穿插每步。 */
class WalBoundedInterleavingTest {
    @TempDir Path directory;

    /** 单个有界状态空间中的路由、隔离与调度。 */
    private record Schedule(IsolationLevel isolation, boolean sharedKey, String actors, long cacheBytes) {
        /** 在报告中输出准确交错，失败时无需猜测线程调度。 */
        @Override public String toString() { return isolation + "-" + (sharedKey ? "shared" : "disjoint") + "-" + actors + "-cache=" + cacheBytes; }
    }

    /** 两写者各两步、读者三步，共 210 种交错，冷热缓存及隔离/冲突组合共 1680 个案例。 */
    static Stream<Schedule> schedules() {
        var orders = new ArrayList<String>(); enumerate(new int[]{2, 2, 3}, "", orders);
        return Stream.of(IsolationLevel.READ_COMMITTED, IsolationLevel.REPEATABLE_READ)
                .flatMap(level -> Stream.of(true, false).flatMap(shared -> Stream.of(0L, 8192L)
                        .flatMap(cache -> orders.stream().map(order -> new Schedule(level, shared, order, cache)))));
    }

    /** 递归枚举每个角色内顺序不变的所有拓扑交错。 */
    private static void enumerate(int[] remaining, String prefix, List<String> result) {
        if (remaining[0] + remaining[1] + remaining[2] == 0) { result.add(prefix); return; }
        for (int actor = 0; actor < remaining.length; actor++) {
            if (remaining[actor] > 0) {
                remaining[actor]--; enumerate(remaining, prefix + actor, result); remaining[actor]++;
            }
        }
    }

    /** 独立模型裁决同键冲突，核对旧 RR、刷新 RC、墓碑及后台回收。 */
    @ParameterizedTest @MethodSource("schedules")
    void allBoundedApiSchedulesPreserveSnapshots(Schedule schedule) {
        var a = new TestMaterializedStore(); var b = new TestMaterializedStore();
        var chains = new TreeMap<String, TreeMap<Long, String>>();
        for (String key : List.of("k1", "k2")) { var chain = new TreeMap<Long, String>(); chain.put(1L, "seed"); chains.put(key, chain); }
        var handles = new WalTransaction[3]; var positions = new int[3]; var active = new boolean[3];
        long published = 1;
        try (var runtime = WalMvccRuntime.open(directory, Map.of("a", a, "b", b), WalOptions.defaults().withHotCacheBytes(schedule.cacheBytes))) {
            var seed = runtime.begin(IsolationLevel.REPEATABLE_READ);
            for (String shard : List.of("a", "b")) { for (String key : chains.keySet()) { runtime.put(seed, shard, key, bytes("seed")); } }
            assertEquals(1, runtime.commit(seed).commitSequence());
            for (char digit : schedule.actors.toCharArray()) {
                int actor = digit - '0'; int step = positions[actor]++;
                if (step == 0) {
                    var isolation = actor == 2 ? schedule.isolation : IsolationLevel.REPEATABLE_READ;
                    handles[actor] = runtime.begin(isolation); active[actor] = true;
                    assertEquals(published, handles[actor].startSequence());
                    if (actor < 2) {
                        String key = actor == 0 || schedule.sharedKey ? "k1" : "k2";
                        for (String shard : List.of("a", "b")) {
                            if (actor == 0) { runtime.delete(handles[actor], shard, key); }
                            else { runtime.put(handles[actor], shard, key, bytes("writer1")); }
                        }
                    }
                } else if (actor < 2) {
                    String key = actor == 0 || schedule.sharedKey ? "k1" : "k2";
                    var tx = handles[actor];
                    if (chains.get(key).lastKey() > tx.startSequence()) {
                        assertEquals(ErrorCode.WRITE_CONFLICT, assertThrows(WalMvccException.class, () -> runtime.commit(tx)).errorCode());
                        assertTrue(runtime.findCommitted(tx.id()).isEmpty());
                    } else {
                        var receipt = runtime.commit(tx); assertEquals(++published, receipt.commitSequence());
                        chains.get(key).put(published, actor == 0 ? null : "writer1");
                        assertEquals(receipt, runtime.commit(tx), "duplicate commit must keep one decision");
                    }
                    active[actor] = false;
                } else {
                    var tx = handles[2]; var snapshot = runtime.beginReadStatement(tx);
                    long expectedSnapshot = schedule.isolation == IsolationLevel.REPEATABLE_READ ? tx.startSequence() : published;
                    assertEquals(expectedSnapshot, snapshot.sequence());
                    var expected = new TreeMap<String, String>();
                    chains.forEach((key, chain) -> {
                        String value = chain.floorEntry(expectedSnapshot).getValue(); if (value != null) { expected.put(key, value); }
                    });
                    for (String shard : List.of("a", "b")) {
                        for (String key : chains.keySet()) {
                            assertEquals(chains.get(key).floorEntry(expectedSnapshot).getValue(),
                                    runtime.get(tx, shard, key, snapshot).map(WalMvccRuntimeTest::text).orElse(null));
                        }
                        var scanned = new TreeMap<String, String>();
                        runtime.scan(tx, shard, null, null, snapshot).forEach((key, value) -> scanned.put(key, text(value)));
                        assertEquals(expected, scanned);
                    }
                    if (step == 2) { runtime.rollback(tx); active[2] = false; }
                }
                runtime.flush(); runtime.collectGarbage(4096);
                long oldest = published;
                for (int i = 0; i < 3; i++) { if (active[i]) { oldest = Math.min(oldest, handles[i].startSequence()); } }
                assertEquals(oldest, runtime.stats().safePoint(), "safe point must match live snapshot protection");
                assertEquals(published, runtime.stats().publishedSequence());
            }
            assertEquals(published, runtime.stats().safePoint());
            for (String key : chains.keySet()) {
                var last = chains.get(key).lastEntry();
                for (var store : List.of(a, b)) {
                    var found = store.get(key, Long.MAX_VALUE);
                    if (last.getValue() == null) { assertTrue(found.isEmpty(), "safe tombstone and older versions must be retired"); continue; }
                    var physical = found.orElseThrow();
                    assertEquals(last.getKey(), physical.commitSequence());
                    assertEquals(last.getValue(), physical.deleted() ? null : text(physical.value()));
                }
            }
        }
    }
}
