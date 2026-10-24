package mvcc.wal;

import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 多次真实强杀和重开同一数据目录，逐笔核对成功前缀及未确认决议。 */
class WalCrashMatrixTest {
    @TempDir Path directory;

    /** 固定边界及随机时刻场景的独立编号。 */
    private record CrashCase(String stage, int repetition) {
        /** 在测试报告中保留可重现的场景标签。 */
        @Override public String toString() { return stage + "-" + repetition; }
    }
    /** 调用方在强杀前实际获得的尝试或确认。 */
    private record Event(String kind, UUID id, long sequence, String key, String value) { }

    /** 普通回归执行每个边界一次，正式次数由显式验收参数控制。 */
    static Stream<CrashCase> cases() {
        int repeats = Integer.getInteger("azir.wal.crashRepeats", 1);
        int random = Integer.getInteger("azir.wal.randomCrashes", 2);
        return Stream.concat(Stream.of("BEFORE_APPEND", "AFTER_FORCE", "BEFORE_PUBLISH", "DURING_INSTALL", "AFTER_PUBLISH", "ACK", "RECOVERY")
                .flatMap(stage -> IntStream.range(0, repeats).mapToObj(n -> new CrashCase(stage, n))),
                IntStream.range(0, random).mapToObj(n -> new CrashCase("RANDOM", n)));
    }

    /** 所有进程均由本测试创建，强杀后必须等待退出才能尝试恢复。 */
    @ParameterizedTest @MethodSource("cases")
    void successfulTransactionsSurviveRealProcessDeath(CrashCase scenario) throws Exception {
        boolean random = scenario.stage.equals("RANDOM");
        var child = launch(random ? WalCrashWorkloadProcess.class : WalCrashProcess.class,
                random ? List.of(directory.toString()) : List.of(directory.toString(), scenario.stage.equals("RECOVERY") ? "ACK" : scenario.stage));
        try {
            waitReady(child, "ready");
            if (random) { Thread.sleep(new Random(99123L + scenario.repetition).nextInt(21)); }
        } finally {
            child.destroyForcibly(); assertTrue(child.waitFor(30, TimeUnit.SECONDS), "owned child must terminate");
        }
        if (scenario.stage.equals("RECOVERY")) {
            var recovering = launch(WalReopenProcess.class, List.of(directory.toString()));
            try { waitReady(recovering, "recovery-ready"); }
            finally { recovering.destroyForcibly(); assertTrue(recovering.waitFor(30, TimeUnit.SECONDS)); }
        }
        if (random) { verifyRandom(scenario); }
        else { verifyBoundary(scenario); }
    }

    /** 子 JVM 只使用当前编译产物与隔离目录，不触碰任何既有服务。 */
    private Process launch(Class<?> main, List<String> arguments) throws IOException {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx128m"); command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(main.getName()); command.addAll(arguments);
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
    }

    /** 使用宽松但有界的调度预算，不把其他本机任务的短时负载误判为协议失败。 */
    private void waitReady(Process child, String barrier) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!Files.exists(directory.resolve(barrier))) {
            if (!child.isAlive() || System.nanoTime() >= deadline) {
                fail("child did not reach barrier: " + Files.readString(directory.resolve("child.log")));
            }
            Thread.sleep(2);
        }
    }

    /** 固定边界必须恢复整笔跨片事务，追加前则没有持久提交。 */
    private void verifyBoundary(CrashCase scenario) throws IOException {
        UUID id = UUID.fromString(Files.readString(directory.resolve("transaction-id")));
        boolean committed = !scenario.stage.equals("BEFORE_APPEND");
        for (int reopen = 0; reopen < 2; reopen++) {
            try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
                assertEquals(committed, runtime.findCommitted(id).isPresent());
                var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
                assertEquals(committed, runtime.get(tx, "a", "balance", snapshot).isPresent());
                assertEquals(committed, runtime.get(tx, "b", "balance", snapshot).isPresent());
                if (committed) {
                    assertArrayEquals(new byte[]{90}, runtime.get(tx, "a", "balance", snapshot).orElseThrow());
                    assertArrayEquals(new byte[]{110}, runtime.get(tx, "b", "balance", snapshot).orElseThrow());
                    assertEquals(1, runtime.findCommitted(id).orElseThrow().commitSequence());
                }
                runtime.rollback(tx);
            }
        }
        save(scenario, "id\tcommitted\n" + id + "\t" + committed + "\n", null);
    }

    /** 已确认记录必须存在，未确认记录按恢复日志核对，最终两片逐键相同。 */
    private void verifyRandom(CrashCase scenario) throws IOException {
        String content = Files.readString(directory.resolve("events.tsv"));
        var lines = content.substring(0, content.lastIndexOf('\n') + 1).lines().toList();
        var events = new ArrayList<Event>();
        for (String line : lines) {
            String[] fields = line.split("\t", -1);
            // 只把完整换行终结的事件作为观测，不能将半行当作确认。
            assertEquals(5, fields.length, "corrupt event evidence");
            events.add(new Event(fields[0], UUID.fromString(fields[1]), Long.parseLong(fields[2]), fields[3], fields[4]));
        }
        var attempts = new HashMap<UUID, Event>(); var acknowledgements = new HashMap<UUID, Long>();
        for (Event event : events) {
            if (event.kind.equals("ATTEMPT")) { assertNull(attempts.put(event.id, event)); }
            else { assertEquals("ACK", event.kind); assertTrue(attempts.containsKey(event.id));
                assertNull(acknowledgements.put(event.id, event.sequence)); }
        }
        assertTrue(acknowledgements.size() >= 3, "must kill after a real acknowledged prefix");
        var first = recoverRandom(attempts, acknowledgements);
        var second = recoverRandom(attempts, acknowledgements); assertEquals(first, second, "recovery must be idempotent");
        var output = new StringBuilder("id\tsequence\tkey\tvalue\tacknowledged\n");
        for (var entry : first.entrySet()) {
            var event = attempts.get(entry.getKey());
            output.append(event.id).append('\t').append(entry.getValue()).append('\t').append(event.key).append('\t')
                    .append(event.value).append('\t').append(acknowledgements.containsKey(event.id)).append('\n');
        }
        save(scenario, output.toString(), directory.resolve("events.tsv"));
    }

    /** 参考模型只使用外部尝试与恢复凭据，不读取内部版本链。 */
    private Map<UUID, Long> recoverRandom(Map<UUID, Event> attempts, Map<UUID, Long> acknowledgements) {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            var recovered = new HashMap<UUID, Long>(); var latest = new HashMap<String, Event>();
            var ordered = new java.util.TreeMap<Long, Event>();
            attempts.forEach((id, attempt) -> runtime.findCommitted(id).ifPresent(receipt -> {
                assertNull(recovered.put(id, receipt.commitSequence()));
                assertNull(ordered.put(receipt.commitSequence(), attempt));
            }));
            acknowledgements.forEach((id, sequence) -> assertEquals(sequence, recovered.get(id), "acknowledged transaction lost"));
            assertEquals(runtime.stats().publishedSequence(), recovered.size(), "unexplained durable transaction");
            long expectedSequence = 0;
            for (var entry : ordered.entrySet()) {
                assertEquals(++expectedSequence, entry.getKey()); latest.put(entry.getValue().key, entry.getValue());
            }
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(tx);
            for (String shard : Set.of("a", "b")) {
                var scanned = runtime.scan(tx, shard, null, null, snapshot);
                var expectedKeys = new java.util.TreeSet<String>();
                for (var entry : latest.entrySet()) {
                    String expected = entry.getValue().value.equals("<NULL>") ? null : entry.getValue().value;
                    assertEquals(expected, runtime.get(tx, shard, entry.getKey(), snapshot).map(WalMvccRuntimeTest::text).orElse(null));
                    if (expected != null) { expectedKeys.add(entry.getKey()); assertEquals(expected, text(scanned.get(entry.getKey()))); }
                }
                assertEquals(expectedKeys, scanned.keySet(), "scan diverges after recovery");
            }
            runtime.rollback(tx); return recovered;
        }
    }

    /** 成功才保存验收证据，包含随机强杀前的原始尝试与确认记录。 */
    private static void save(CrashCase scenario, String result, Path source) throws IOException {
        String target = System.getProperty("azir.wal.crashEvidence"); if (target == null) { return; }
        Path root = Path.of(target); Files.createDirectories(root);
        Files.writeString(root.resolve(scenario + "-recovered.tsv"), result, StandardCharsets.UTF_8);
        if (source != null) { Files.copy(source, root.resolve(scenario + "-events.tsv")); }
    }
}
