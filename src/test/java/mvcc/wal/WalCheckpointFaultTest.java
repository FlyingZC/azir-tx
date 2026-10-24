package mvcc.wal;

import mvcc.common.IsolationLevel;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static mvcc.wal.WalMvccRuntimeTest.*;
import static org.junit.jupiter.api.Assertions.*;

/** 对新代文件、原子指针和旧代删除执行真实进程强杀，验证每次成功事务都可恢复。 */
class WalCheckpointFaultTest {
    @TempDir Path directory;

    /** 独立目录下的可复现故障身份。 */
    private record Case(String stage, int repetition) {
        /** 在报告中保留阶段及轮次。 */
        @Override public String toString() { return stage + "-" + repetition; }
    }

    /** 普通回归每阶段一次，正式测试可扩展重复次数并单独控制子进程启动频率。 */
    static Stream<Case> cases() {
        return Stream.of("BEFORE_CHECKPOINT_FILES", "AFTER_RECEIPTS_FORCE", "AFTER_SNAPSHOT_FORCE", "BEFORE_CURRENT_MOVE",
                "AFTER_CURRENT_MOVE", "AFTER_CURRENT_FORCE", "BEFORE_OLD_GENERATION_DELETE", "AFTER_OLD_GENERATION_DELETE",
                "PARTIAL_IMAGE", "PARTIAL_RECEIPT", "PARTIAL_POINTER").flatMap(stage ->
                IntStream.range(0, Integer.getInteger("azir.wal.checkpointFaultRepeats", 1)).mapToObj(n -> new Case(stage, n)));
    }

    /** 任一边界崩溃后必须恢复同一个完整前缀，重复重开及后续追加也不能改变决议。 */
    @ParameterizedTest @MethodSource("cases")
    void killedCheckpointNeverLosesAcknowledgedTransactions(Case scenario) throws Exception {
        long began = System.nanoTime();
        var command = new ArrayList<String>(); command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-Xmx128m"); command.add("-XX:ActiveProcessorCount=1"); command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(WalCheckpointCrashProcess.class.getName()); command.add(directory.toString()); command.add(scenario.stage);
        var child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!Files.exists(directory.resolve("ready"))) {
                if (!child.isAlive() || System.nanoTime() > deadline) { fail("owned child missed checkpoint barrier: " + Files.readString(directory.resolve("child.log"))); }
                Thread.sleep(2);
            }
        } finally { child.destroyForcibly(); assertTrue(child.waitFor(30, TimeUnit.SECONDS)); }
        UUID old = UUID.fromString(Files.readString(directory.resolve("old-id"))); UUID current = UUID.fromString(Files.readString(directory.resolve("new-id")));
        for (int attempt = 0; attempt < 2; attempt++) {
            try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
                assertEquals(2, runtime.stats().publishedSequence());
                assertEquals(1, runtime.findCommitted(old).orElseThrow().commitSequence());
                assertEquals(2, runtime.findCommitted(current).orElseThrow().commitSequence());
                var reader = runtime.begin(IsolationLevel.REPEATABLE_READ); var snapshot = runtime.beginReadStatement(reader);
                assertEquals("new", text(runtime.get(reader, "a", "k", snapshot).orElseThrow()));
                assertEquals("new", text(runtime.get(reader, "b", "k", snapshot).orElseThrow())); runtime.rollback(reader);
            }
        }
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a", "b"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); runtime.put(tx, "a", "after", bytes("continued"));
            assertEquals(3, runtime.commit(tx).commitSequence()); assertTrue(runtime.checkpoint().created());
        }
        String evidence = System.getProperty("azir.wal.checkpointEvidence");
        if (evidence != null) {
            Path root = Path.of(evidence); Files.createDirectories(root);
            Files.writeString(root.resolve(scenario + ".tsv"), "old_id\tnew_id\tconfirmed_sequence\n" + old + "\t" + current + "\t2\n");
        }
        // 正式验收通过间隔限制本测试子进程的启动率，不占用用户其它任务的调度设置。
        long minimum = TimeUnit.MILLISECONDS.toNanos(Long.getLong("azir.wal.checkpointFaultIntervalMillis", 0));
        long remaining = minimum - (System.nanoTime() - began);
        if (remaining > 0) { TimeUnit.NANOSECONDS.sleep(remaining); }
    }
}
