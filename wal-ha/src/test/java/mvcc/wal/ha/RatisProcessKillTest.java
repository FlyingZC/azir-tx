package mvcc.wal.ha;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 三个独立 JVM 的确认后强杀，不能用同进程对象关闭冒充进程崩溃。 */
class RatisProcessKillTest {
    @TempDir Path directory;

    /** 真实确认后强杀领导者，新主保留确认，原节点带完整持久状态重开并追平。 */
    @Test void acknowledgedCrossShardDataSurvivesJvmKillAndRestart() throws Exception {
        var ports = ports();
        UUID group = UUID.randomUUID(); var children = new ArrayList<Child>();
        try {
            for (int i = 0; i < 3; i++) { children.add(start(i, group, ports)); }
            int first = leader(children); String value = Base64.getUrlEncoder().withoutPadding().encodeToString("confirmed".getBytes(StandardCharsets.UTF_8));
            String ack = children.get(first).request("WRITE " + value); assertTrue(ack.startsWith("COMMITTED "), ack);
            String id = ack.split(" ")[1]; assertEquals("1", ack.split(" ")[2]);
            Files.writeString(directory.resolve("acknowledged.txt"), id + "\t1\t" + value + "\n");
            Child killed = children.get(first); killed.kill();
            int next = leader(children); assertNotEquals(first, next);
            assertEquals("VALUES " + value + " " + value, children.get(next).request("READ")); assertEquals("LOOKUP COMMITTED", children.get(next).request("LOOKUP " + id));
            String newer = Base64.getUrlEncoder().withoutPadding().encodeToString("after-failover".getBytes(StandardCharsets.UTF_8));
            String second = children.get(next).request("WRITE " + newer); assertTrue(second.endsWith(" 2"), second);
            killed.close(); children.set(first, start(first, group, ports));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); long applied = 0;
            while (System.nanoTime() < until) { String status = children.get(first).request("STATUS"); applied = Long.parseLong(status.split(" ")[2]); if (applied >= 2) { break; } }
            assertEquals(2, applied); assertEquals("VALUES " + newer + " " + newer, children.get(next).request("READ"));
        } finally { finish(children, "confirmed-kill-restart"); }
    }

    /** 两个独立副本进程离线，原领导者不能确认新写，也不能凭旧缓存建立新读。 */
    @Test void minorityProcessCannotAcknowledgeOrOpenSnapshot() throws Exception {
        var ports = ports(); UUID group = UUID.randomUUID(); var children = new ArrayList<Child>();
        try {
            for (int i = 0; i < 3; i++) { children.add(start(i, group, ports)); }
            int first = leader(children); var node = children.get(first); String value = value("confirmed");
            assertTrue(node.request("WRITE " + value).endsWith(" 1")); assertTrue(node.request("BUFFER " + value("unconfirmed")).startsWith("BUFFERED "));
            for (int i = 0; i < 3; i++) { if (i != first) { children.get(i).kill(); } }
            String decision = node.request("FINISH"); assertTrue(decision.startsWith("ERROR "), decision);
            assertEquals("1", node.request("STATUS").split(" ")[2]); String read = node.request("READ"); assertTrue(read.startsWith("ERROR "), read);
            assertEquals("1", node.request("STATUS").split(" ")[2]);
        } finally { finish(children, "minority-rejects"); }
    }

    /** 暂停旧主后由多数派切主，旧主恢复时不能提交切主前的本地未决写集。 */
    @Test @EnabledOnOs(OS.LINUX) void pausedOldLeaderCannotCommitOldTransactionAfterNewLeaderWrites() throws Exception {
        var ports = ports(); UUID group = UUID.randomUUID(); var children = new ArrayList<Child>(); Child paused = null;
        try {
            for (int i = 0; i < 3; i++) { children.add(start(i, group, ports)); }
            int first = leader(children); var original = children.get(first); assertTrue(original.request("WRITE " + value("initial")).endsWith(" 1"));
            String oldId = original.request("BUFFER " + value("stale")).split(" ")[1]; original.signal("STOP"); paused = original;
            // 保留原进程和所有本地文件，只让它暂停；发现逻辑排除暂停者，不能把观察超时当作死亡。
            int second = leader(children, first); String winner = value("new-leader"); assertTrue(children.get(second).request("WRITE " + winner).endsWith(" 2"));
            original.signal("CONT"); paused = null;
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); long applied = 0;
            while (System.nanoTime() < until) { String status = original.request("STATUS"); applied = Long.parseLong(status.split(" ")[2]); if (applied >= 2 && status.split(" ")[1].equals("0")) { break; } TimeUnit.MILLISECONDS.sleep(20); }
            assertEquals(2, applied); String decision = original.request("FINISH"); assertTrue(decision.startsWith("ERROR "), decision);
            assertEquals("VALUES " + winner + " " + winner, children.get(second).request("READ"));
            assertEquals("LOOKUP NOT_CONFIRMED", children.get(second).request("LOOKUP " + oldId)); assertEquals("2", children.get(second).request("STATUS").split(" ")[2]);
        } finally {
            try { if (paused != null && paused.process.isAlive()) { paused.signal("CONT"); } }
            finally { finish(children, "paused-old-leader"); }
        }
    }

    /** 同时保留三个端口直到分配结束，降低本测试内部端口重复概率。 */
    private static List<Integer> ports() throws Exception {
        var result = new ArrayList<Integer>(); var held = new ArrayList<java.net.ServerSocket>();
        try { for (int i = 0; i < 3; i++) { var socket = new java.net.ServerSocket(0); held.add(socket); result.add(socket.getLocalPort()); } }
        finally { for (var socket : held) { socket.close(); } } return result;
    }

    /** 结果载荷固定编码，不把 INTENT 或命令发送完成当作成功确认。 */
    private static String value(String text) { return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8)); }

    /** 尽力清理所有自有进程，再按显式证据目录保存日志，失败也保留可复查现场。 */
    private void finish(List<Child> children, String name) throws Exception {
        Throwable failure = null;
        try {
            for (var child : children) { try { child.close(); } catch (Throwable error) { if (failure == null) { failure = error; } else { failure.addSuppressed(error); } } }
        } finally {
            String configured = System.getProperty("azir.wal.ha.processEvidenceDir");
            if (configured != null) {
                Path target = Path.of(configured).toAbsolutePath().resolve(name + "-" + UUID.randomUUID()); Files.createDirectories(target);
                try (var paths = Files.walk(directory)) {
                    for (var path : paths.filter(Files::isRegularFile).toList()) {
                        if (!path.getFileName().toString().endsWith(".log") && !path.getFileName().toString().endsWith(".txt")) { continue; }
                        Path output = target.resolve(directory.relativize(path)); Files.createDirectories(output.getParent()); Files.copy(path, output);
                    }
                }
            }
        }
        if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; }
    }

    /** 子 JVM 最大堆 128 MiB、一个处理器，所有目录和标准流仅属于本次测试。 */
    private Child start(int index, UUID group, List<Integer> ports) throws Exception {
        Path root = directory.resolve("p" + index); Files.createDirectories(root);
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-XX:ActiveProcessorCount=1", "-cp", cp,
                RatisWalProcessProbe.class.getName(), root.toString(), "p" + index, group.toString())); for (int port : ports) { args.add(Integer.toString(port)); }
        var process = new ProcessBuilder(args).redirectError(root.resolve("process-" + System.nanoTime() + ".log").toFile()).start();
        var child = new Child(process, root.resolve("commands-" + process.pid() + ".txt")); String started = child.responses.poll(30, TimeUnit.SECONDS);
        if (started == null || !started.startsWith("STARTED ")) { child.close(); throw new AssertionError("probe did not start: " + started); }
        assertEquals(Long.toString(process.pid()), started.split(" ")[1]); child.record(started); return child;
    }

    /** 先核对 ProcessHandle 存活，再用实际多数派屏障证明候选领导者能服务。 */
    private static int leader(List<Child> children) throws Exception {
        return leader(children, -1);
    }

    /** 暂停节点仍然存活，明确排除后等待其余多数派选出实际可服务领导者。 */
    private static int leader(List<Child> children, int excluded) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < until) {
            for (int i = 0; i < children.size(); i++) {
                if (i == excluded) { continue; }
                var child = children.get(i); if (!child.process.isAlive()) { continue; }
                String status = child.request("STATUS");
                if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && child.request("PING").startsWith("PING ")) { return i; }
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        throw new AssertionError("no process with ready majority barrier");
    }

    /** 每个进程一个有界结果队列，观察超时本身不触发重启。 */
    private static final class Child implements AutoCloseable {
        final Process process; final java.io.BufferedWriter commands; final java.util.concurrent.BlockingQueue<String> responses = new java.util.concurrent.ArrayBlockingQueue<>(64);
        final Thread reader;
        final Path history;

        /** 持续读取自有 stdout，库日志单独保存在 stderr 文件。 */
        Child(Process process, Path history) {
            this.process = process; this.history = history; commands = new java.io.BufferedWriter(new java.io.OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            reader = Thread.ofPlatform().daemon().name("wal-probe-output-" + process.pid()).start(() -> {
                try (var input = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line; (line = input.readLine()) != null;) { if (line.startsWith("AZIR ")) { responses.put(line.substring(5)); } }
                } catch (Exception failure) { responses.offer("READER_ENDED"); }
            });
        }

        /** 相同进程串行发送调度命令，INTENT 不是确认成功。 */
        synchronized String request(String command) throws Exception {
            if (!process.isAlive()) { throw new AssertionError("probe process already terminal"); }
            record("SEND " + command);
            commands.write(command); commands.newLine(); commands.flush();
            for (String response; (response = responses.poll(10, TimeUnit.SECONDS)) != null;) { record("RECV " + response); if (!response.startsWith("INTENT ")) { return response; } }
            throw new AssertionError("probe observation timed out while live=" + process.isAlive());
        }

        /** 只记录父进程实际收到的结果，用于独立核对成功前缀和故障顺序。 */
        private void record(String line) throws Exception { Files.writeString(history, System.nanoTime() + "\t" + line + "\n", StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND); }

        /** 强杀明确持有的测试子进程并确认终态，不能按模糊进程名批量终止。 */
        void kill() throws Exception { record("KILL " + process.pid()); process.destroyForcibly(); assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertNotEquals(0, process.exitValue()); }

        /** Linux 暂停和恢复只作用于仍存活的自有 ProcessHandle；暂停不被当作进程退出。 */
        void signal(String signal) throws Exception {
            if ((!signal.equals("STOP") && !signal.equals("CONT")) || !process.isAlive()) { throw new IllegalStateException("invalid owned process signal"); }
            record("SIGNAL " + signal + " " + process.pid()); var command = new ProcessBuilder("/bin/kill", "-" + signal, Long.toString(process.pid())).start();
            assertTrue(command.waitFor(5, TimeUnit.SECONDS)); assertEquals(0, command.exitValue());
        }

        /** 清理只停止本测试明确创建的进程，不查找或终止用户其他 Java 任务。 */
        @Override public void close() throws Exception {
            if (process.isAlive()) {
                try { commands.write("STOP"); commands.newLine(); commands.flush(); } catch (Exception ignored) { }
                if (!process.waitFor(15, TimeUnit.SECONDS)) { process.destroyForcibly(); assertTrue(process.waitFor(10, TimeUnit.SECONDS)); }
            }
            commands.close(); reader.join(2000);
        }
    }
}
