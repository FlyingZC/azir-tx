package mvcc.wal.ha;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.lifecycle.Startables;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** 三个独立 JVM 和三份 MySQL 的实际安装强杀，核对确认前缀、代次与完整双片摘要。 */
class RatisMysqlProcessCrashTest {
    static final List<org.testcontainers.containers.MySQLContainer<?>> DATABASES = List.of(MysqlGenerationSupport.database(), MysqlGenerationSupport.database(), MysqlGenerationSupport.database());
    @TempDir Path directory;

    /** 全类复用三个自有受限实例，各参数场景使用新的控制库和组身份。 */
    @BeforeAll static void startDatabases() { Startables.deepStart(DATABASES.stream()).join(); }

    /** 只移除本测试创建的数据库容器，保留其他用户服务。 */
    @AfterAll static void stopDatabases() { for (var database : DATABASES) { database.stop(); } }

    /** 只在确认实际边界后强杀，重启沿用投票及业务目录，最终切主核对所有确认凭据。 */
    @ParameterizedTest @ValueSource(strings = {"BEFORE_INPUT_FORCE", "AFTER_INSTALL_RECORD", "MID_RESTORE_BATCH", "AFTER_RESTORE", "BEFORE_ACTIVE", "AFTER_ACTIVE", "AFTER_CLEARED"})
    void installationSurvivesRealProcessKill(String boundary) throws Exception {
        try (var cluster = new Cluster(directory, boundary)) {
            int source = cluster.leader(-1); var receipts = new ArrayList<Receipt>(); receipts.add(commit(cluster.nodes.get(source), "INIT", 1));
            int target = (source + 1) % 3; cluster.cold(target, 1); var before = cluster.active(target); cluster.nodes.get(target).close();
            for (int value = 1; value <= 10; value++) { receipts.add(commit(cluster.nodes.get(source), "WRITE " + value, value + 1)); }
            receipts.add(commit(cluster.nodes.get(source), "DELETE", 12)); cluster.cold(source, 12);
            long snapshot = Long.parseLong(cluster.nodes.get(source).request("SNAPSHOT").split(" ")[1]);
            String purged = cluster.nodes.get(source).request("PURGE " + snapshot); assertTrue(purged.startsWith("PURGED "), purged); assertTrue(Long.parseLong(purged.split(" ")[1]) > 2);
            cluster.replace(target, boundary); cluster.nodes.get(target).boundary(boundary); UUID expectedGeneration = cluster.capture(target, "first");
            if (boundary.equals("MID_RESTORE_BATCH")) { cluster.partial(target); }
            cluster.nodes.get(target).kill(boundary); int kills = 1; source = cluster.leader(target); receipts.add(commit(cluster.nodes.get(source), "LIVE 1", 13));
            if (boundary.equals("AFTER_INSTALL_RECORD")) {
                cluster.replace(target, "MID_RESTORE_BATCH"); cluster.nodes.get(target).boundary("MID_RESTORE_BATCH"); UUID recovered = cluster.capture(target, "recovery");
                assertEquals(expectedGeneration, recovered); cluster.partial(target); cluster.nodes.get(target).kill("MID_RESTORE_BATCH"); kills++;
                source = cluster.leader(target); receipts.add(commit(cluster.nodes.get(source), "LIVE 2", 14));
            }
            cluster.replace(target, null); cluster.nodes.get(target).started(); long sequence = 12L + kills; cluster.await(target, sequence, snapshot); cluster.cold(target, sequence);
            var active = cluster.active(target); assertNotEquals(before.generation(), active.generation()); assertEquals(snapshot, active.index()); assertEquals(12, active.sequence());
            if (expectedGeneration != null) { assertEquals(expectedGeneration, active.generation()); }
            assertFalse(Files.exists(directory.resolve("p" + target + "/business/INSTALLING")));
            source = cluster.leader(-1); assertEquals("TRANSFER true", cluster.nodes.get(source).request("TRANSFER p" + target)); cluster.awaitLeader(target);
            String data = cluster.nodes.get(target).request("READ"); assertEquals("DATA " + sequence + " " + expectedHash(10, kills), data);
            for (var receipt : receipts) { assertEquals("LOOKUP COMMITTED " + receipt.sequence() + " " + receipt.digest(), cluster.nodes.get(target).request("LOOKUP " + receipt.id())); }
            var continued = commit(cluster.nodes.get(target), "LIVE " + (kills + 1), sequence + 1); receipts.add(continued);
            assertEquals("DATA " + (sequence + 1) + " " + expectedHash(10, kills + 1), cluster.nodes.get(target).request("READ"));
            cluster.history.record(target, cluster.nodes.get(target).process.pid(), "VERIFIED", "KILLS=" + kills + " SNAPSHOT=" + snapshot + " SEQUENCE=" + (sequence + 1) + " RECEIPTS=" + receipts.size());
            Files.writeString(directory.resolve("case.json"), "{\"passed\":true,\"boundary\":\"" + boundary + "\",\"group\":\"" + cluster.group + "\",\"target\":" + target + ",\"kills\":" + kills
                    + ",\"snapshotIndex\":" + snapshot + ",\"snapshotSequence\":12,\"finalSequence\":" + (sequence + 1) + ",\"confirmedReceipts\":" + receipts.size()
                    + ",\"generation\":\"" + active.generation() + "\",\"expectedDigest\":\"" + expectedHash(10, kills + 1) + "\"}\n");
        }
    }

    /** 源成功回复必须携带完整凭据，INTENT 或错误均不进入确认前缀。 */
    private static Receipt commit(Child child, String command, long sequence) throws Exception {
        String reply = child.request(command); assertTrue(reply.startsWith("COMMITTED "), reply); String[] parts = reply.split(" ");
        assertEquals(sequence, Long.parseLong(parts[2])); UUID id = UUID.fromString(parts[1]); assertTrue(parts[3].matches("[a-f0-9]{64}")); return new Receipt(id, sequence, parts[3]);
    }

    /** 父进程独立构造预期内容，不调用子进程的摘要编码实现。 */
    private static String expectedHash(int blob, int live) throws Exception {
        var hash = java.security.MessageDigest.getInstance("SHA-256");
        for (String shard : List.of("a", "b")) {
            for (int i = 0; i < 32; i++) { expectedEntry(hash, shard + "/cold" + i, i == 0 ? null : (shard + ":" + i).getBytes(StandardCharsets.UTF_8)); }
            byte[] value = new byte[480 * 1024]; Arrays.fill(value, (byte) (blob + (shard.equals("a") ? 0 : 100))); expectedEntry(hash, shard + "/blob", value);
            expectedEntry(hash, shard + "/live", Integer.toString(shard.equals("a") ? live : -live).getBytes(StandardCharsets.UTF_8));
        }
        return java.util.HexFormat.of().formatHex(hash.digest());
    }

    /** 用固定大端长度编码边界，空值与空数组不同。 */
    private static void expectedEntry(java.security.MessageDigest hash, String key, byte[] value) {
        byte[] name = key.getBytes(StandardCharsets.UTF_8); hash.update(java.nio.ByteBuffer.allocate(4).putInt(name.length).array()); hash.update(name);
        hash.update(java.nio.ByteBuffer.allocate(4).putInt(value == null ? -1 : value.length).array()); if (value != null) { hash.update(value); }
    }

    /** 确认记录只来自父进程实际收到的成功回复。 */
    private record Receipt(UUID id, long sequence, String digest) { }

    /** 父进程单调时钟统一记录所有进程的外部观察与注入顺序。 */
    private static final class History {
        final Path path;
        /** 只保存测试数据、身份和摘要，不记录环境凭据。 */
        History(Path path) { this.path = path; }
        /** 每条事件同步落到外部历史，收到回复后才允许父进程继续操作。 */
        synchronized void record(int node, long pid, String kind, String text) throws IOException {
            Files.writeString(path, System.nanoTime() + "\t" + node + "\t" + pid + "\t" + kind + "\t" + text + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    /** 有界 stdout 队列分离启动、安装事件与命令回复，恢复阻塞时不要求伪造 STARTED。 */
    private static final class Child implements AutoCloseable {
        final Process process; final int index; final History history; final BufferedWriter commands; final BlockingQueue<String> events = new ArrayBlockingQueue<>(16); final BlockingQueue<String> replies = new ArrayBlockingQueue<>(64); final Thread reader;

        /** 所有进程句柄由父测试直接创建并保留，终止不依赖模糊名字或端口猜测。 */
        Child(Process process, int index, History history) throws Exception {
            this.process = process; this.index = index; this.history = history; commands = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8)); history.record(index, process.pid(), "SPAWN", "OWNED_JVM");
            reader = Thread.ofPlatform().daemon().name("mysql-probe-reader-" + process.pid()).start(() -> {
                try (var input = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line; (line = input.readLine()) != null;) {
                        if (!line.startsWith("AZIR ")) { continue; } String value = line.substring(5); history.record(index, process.pid(), "RECV", value);
                        if (value.startsWith("BOOT ") || value.startsWith("STARTED ") || value.startsWith("BOUNDARY ")) { events.put(value); } else { replies.put(value); }
                    }
                } catch (Exception failure) { replies.offer("READER_ENDED"); }
                finally { events.offer("TERMINAL"); }
            });
            String boot = events.poll(30, TimeUnit.SECONDS); assertEquals("BOOT " + process.pid(), boot);
        }

        /** 只有明确等待正常服务启动的进程才要求 STARTED，安装恢复故障可以在此之前发生。 */
        void started() throws Exception { String event = events.poll(90, TimeUnit.SECONDS); assertEquals("STARTED " + process.pid(), event); }

        /** 等待实际回调，不用固定睡眠推断安装到了哪个边界。 */
        void boundary(String expected) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) {
                String event = events.poll(Math.max(1, until - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (event != null && event.startsWith("STARTED ")) { continue; }
                assertEquals("BOUNDARY " + expected + " " + process.pid(), event); assertTrue(process.isAlive()); return;
            }
            throw new AssertionError("boundary observation expired; alive=" + process.isAlive());
        }

        /** 同一进程串行命令，等待到期只报告观察失败，不会自行重启。 */
        synchronized String request(String command) throws Exception {
            if (!process.isAlive()) { throw new AssertionError("owned process already terminal"); }
            history.record(index, process.pid(), "SEND", command); commands.write(command); commands.newLine(); commands.flush(); long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                String reply = replies.poll(Math.max(1, until - System.nanoTime()), TimeUnit.NANOSECONDS); if (reply != null && reply.startsWith("INTENT ")) { continue; }
                if (reply != null) { return reply; } break;
            }
            throw new AssertionError("command observation expired; alive=" + process.isAlive());
        }

        /** 只在真实边界后强杀并确认非零终态，才允许测试启动替代进程。 */
        void kill(String boundary) throws Exception {
            history.record(index, process.pid(), "KILL", boundary); process.destroyForcibly(); assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertNotEquals(0, process.exitValue()); history.record(index, process.pid(), "EXIT", Integer.toString(process.exitValue()));
        }

        /** 清理仅作用于本测试持有的进程，正常停止失败的强制清理与故障注入分开记录。 */
        @Override public void close() throws Exception {
            if (process.isAlive()) {
                try { history.record(index, process.pid(), "SEND", "STOP"); commands.write("STOP"); commands.newLine(); commands.flush(); } catch (IOException ignored) { }
                if (!process.waitFor(20, TimeUnit.SECONDS)) { history.record(index, process.pid(), "CLEANUP_KILL", "OWNED_PROCESS"); process.destroyForcibly(); assertTrue(process.waitFor(10, TimeUnit.SECONDS)); }
            }
            try { commands.close(); } catch (IOException ignored) { if (process.isAlive()) { throw ignored; } } reader.join(2000);
        }
    }

    /** 每个场景三个实际 JVM、三个独立控制库及持久目录，故障只替换明确目标。 */
    private static final class Cluster implements AutoCloseable {
        final Path root; final String boundary; final UUID group = UUID.randomUUID(); final List<Integer> ports = new ArrayList<>(); final List<String> schemas = new ArrayList<>(); final List<Child> nodes = new ArrayList<>(); final History history;

        /** 三份数据库仅在本轮拥有的容器中分配场景私有控制库。 */
        Cluster(Path root, String boundary) throws Exception {
            this.root = root; this.boundary = boundary; history = new History(root.resolve("events.tsv")); var held = new ArrayList<java.net.ServerSocket>();
            try { for (int i = 0; i < 3; i++) { var socket = MysqlGenerationSupport.reservePeerPort(); held.add(socket); ports.add(socket.getLocalPort()); } } finally { for (var socket : held) { socket.close(); } }
            try { for (int i = 0; i < 3; i++) { String schema = "c_" + UUID.randomUUID().toString().replace("-", ""); MysqlGenerationSupport.createControl(DATABASES.get(i), schema); schemas.add(schema); nodes.add(start(i, null)); nodes.get(i).started(); } }
            catch (Throwable failure) { close(); throw failure; }
        }

        /** 子 JVM 最大堆 192 MiB、一个处理器，凭据经环境传入且不会写入历史。 */
        Child start(int index, String fault) throws Exception {
            Path directory = root.resolve("p" + index); Files.createDirectories(directory); String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx192m", "-XX:ActiveProcessorCount=1", "-cp", classpath, RatisMysqlProcessProbe.class.getName(), directory.toString(), "p" + index, group.toString())); for (int port : ports) { args.add(Integer.toString(port)); }
            var builder = new ProcessBuilder(args).redirectError(directory.resolve("stderr-" + System.nanoTime() + ".log").toFile());
            builder.environment().put("AZIR_WAL_PROBE_JDBC_URL", DATABASES.get(index).getJdbcUrl()); builder.environment().put("AZIR_WAL_PROBE_CONTROL_SCHEMA", schemas.get(index)); builder.environment().put("AZIR_WAL_PROBE_PASSWORD", DATABASES.get(index).getPassword());
            if (fault != null) { builder.environment().put("AZIR_WAL_PROBE_FAULT", fault); } else { builder.environment().remove("AZIR_WAL_PROBE_FAULT"); }
            Process process = builder.start(); try { return new Child(process, index, history); } catch (Throwable failure) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); throw failure; }
        }

        /** 旧进程必须确认为终态或显式正常关闭，替换不由观察超时触发。 */
        void replace(int index, String fault) throws Exception { var old = nodes.get(index); old.close(); nodes.set(index, start(index, fault)); }

        /** 当前多数派可选择新的健康领导者，避免把旧角色缓存当作继续服务的证明。 */
        int leader(int excluded) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (System.nanoTime() < until) {
                for (int i = 0; i < nodes.size(); i++) { var node = nodes.get(i); if (i == excluded || !node.process.isAlive()) { continue; }
                    String status = node.request("STATUS"); if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && node.request("PING").startsWith("PING ")) { return i; }
                } TimeUnit.MILLISECONDS.sleep(30);
            }
            throw new AssertionError("no healthy process with majority barrier");
        }

        /** 核对实际冷数据水位和截断结果，不把普通内存查询当作冷恢复证明。 */
        void cold(int index, long sequence) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < until) { String reply = nodes.get(index).request("COLD " + sequence); if (reply.equals("COLD " + sequence + " 0 100")) { return; } TimeUnit.MILLISECONDS.sleep(30); }
            throw new AssertionError("owned MySQL process did not reach cold checkpoint");
        }

        /** 恢复到完整快照且追上后续日志后才能切为主，期间失败不会触发重启。 */
        void await(int index, long sequence, long snapshot) throws Exception {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (System.nanoTime() < until) { String reply = nodes.get(index).request("STATUS"); if (reply.startsWith("STATUS ")) { String[] fields = reply.split(" "); if (fields[2].equals(Long.toString(sequence)) && fields[3].equals("true") && Long.parseLong(fields[4]) >= snapshot) { return; } } TimeUnit.MILLISECONDS.sleep(30); }
            throw new AssertionError("recovered process did not finish installation and catch up");
        }

        /** 切主需要真实多数派读屏障，不能只检查管理回复。 */
        void awaitLeader(int index) throws Exception { long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30); while (System.nanoTime() < until) { String status = nodes.get(index).request("STATUS"); if (status.startsWith("STATUS ") && Long.parseLong(status.split(" ")[1]) > 0 && nodes.get(index).request("PING").startsWith("PING ")) { return; } TimeUnit.MILLISECONDS.sleep(30); } throw new AssertionError("transfer target did not become serving leader"); }

        /** 读取真实活动指针。 */
        RatisGenerationRecord active(int index) throws IOException { return RatisGenerationRecord.read(root.resolve("p" + index + "/business/ACTIVE")); }

        /** 在强杀前保留指针和操作的独立小文件证据，恢复后核对是否使用原代次。 */
        UUID capture(int index, String label) throws Exception {
            Path business = root.resolve("p" + index + "/business"); var active = active(index); Files.copy(business.resolve("ACTIVE"), root.resolve(label + "-active.record"));
            UUID generation = active.index() >= 0 ? active.generation() : null;
            if (Files.exists(business.resolve("INSTALLING"))) { var pending = RatisGenerationRecord.read(business.resolve("INSTALLING")); generation = pending.generation(); Files.copy(business.resolve("INSTALLING"), root.resolve(label + "-installing.record")); }
            history.record(index, nodes.get(index).process.pid(), "CHECK", "GENERATION " + (generation == null ? "UNPUBLISHED" : generation)); return generation;
        }

        /** MID_RESTORE_BATCH 必须对应真实两行已提交、前缀尚未发布的数据库状态。 */
        void partial(int index) throws Exception {
            var pending = RatisGenerationRecord.read(root.resolve("p" + index + "/business/INSTALLING"));
            try (var control = MysqlGenerationSupport.control(DATABASES.get(index), schemas.get(index))) {
                var factory = new MysqlWalReplicaBackendFactory(Map.of("a", control), MysqlWalReplicaBackendFactory.Options.defaults()); String schema = factory.databaseName(pending.identity(), pending.generation(), "a");
                try (var connection = control.getConnection(); var query = connection.createStatement(); var row = query.executeQuery("select restoring,applied_seq,(select count(*) from `" + schema + "`.azir_wal_version) from `" + schema + "`.azir_wal_binding where id=1")) {
                    assertTrue(row.next()); assertTrue(row.getBoolean(1)); assertEquals(0, row.getLong(2)); assertEquals(2, row.getInt(3));
                    history.record(index, nodes.get(index).process.pid(), "CHECK", "PARTIAL_RESTORE 2 1 0");
                }
            }
        }

        /** 关闭全部自有进程后归档脱敏历史、日志和小型元数据，不保存任何连接环境。 */
        @Override public void close() throws Exception {
            Throwable failure = null;
            try { for (var node : nodes) { try { node.close(); } catch (Throwable close) { if (failure == null) { failure = close; } else { failure.addSuppressed(close); } } } }
            finally {
                String configured = System.getProperty("azir.wal.ha.mysqlProcessEvidenceDir");
                if (configured != null) {
                    Path target = Path.of(configured).toAbsolutePath().resolve(boundary + "-" + group); Files.createDirectories(target);
                    try (var paths = Files.walk(root)) { for (Path path : paths.filter(Files::isRegularFile).toList()) {
                        String name = path.getFileName().toString(); if (!name.endsWith(".log") && !name.endsWith(".record") && !name.equals("events.tsv") && !name.equals("case.json")) { continue; }
                        Path output = target.resolve(root.relativize(path)); Files.createDirectories(output.getParent()); Files.copy(path, output);
                    } }
                }
            }
            if (failure instanceof Exception exception) { throw exception; } if (failure instanceof Error error) { throw error; }
        }
    }
}
