/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.testcontainers.lifecycle.Startables;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Manual low load long and steady probe: 3rd & 3rd reading, global fixed rate, restricted database and host pressure retreat.
 */
public final class WalCheckpointSteadyProbe {

    /**
     * Probe does not allow for exemplification.
     */
    private WalCheckpointSteadyProbe() {
    }

    /**
     * Use a stand-alone directory to execute the number of active seconds given. Short saves only verify collection code without changing the production default.
     *
     * @param args args
     * @throws AssertionError if the operation cannot complete
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path output = Path.of(args[0]);
        Files.createDirectory(output);
        long seconds = args.length > 1 ? Long.parseLong(args[1]) : 120;
        int keySlots = args.length > 2 ? Integer.parseInt(args[2]) : 8;
        long cacheBytes = args.length > 3 ? Long.parseLong(args[3]) : 4096;
        boolean managedMode = args.length > 4 && args[4].equals("managed");
        if (keySlots < 1 || keySlots > 4096 || cacheBytes < 0 || cacheBytes > 8L * 1024 * 1024) {
            throw new IllegalArgumentException("invalid bounded-cache probe workload");
        }
        if (seconds < 1 || seconds > 7 * 86400) {
            throw new IllegalArgumentException("duration outside probe budget");
        }
        long target = TimeUnit.SECONDS.toNanos(seconds);
        long maximumWall = target * 2 + TimeUnit.MINUTES.toNanos(5);
        var paused = new AtomicBoolean();
        var running = new AtomicBoolean(true);
        var fault = new AtomicReference<Throwable>();
        var activeNanos = new AtomicLong();
        var cycles = new AtomicLong();
        var backupCycles = new AtomicLong();
        var backpressure = new AtomicLong();
        var completed = new AtomicReference<String>();
        var a = WalCheckpointMysqlSupport.container();
        var b = WalCheckpointMysqlSupport.container();
        try {
            var preflight = new HostPressure();
            preflight.sample();
            TimeUnit.MILLISECONDS.sleep(200);
            double initialCpu = preflight.sample();
            if (preflight.availableMiB < 10_240 || initialCpu > 85 || preflight.ioPressure > 5 || preflight.memoryPressure > .1 || preflight.swapping) {
                throw new IllegalStateException("DEFERRED_HOST_PRESSURE: insufficient headroom before test containers");
            }
            Startables.deepStart(Stream.of(a, b)).join();
            var firstLimits = a.getDockerClient().inspectContainerCmd(a.getContainerId()).exec().getHostConfig();
            var secondLimits = b.getDockerClient().inspectContainerCmd(b.getContainerId()).exec().getHostConfig();
            for (var limits : new com.github.dockerjava.api.model.HostConfig[]{firstLimits, secondLimits}) {
                Assertions.assertEquals(512L * 1024 * 1024, limits.getMemory());
                Assertions.assertEquals(500_000_000L, limits.getNanoCPUs());
            }
            Files.writeString(output.resolve("resource-limits.json"), "{\"containerIds\":[\"" + a.getContainerId() + "\",\"" + b.getContainerId()
                    + "\"],\"mysqlMemoryBytesEach\":536870912,\"mysqlCpuCoresEach\":0.5,\"jvmMaximumHeapBytes\":"
                    + Runtime.getRuntime().maxMemory() + ",\"jvmAvailableProcessors\":" + Runtime.getRuntime().availableProcessors() + "}\n");
            try (
                    var left = WalCheckpointMysqlSupport.pool(a); var right = WalCheckpointMysqlSupport.pool(b);
                    var model = new WalSteadyModel(output.resolve("history.tsv.gz")); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(left), "b", new MysqlWalMaterializedStore(right));
                var options = new WalOptions(32, 16, 64 * 1024, 32, 16, 1, 8L * 1024 * 1024, 16L * 1024 * 1024, 60_000, 64, 50)
                        .withHotCacheBytes(cacheBytes);
                var policy = new WalRetentionOptions(5000, 3_600_000, 4L * 1024 * 1024, 8L * 1024 * 1024, 10_000);
                var managed = managedMode ? WalManagedRuntime.open(output.resolve("wal"), stores, options, policy, WalMaintenanceOptions.defaults()) : null;
                var runtime = managed == null ? WalMvccRuntime.open(output.resolve("wal"), stores, options, policy) : managed.runtime();
                try (
                        AutoCloseable owner = managed == null ? runtime : managed;
                        var samples = Files.newBufferedWriter(output.resolve("samples.jsonl"))) {
                    long started = System.nanoTime();
                    var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                    for (int worker = 0; worker < 3; worker++) {
                        int id = worker;
                        futures.add(tasks.submit(() -> {
                            var pace = new Pace(30_000_000, id * 10_000_000);
                            long ordinal = 0;
                            try {
                                while (running.get()) {
                                    if (paused.get()) {
                                        pause();
                                        continue;
                                    }
                                    pace.await();
                                    if (!running.get() || paused.get()) {
                                        continue;
                                    }
                                    String key = "w" + id + ":k" + ordinal % keySlots;
                                    String value = ordinal % 11 == 0 ? null : id + ":" + ordinal;
                                    WalTransaction tx = null;
                                    try {
                                        tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
                                        for (String shard : new String[]{"a", "b"}) {
                                            if (value == null) {
                                                runtime.delete(tx, shard, key);
                                            } else {
                                                runtime.put(tx, shard, key, payload(value));
                                            }
                                        }
                                        model.committed(runtime.commit(tx), tx.startSequence(), key, value);
                                        ordinal++;
                                    } catch (final WalMvccException rejected) {
                                        if (!managedMode || rejected.errorCode() != com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED) {
                                            throw rejected;
                                        }
                                        if (tx != null) {
                                            runtime.rollback(tx);
                                        }
                                        backpressure.incrementAndGet();
                                    }
                                }
                            } catch (final Throwable failure) {
                                fault.compareAndSet(null, failure);
                                running.set(false);
                            }
                        }));
                    }
                    for (int worker = 0; worker < 3; worker++) {
                        int id = worker;
                        futures.add(tasks.submit(() -> {
                            var pace = new Pace(10_000_000, id * 3_000_000);
                            var random = new Random(20261010L + id);
                            long ordinal = 0;
                            long statement = 0;
                            try {
                                while (running.get()) {
                                    if (paused.get()) {
                                        pause();
                                        continue;
                                    }
                                    String token = "r" + id + ":" + ordinal;
                                    long lower = model.pin(token);
                                    var isolation = ordinal++ % 2 == 0 ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED;
                                    WalTransaction tx = null;
                                    try {
                                        tx = runtime.begin(isolation);
                                        assertTrue(tx.startSequence() >= lower);
                                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                                        while (running.get() && !paused.get() && System.nanoTime() < end) {
                                            pace.await();
                                            long minimum = model.known();
                                            var snapshot = runtime.beginReadStatement(tx);
                                            if (isolation == IsolationLevel.REPEATABLE_READ) {
                                                Assertions.assertEquals(tx.startSequence(), snapshot.sequence());
                                            } else {
                                                assertTrue(snapshot.sequence() >= minimum);
                                            }
                                            String key = "w" + random.nextInt(3) + ":k" + random.nextInt(keySlots);
                                            var x = runtime.get(tx, "a", key, snapshot);
                                            var y = runtime.get(tx, "b", key, snapshot);
                                            Assertions.assertEquals(x.isPresent(), y.isPresent());
                                            if (x.isPresent()) {
                                                assertArrayEquals(x.orElseThrow(), y.orElseThrow());
                                            }
                                            model.observed(token, snapshot.sequence(), key, x.map(WalCheckpointSteadyProbe::decode).orElse(null),
                                                    y.map(WalCheckpointSteadyProbe::decode).orElse(null), isolation, tx.startSequence(), minimum, statement++ % 10 == 0);
                                        }
                                    } catch (final WalMvccException rejected) {
                                        if (!managedMode || rejected.errorCode() != com.flyingzc.azir.common.ErrorCode.RESOURCE_EXHAUSTED) {
                                            throw rejected;
                                        }
                                        backpressure.incrementAndGet();
                                    } finally {
                                        if (tx != null) {
                                            runtime.rollback(tx);
                                        }
                                        model.unpin(token);
                                    }
                                }
                            } catch (final Throwable failure) {
                                fault.compareAndSet(null, failure);
                                running.set(false);
                            }
                        }));
                    }
                    var host = new HostPressure();
                    long previous = System.nanoTime();
                    long ticks = 0;
                    while (running.get() && activeNanos.get() < target) {
                        TimeUnit.SECONDS.sleep(1);
                        long now = System.nanoTime();
                        if (!paused.get()) {
                            activeNanos.addAndGet(now - previous);
                        }
                        previous = now;
                        double busy = host.sample();
                        boolean pressure = busy > 85 || host.availableMiB < 8192
                                || host.ioPressure > 5 || host.memoryPressure > .1 || host.swapping;
                        paused.set(pressure);
                        if (now - started > maximumWall) {
                            throw new IllegalStateException("DEFERRED_HOST_PRESSURE: active-time target not reached");
                        }
                        if (!pressure) {
                            if (!managedMode) {
                                runtime.collectGarbage(64);
                            }
                            if (runtime.checkpoint().created()) {
                                cycles.incrementAndGet();
                            }
                            if (managedMode && ticks % 20 == 4) {
                                verifyBackup(output, backupCycles.incrementAndGet(), runtime, a, b, options, model, keySlots);
                            }
                        }
                        model.flush();
                        var stats = runtime.stats();
                        assertTrue(stats.hotCacheBytes() <= cacheBytes);
                        if (Files.size(output.resolve("history.tsv.gz")) > 2L * 1024 * 1024 * 1024) {
                            throw new IllegalStateException("DEFERRED_TRACE_BUDGET: stop this probe before disk evidence exceeds budget");
                        }
                        long heap = java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
                        samples.write("{\"activeSeconds\":" + TimeUnit.NANOSECONDS.toSeconds(activeNanos.get()) + ",\"paused\":" + pressure
                                + ",\"hostCpuBusy\":" + busy + ",\"hostAvailableMiB\":" + host.availableMiB + ",\"heapBytes\":" + heap
                                + ",\"hostIoPressure\":" + host.ioPressure + ",\"hostMemoryPressure\":" + host.memoryPressure + ",\"swapping\":" + host.swapping
                                + ",\"published\":" + stats.publishedSequence() + ",\"checkpoint\":" + stats.checkpointSequence()
                                + ",\"pendingReceipts\":" + stats.pendingReceipts() + ",\"receiptArchives\":" + stats.receiptArchives()
                                + ",\"retainedBytes\":" + stats.retainedBytes() + ",\"walBytes\":" + stats.walBytes()
                                + ",\"hotCacheBytes\":" + stats.hotCacheBytes() + ",\"hotCacheKeys\":" + stats.hotCacheKeys()
                                + ",\"cacheHits\":" + stats.cacheHits() + ",\"cacheMisses\":" + stats.cacheMisses()
                                + ",\"cacheEvictions\":" + stats.cacheEvictions() + ",\"conflictStoreLookups\":" + stats.conflictStoreLookups()
                                + ",\"materializationLag\":" + stats.materializationLag() + ",\"archiveBytes\":" + stats.receiptArchiveBytes()
                                + ",\"managed\":" + managedMode + ",\"backupCycles\":" + backupCycles.get()
                                + ",\"maintenanceCycles\":" + (managed == null ? 0 : managed.health().maintenanceCycles()) + "}\n");
                        samples.flush();
                        if (++ticks % 10 == 0) {
                            System.out.println("cycles=" + cycles.get() + " successfulWrites=" + model.known() + " paused=" + pressure);
                        }
                    }
                    running.set(false);
                    for (var future : futures) {
                        future.get(30, TimeUnit.SECONDS);
                    }
                    if (fault.get() != null) {
                        throw new AssertionError("low-load history failed", fault.get());
                    }
                    while (runtime.stats().materializedSequences().values().stream().anyMatch(seq -> seq != runtime.stats().publishedSequence())) {
                        runtime.flush();
                    }
                    runtime.collectGarbage(4096);
                    runtime.checkpoint();
                    cycles.incrementAndGet();
                    Assertions.assertEquals(model.known(), runtime.stats().publishedSequence());
                    for (var entry : model.finalValues().entrySet()) {
                        String expected = entry.getValue().deleted() ? null : new String(entry.getValue().value(), StandardCharsets.UTF_8);
                        for (var store : stores.values()) {
                            var persisted = store.get(entry.getKey(), Long.MAX_VALUE);
                            if (entry.getValue().deleted() && persisted.isEmpty()) {
                                continue;
                            }
                            var version = persisted.orElseThrow();
                            Assertions.assertEquals(entry.getValue().commitSequence(), version.commitSequence());
                            Assertions.assertEquals(expected, version.deleted() ? null : decode(version.value()));
                        }
                    }
                    if (seconds >= 120) {
                        assertTrue(cycles.get() >= 100);
                    }
                    long disk;
                    try (var files = Files.walk(output.resolve("wal"))) {
                        disk = files.filter(Files::isRegularFile).mapToLong(path -> {
                            try {
                                return Files.size(path);
                            } catch (final Exception failure) {
                                throw new IllegalStateException(failure);
                            }
                        }).sum();
                    }
                    String result = "{\"status\":\"passed\",\"businessConcurrency\":6,\"writers\":3,\"readers\":3,\"writeRateLimit\":100,\"readStatementRateLimit\":300,"
                            + "\"activeSeconds\":" + TimeUnit.NANOSECONDS.toSeconds(activeNanos.get()) + ",\"successfulWrites\":" + model.known()
                            + ",\"pointStatementsChecked\":" + model.observations() + ",\"sampledStatements\":" + model.samples()
                            + ",\"checkpointCycles\":" + cycles.get() + ",\"finalWalDirectoryBytes\":" + disk
                            + ",\"logicalKeys\":" + (3 * keySlots) + ",\"hotCacheBudgetBytes\":" + cacheBytes
                            + ",\"cacheEvictions\":" + runtime.stats().cacheEvictions() + ",\"coldStoreReads\":" + runtime.stats().cacheMisses()
                            + ",\"managed\":" + managedMode + ",\"backupCycles\":" + backupCycles.get() + ",\"expectedBackpressure\":" + backpressure.get()
                            + ",\"recoveryVerified\":true,\"mysqlPhysicalFinalVerified\":true,\"productionRetentionChanged\":false,\"testReceiptRetentionMillis\":5000,\"isCapacityBenchmark\":false}\n";
                    completed.set(result);
                } finally {
                    running.set(false);
                }
                try (var restored = WalMvccRuntime.open(output.resolve("wal"), stores, options, policy)) {
                    Assertions.assertEquals(model.known(), restored.stats().publishedSequence());
                    Assertions.assertEquals(model.lastReceipt(), restored.findCommitted(model.lastReceipt().transactionId()).orElseThrow());
                    if (seconds >= 120) {
                        Assertions.assertEquals(WalCommitLookup.Status.UNKNOWN_OR_EXPIRED, restored.lookupCommit(model.firstReceipt().transactionId()).status());
                    }
                    var tx = restored.begin(IsolationLevel.REPEATABLE_READ);
                    var snapshot = restored.beginReadStatement(tx);
                    for (var entry : model.finalValues().entrySet()) {
                        String expected = entry.getValue().deleted() ? null : new String(entry.getValue().value(), StandardCharsets.UTF_8);
                        for (String shard : stores.keySet()) {
                            Assertions.assertEquals(expected, restored.get(tx, shard, entry.getKey(), snapshot).map(WalCheckpointSteadyProbe::decode).orElse(null));
                        }
                    }
                    restored.rollback(tx);
                }
            }
        } catch (final Throwable failure) {
            running.set(false);
            Files.writeString(output.resolve("failure.txt"), failure.toString() + "\n");
            throw failure;
        } finally {
            try {
                a.stop();
            } finally {
                b.stop();
            }
        }
        // The engine, the evidence stream, the implementer and database were all successfully shut down before the official pass status was preserved.
        Files.writeString(output.resolve("verification.json"), completed.get());
        System.out.println(completed.get());
    }

    /**
     * A separate database of the recovery fixed boundary while the source continues to write, then the whole recovery snapshot is checked by the source history model.
     *
     * @param output output
     * @param ordinal ordinal
     * @param runtime runtime
     * @param a a
     * @param b b
     * @param options options
     * @param model model
     * @param keySlots key slots
     * @throws Exception if the operation cannot complete
     */
    private static void verifyBackup(final Path output, final long ordinal, final WalMvccRuntime runtime, final org.testcontainers.containers.MySQLContainer<?> a,
                                     final org.testcontainers.containers.MySQLContainer<?> b, final WalOptions options, final WalSteadyModel model, final int keySlots) throws Exception {
        String token = "backup:" + ordinal;
        long lower = model.pin(token);
        try {
            Path archive = output.resolve("backup-" + ordinal);
            var limits = new WalBackupOptions(32L * 1024 * 1024, 128, 1024 * 1024, 30_000);
            var info = runtime.backup(archive, limits);
            assertTrue(info.sequence() >= lower);
            for (var container : java.util.List.of(a, b)) {
                var result = container.execInContainer("mysql", "-uroot", "-p" + container.getPassword(), "-e",
                        "create database if not exists wal_probe_restore; grant all on wal_probe_restore.* to 'wal_test'@'%'");
                Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            }
            try (var left = restorePool(a); var right = restorePool(b)) {
                for (var pool : java.util.List.of(left, right)) {
                    try (var connection = pool.getConnection(); var query = connection.createStatement()) {
                        query.executeUpdate("drop table if exists azir_wal_version");
                        query.executeUpdate("drop table if exists azir_wal_binding");
                    }
                }
                var stores = Map.<String, WalMaterializedStore>of("a", new MysqlWalMaterializedStore(left), "b", new MysqlWalMaterializedStore(right));
                Path target = output.resolve("restored-" + ordinal);
                WalBackup.restore(archive, target, stores, options, limits);
                try (var restored = WalMvccRuntime.open(target, stores, options.withHotCacheBytes(0))) {
                    var reader = restored.begin(IsolationLevel.REPEATABLE_READ);
                    var snapshot = restored.beginReadStatement(reader);
                    Assertions.assertEquals(info.sequence(), snapshot.sequence());
                    for (int writer = 0; writer < 3; writer++) {
                        for (int k = 0; k < keySlots; k++) {
                            String key = "w" + writer + ":k" + k;
                            var x = restored.get(reader, "a", key, snapshot);
                            var y = restored.get(reader, "b", key, snapshot);
                            model.observed(token, snapshot.sequence(), key, x.map(WalCheckpointSteadyProbe::decode).orElse(null),
                                    y.map(WalCheckpointSteadyProbe::decode).orElse(null), IsolationLevel.REPEATABLE_READ, reader.startSequence(), lower, true);
                        }
                    }
                    restored.rollback(reader);
                }
            }
            Files.writeString(output.resolve("backup-cuts.tsv"), ordinal + "\t" + info.sequence() + "\t" + info.bytes() + "\n",
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } finally {
            model.unpin(token);
        }
    }

    /**
     * Recovery verify only uses another special schema for this probe database instance, and the source table cannot be deleted by error.
     *
     * @param container container
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static com.zaxxer.hikari.HikariDataSource restorePool(final org.testcontainers.containers.MySQLContainer<?> container) {
        String source = container.getJdbcUrl();
        String target = source.replace("/wal_checkpoint", "/wal_probe_restore");
        if (target.equals(source)) {
            throw new IllegalArgumentException("restore probe database route mismatch");
        }
        var config = new com.zaxxer.hikari.HikariConfig();
        config.setJdbcUrl(target);
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(2);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(1000);
        return new com.zaxxer.hikari.HikariDataSource(config);
    }

    /**
     * Synthetic fixed-sized payload, reading also checks filling areas and cannot compare prefix to ignore data damage.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] payload(final String value) {
        byte[] text = value.getBytes(StandardCharsets.UTF_8);
        var result = ByteBuffer.allocate(512);
        result.putInt(text.length).put(text);
        return result.array();
    }

    /**
     * Complete validate payload length, body text and fill area, return the logical values used in the reference model.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String decode(final byte[] value) {
        Assertions.assertEquals(512, value.length);
        var in = ByteBuffer.wrap(value);
        int size = in.getInt();
        assertTrue(size > 0 && size < 64);
        byte[] text = new byte[size];
        in.get(text);
        while (in.hasRemaining()) {
            Assertions.assertEquals((byte) 0, in.get());
        }
        return new String(text, StandardCharsets.UTF_8);
    }

    /**
     * Only this probe business is suspended while the host is busy, without interruption of ongoing files and JDBC operations.
     */
    private static void pause() {
        java.util.concurrent.locks.LockSupport.parkNanos(100_000_000);
    }

    /**
     * Each client uses a fixed cycle and does not catch up with the backlog token, avoiding any sudden break after host recovery.
     */
    private static final class Pace {

        private final long interval;

        private long next;

        /**
         * Stagger three clients to start and keep global total speed bound.
         *
         * @param interval interval
         * @param offset offset
         */
        Pace(final long interval, final long offset) {
            this.interval = interval;
            next = System.nanoTime() + offset;
        }

        /**
         * Waits for the next working gap of this client.
         */
        void await() {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                java.util.concurrent.locks.LockSupport.parkNanos(wait);
            }
            next = Math.max(next + interval, System.nanoTime() + interval);
        }
    }

    /**
     * Read-only Aggregated CPUs and available memory do not read command line, environment or private data for other tasks.
     */
    private static final class HostPressure {

        private long total;

        private long idle;

        private long availableMiB;

        private long swapOut = -1;

        private double ioPressure;

        private double memoryPressure;

        private boolean swapping;

        /**
         * The host sampler only serves this low load verify.
         */
        HostPressure() {
        }

        /**
         * Take a sample every second and suspend this probe when the CPU pressure is high or there is insufficient available memory.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        double sample() throws Exception {
            String line = Files.readAllLines(Path.of("/proc/stat")).getFirst();
            String[] fields = line.trim().split("\\s+");
            long nextTotal = 0;
            for (int i = 1; i <= 8; i++) {
                nextTotal += Long.parseLong(fields[i]);
            }
            long nextIdle = Long.parseLong(fields[4]) + Long.parseLong(fields[5]);
            double busy = total == 0 || nextTotal == total ? 0 : 100.0 * (nextTotal - total - (nextIdle - idle)) / (nextTotal - total);
            total = nextTotal;
            idle = nextIdle;
            for (String memory : Files.readAllLines(Path.of("/proc/meminfo"))) {
                if (memory.startsWith("MemAvailable:")) {
                    availableMiB = Long.parseLong(memory.trim().split("\\s+")[1]) / 1024;
                }
            }
            ioPressure = pressure("io");
            memoryPressure = pressure("memory");
            for (String vm : Files.readAllLines(Path.of("/proc/vmstat"))) {
                if (vm.startsWith("pswpout ")) {
                    long next = Long.parseLong(vm.split("\\s+")[1]);
                    swapping = swapOut >= 0 && next > swapOut;
                    swapOut = next;
                }
            }
            return busy;
        }

        /**
         * Pressure data read-only system aggregates, without PSI platforms to retain CPU and memory thresholds.
         *
         * @param kind kind
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        private static double pressure(final String kind) throws Exception {
            Path path = Path.of("/proc/pressure", kind);
            if (!Files.exists(path)) {
                return 0;
            }
            String first = Files.readAllLines(path).getFirst();
            return Double.parseDouble(first.trim().split("\\s+")[1].split("=")[1]);
        }
    }
}
