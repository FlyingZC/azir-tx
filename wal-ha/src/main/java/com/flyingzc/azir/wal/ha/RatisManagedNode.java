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

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalBackupOptions;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMutation;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import java.io.IOException;

/**
 * Owns a replicated WAL node and its background maintenance and health lifecycle.
 */
public final class RatisManagedNode implements AutoCloseable {

    private final RatisWalNode node;

    private final Config config;

    private final ScheduledExecutorService observer;

    private volatile RatisTimedSample<Status> sampled;

    private volatile boolean closing, closed;

    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();

    /**
     * Finishing role, identity and durable maintenance arguments validate before creating backend space and starting Raft.
     *
     * @param config config
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisManagedNode open(final Config config) throws IOException {
        return open(config, stage -> {
        });
    }

    /**
     * The boundary test inside the package is fully and officially assembled, with empty hooks always used at open entrances.
     *
     * @param config config
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws StartupException if the operation cannot complete
     */
    static RatisManagedNode open(final Config config, final RatisWalState.Hook hook) throws IOException {
        Objects.requireNonNull(config);
        preflight(config);
        RatisWalNode node = null;
        try {
            var factory = new MysqlWalReplicaBackendFactory(config.controls(), config.namespaces());
            node = RatisWalNode.openAuthenticatedForTest(config.directory(), config.node(), config.group(), config.controls().keySet(), factory, config.transactions(), config.backups(),
                    config.security(), config.transport(), hook);
            node.enableHealthManagement(config.maintenance().health());
            node.enableVersionGc(config.maintenance().gc());
            node.enableSnapshotMaintenance(config.maintenance().snapshot());
            return new RatisManagedNode(node, config);
        } catch (final Throwable failure) {
            if (node != null) {
                try {
                    node.close();
                } catch (final Throwable cleanup) {
                    var pending = new StartupException(failure, node::close);
                    pending.addSuppressed(cleanup);
                    throw pending;
                }
            }
            if (failure instanceof RatisStartupException pending && pending.cleanupPending()) {
                throw new StartupException(failure, pending::retryCleanup);
            }
            if (failure instanceof IOException io) {
                throw io;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IOException("managed startup failed", failure);
        }
    }

    /**
     * The durable policy may not be quietly replaced by a caller missing or changing to a different default value.
     *
     * @param config config
     * @throws IOException if the operation cannot complete
     */
    private static void preflight(final Config config) throws IOException {
        if (Files.exists(config.directory(), LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(config.directory(), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("managed directory is not a private directory");
        }
        config.security().validate(config.group(), config.node());
        var id = new WalReplicaIdentity(config.group().getGroupId().getUuid(), config.group().getGroupId().getUuid(), config.node().toString());
        RatisTransportIdentity.verify(config.directory(), id, true);
        var health = RatisHealthRecord.load(config.directory(), id);
        var gc = RatisGcPolicy.load(config.directory(), id);
        var snapshot = RatisSnapshotPolicy.load(config.directory(), id);
        if (health != null && !health.options().equals(config.maintenance().health()) || gc != null && !gc.options().equals(config.maintenance().gc())
                || snapshot != null && !snapshot.options().equals(config.maintenance().snapshot())) {
            throw new IOException("managed settings differ from persistent maintenance policy");
        }
        if ((gc != null || snapshot != null) && health == null) {
            throw new IOException("managed maintenance policy dependency missing");
        }
    }

    /**
     * Only open can assemble the back end of production.
     *
     * @param node node
     * @param config config
     */
    RatisManagedNode(final RatisWalNode node, final Config config) {
        this.node = node;
        this.config = config;
        sampled = RatisTimedSample.initial(new Status(false, false, 0, null, null, node.healthStats(), node.gcStats(), node.snapshotStats(), 0, true, null));
        observer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-managed-status-" + config.node()).factory());
        observer.scheduleWithFixedDelay(this::sample, 0, config.maintenance().sampleMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Majority verify and statistical sampling is performed only on a stand-alone line, with failure to output only the type of dissensitization anomaly.
     */
    private void sample() {
        // Freshness includes the time-consuming nature of majority detection and indicator sampling, which cannot be recalculated after slow sampling.
        long began = System.nanoTime();
        try {
            if (closing) {
                return;
            }
            long epoch = 0;
            String error = null;
            if (node.ready() && node.leaderEpoch() > 0) {
                try {
                    epoch = node.probeManagedLeader(config.maintenance().probeMillis());
                } catch (final Exception failure) {
                    error = failure.getClass().getSimpleName();
                }
            }
            var local = node.managedSample();
            var observation = sampled;
            var previous = observation.value();
            // Generation Keeps the old sampling time while busy and cannot detect the old storage count with a new role.
            if (local == null) {
                sampled = observation.retaining(new Status(node.ready(), false, 0, previous.generation(), previous.storage(), node.healthStats(), node.gcStats(), node.snapshotStats(), 0,
                        previous.stale(), "StorageSampleBusy"));
                return;
            }
            sampled = RatisTimedSample.captured(new Status(node.ready(), epoch > 0 && epoch == node.leaderEpoch(), epoch, local.generation(), local.stats(), node.healthStats(), node.gcStats(),
                    node.snapshotStats(), 0, false, error), began);
        } catch (final Exception failure) {
            var observation = sampled;
            var previous = observation.value();
            sampled = observation.retaining(new Status(false, false, 0, previous.generation(), previous.storage(), node.healthStats(), node.gcStats(), node.snapshotStats(), 0, previous.stale(),
                    failure.getClass().getSimpleName()));
        }
    }

    /**
     * Access to caches and real-time role controls is not blocked, and expired samples cannot continue to claim writeability.
     *
     * @return the operation result
     */
    public Status status() {
        var observation = sampled;
        var value = observation.value();
        long age = observation.ageMillis(System.nanoTime());
        boolean stale = age > config.maintenance().staleMillis();
        return new Status(!closing && node.ready(), !closing && !stale && value.writableHint() && node.ready() && value.verifiedEpoch() == node.leaderEpoch(), value.verifiedEpoch(),
                value.generation(), value.storage(), value.health(), value.gc(), value.snapshot(), age, stale, value.error());
    }

    /**
     * Manual transaction with original kernel ownership and still real majority barrier.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    public RatisTransaction begin(final IsolationLevel isolation) {
        if (closing) {
            throw new IllegalStateException("managed node closing");
        }
        return node.beginManagedTransaction(isolation);
    }

    /**
     * Do not automatically rerun business callback, unknown commit with original UUID dissemination, resource shutdown is false rollback.
     *
     * @param <T> value type
     * @param isolation global transaction isolation level
     * @param work work
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    public <T> Execution<T> execute(final IsolationLevel isolation, final Work<T> work) throws Exception {
        Objects.requireNonNull(work);
        try (var transaction = begin(isolation)) {
            T result = work.run(transaction.operations());
            return new Execution<>(result, transaction.commit());
        }
    }

    /**
     * Only COMMITTED was confirmed that the UNKNOWN contract continued to be complied with as a result of missing or expired results.
     *
     * @param transaction transaction context
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    public WalCommitLookup lookupCommit(final UUID transaction) {
        if (closing) {
            throw new IllegalStateException("managed node closing");
        }
        return node.lookupCommit(transaction);
    }

    /**
     * Plan to save durable before handing the instrument over to the operator for execution; saving failed without modifying members.
     *
     * @param removed removed
     * @param address address
     * @param ticket ticket
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RatisMemberReplacement planReplacement(final RaftPeerId removed, final String address, final Path ticket, final long timeoutMillis) throws IOException {
        var plan = node.planReplacement(removed, address, timeoutMillis);
        RatisReplacementTickets.save(ticket, plan);
        return plan;
    }

    /**
     * Continue with validate and synchronized original, without unauthorized new identity additions.
     *
     * @param ticket ticket
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RatisWalNode.ReplacementProgress beginReplacement(final Path ticket, final long timeoutMillis) throws IOException {
        return node.beginReplacement(RatisReplacementTickets.load(ticket, config.group().getGroupId().getUuid()), timeoutMillis);
    }

    /**
     * The true homing and co-configuration of the same instrument commit remains the responsibility of the nodal agreement.
     *
     * @param ticket ticket
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RatisWalNode.ReplacementProgress completeReplacement(final Path ticket, final long timeoutMillis) throws IOException {
        return node.completeReplacement(RatisReplacementTickets.load(ticket, config.group().getGroupId().getUuid()), timeoutMillis);
    }

    /**
     * Samples are empty before closing nodes; failed retry, without closing the control pool borrowed by the caller.
     *
     * @throws IOException if the operation cannot complete
     */
    @Override
    public void close() throws IOException {
        closeGuard.lock();
        try {
            if (closed) {
                return;
            }
            closing = true;
            observer.shutdown();
            try {
                if (!observer.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new IOException("managed status worker did not drain; retry close");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("managed close interrupted", interrupted);
            }
            node.close();
            closed = true;
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Maintenance of budget and majority detection deadlines, which cannot continue to be declared written.
     *
     * @param health health
     * @param gc gc
     * @param snapshot snapshot
     * @param sampleMillis sample millis
     * @param staleMillis stale millis
     * @param probeMillis probe millis
     */
    public record Maintenance(RatisHealthOptions health, RatisGcOptions gc, RatisSnapshotOptions snapshot, long sampleMillis, long staleMillis, long probeMillis) {

        /**
         * Sampling is a fixed delay for single workers and detection does not cancel WAL through interruption.
         *
         * @param health health
         * @param gc gc
         * @param snapshot snapshot
         * @param sampleMillis sample millis
         * @param staleMillis stale millis
         * @param probeMillis probe millis
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Maintenance {
            Objects.requireNonNull(health);
            Objects.requireNonNull(gc);
            Objects.requireNonNull(snapshot);
            if (sampleMillis < 100 || sampleMillis > 60_000 || probeMillis < 1 || probeMillis > 5000 || staleMillis <= sampleMillis + probeMillis || staleMillis > 86_400_000) {
                throw new IllegalArgumentException("invalid managed maintenance observation budget");
            }
        }

        /**
         * Default sampling per second, majority detection one second, 15 seconds expired.
         *
         * @return the operation result
         */
        public static Maintenance defaults() {
            return new Maintenance(RatisHealthOptions.defaults(), RatisGcOptions.defaults(), RatisSnapshotOptions.defaults(), 1000, 15_000, 1000);
        }
    }

    /**
     * The caller clearly provides a directory, three vote configuration, control pool and TLS identity, which is always the caller.
     *
     * @param directory directory
     * @param node node
     * @param group group
     * @param controls controls
     * @param security security
     * @param transactions transactions
     * @param backups backups
     * @param namespaces namespaces
     * @param transport transport
     * @param maintenance maintenance
     */
    public record Config(Path directory, RaftPeerId node, RaftGroup group, Map<String, HikariDataSource> controls, RatisTlsSecurity security,
            WalOptions transactions, WalBackupOptions backups, MysqlWalReplicaBackendFactory.Options namespaces, RatisTransportLimits transport, Maintenance maintenance) {

        /**
         * Defensive replicating configuration, rejecting missing route and unsupported role layouts.
         *
         * @param directory directory
         * @param node node
         * @param group group
         * @param controls controls
         * @param security security
         * @param transactions transactions
         * @param backups backups
         * @param namespaces namespaces
         * @param transport transport
         * @param maintenance maintenance
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public Config {
            directory = Objects.requireNonNull(directory).toAbsolutePath().normalize();
            Objects.requireNonNull(node);
            Objects.requireNonNull(group);
            controls = Map.copyOf(controls);
            Objects.requireNonNull(security);
            Objects.requireNonNull(transactions);
            Objects.requireNonNull(backups);
            Objects.requireNonNull(namespaces);
            Objects.requireNonNull(transport);
            Objects.requireNonNull(maintenance);
            if (directory.getParent() == null || controls.isEmpty() || controls.size() > 1024 || group.getPeer(node) == null || group.getPeers().size() > 9
                    || group.getPeers().stream().filter(peer -> peer.getStartupRole() == RaftPeerRole.FOLLOWER).count() != 3
                    || group.getPeers().stream().anyMatch(peer -> peer.getStartupRole() != RaftPeerRole.FOLLOWER && peer.getStartupRole() != RaftPeerRole.LISTENER)) {
                throw new IllegalArgumentException("managed node requires three voters and explicit bounded MySQL routes");
            }
            var endpoints = new HashSet<String>();
            var identities = new HashSet<RaftPeerId>();
            for (var peer : group.getPeers()) {
                RatisMembershipView.validate(peer);
                if (!endpoints.add(peer.getAddress()) || !identities.add(peer.getId())) {
                    throw new IllegalArgumentException("duplicate managed peer identity or address");
                }
            }
            for (var shard : controls.keySet()) {
                new WalMutation(shard, "", null);
                if (shard.contains("\u0000")) {
                    throw new IllegalArgumentException("invalid managed shard");
                }
            }
        }

        /**
         * Frequent default budget, data sources and TLS are not default guesses.
         *
         * @param directory directory
         * @param node node
         * @param group group
         * @param controls controls
         * @param security security
         * @return the operation result
         */
        public static Config defaults(final Path directory, final RaftPeerId node, final RaftGroup group, final Map<String, HikariDataSource> controls, final RatisTlsSecurity security) {
            return new Config(directory, node, group, controls, security, WalOptions.defaults(), WalBackupOptions.defaults(), MysqlWalReplicaBackendFactory.Options.defaults(),
                    RatisTransportLimits.defaults(), Maintenance.defaults());
        }

        /**
         * Tostrring does not call external data sources or key objects, avoiding log output connection information.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return "ManagedConfig[node=" + node + ", group=" + group.getGroupId() + ", directory=" + directory + ", shards=" + controls.keySet() + ", credentials=redacted]";
        }
    }

    /**
     * The cache sample corresponds to the fixed kernel generation, with recent majority certification only for route tips, and actual transaction still executes read barrier.
     *
     * @param localReady local ready
     * @param writableHint writable hint
     * @param verifiedEpoch verified epoch
     * @param generation generation
     * @param storage storage
     * @param health health
     * @param gc gc
     * @param snapshot snapshot
     * @param ageMillis age millis
     * @param stale stale
     * @param error error
     */
    public record Status(boolean localReady, boolean writableHint, long verifiedEpoch, UUID generation, WalMvccRuntime.Stats storage,
            RatisWalNode.HealthStats health, RatisWalNode.GcStats gc, RatisWalNode.SnapshotStats snapshot, long ageMillis, boolean stale, String error) {
    }

    /**
     * Automatic transaction callback only exposes business operations and does not provide manual commit or rollback.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    public interface Work<T> {

        /**
         * The callback is only for this transaction operation, and external side effects require an alternative business idempotent /message program.
         *
         * @param transaction transaction context
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        T run(RatisTransaction.Operations transaction) throws Exception;
    }

    /**
     * Only return business values and receipt after actual confirmation of commit.
     *
     * @param <T> value type
     * @param value value to write
     * @param receipt receipt
     */
    public record Execution<T>(T value, WalReceipt receipt) {

        /**
         * Business values can be empty, but the successful result is always a kernel returned completed receipt.
         *
         * @param value value to write
         * @param receipt receipt
         */
        public Execution {
            Objects.requireNonNull(receipt);
        }

        /**
         * Read-only transaction completes unwritten durable decision. Empty summary receipt does not commit to reboot querying the new transaction ID.
         *
         * @return the operation result
         */
        public boolean durableDecision() {
            return !receipt.digest().isEmpty();
        }
    }

    /**
     * If the startup fails and the clean-up is not completed, keep the resource handle and the caller must clear or terminate the process.
     */
    public static final class StartupException extends RatisStartupException {

        /**
         * Only the clean-up capacity will be retained and the operational entry points for uncompleted nodes will not be opened.
         *
         * @param failure failure
         * @param pending pending
         */
        StartupException(final Throwable failure, final java.io.Closeable pending) {
            super(failure, pending);
        }
    }
}
