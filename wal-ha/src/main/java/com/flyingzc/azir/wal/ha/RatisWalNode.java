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
import com.flyingzc.azir.wal.WalCommit;
import com.flyingzc.azir.wal.WalCommitAuthority;
import com.flyingzc.azir.wal.WalCommitLookup;
import com.flyingzc.azir.wal.WalMaterializedStore;
import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalMvccRuntime;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalReadSnapshot;
import com.flyingzc.azir.wal.WalReceipt;
import com.flyingzc.azir.wal.WalReplicaIdentity;
import com.flyingzc.azir.wal.WalReplicationBatch;
import com.flyingzc.azir.wal.WalRetentionOptions;
import com.flyingzc.azir.wal.WalTransaction;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import com.flyingzc.azir.common.IsolationLevel;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;

import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Replicates complete transaction decisions with Apache Ratis and coordinates node resources and recovery.
 */
public final class RatisWalNode implements AutoCloseable {

    private final WalReplicaIdentity identity;

    private final RaftGroup group;

    private final Authority authority;

    private final RatisWalState state;

    private final RatisWalStateMachine machine;

    private final RaftServer server;

    private final RatisTlsSecurity security;

    private final RatisTransportBudget transport;

    private final Path nodeDirectory;

    private volatile RatisHealthController health;

    private volatile boolean healthQuarantined, healthCampaignBlocked;

    private final AtomicLong campaignDenials = new AtomicLong();

    private volatile long healthRetiredTerm;

    private volatile RatisSnapshotController snapshotController;

    private volatile RatisGcController versionGc;

    private volatile boolean snapshotLogPressure;

    private volatile boolean serving;

    private volatile boolean closed;

    private final java.util.concurrent.locks.ReentrantLock closeGuard = new java.util.concurrent.locks.ReentrantLock();

    private final java.util.concurrent.locks.ReentrantLock membershipGuard = new java.util.concurrent.locks.ReentrantLock();

    private final ClientId membershipClient = ClientId.randomId();

    private final AtomicLong membershipCalls = new AtomicLong();

    private final java.util.concurrent.ExecutorService membershipExecutor = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(1), Thread.ofPlatform().name("wal-membership-command").factory(), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());

    private java.util.concurrent.ScheduledExecutorService retirementExecutor;

    private final AtomicLong retirementRuns = new AtomicLong(), retiredGenerations = new AtomicLong(), retiredSchemas = new AtomicLong(), retiredEntries = new AtomicLong();

    private volatile RetirementProgress lastRetirement;

    private volatile String retirementFailure;

    /**
     * The return loop test entrance accepts only three fixed single nodes and does not allow the unconfigured TLS configuration to be exposed to an outside address.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param stores stores
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static RatisWalNode openLoopback(final Path directory, final RaftPeerId id, final RaftGroup group,
                                            final Set<String> shards, final Map<String, WalMaterializedStore> stores, final WalOptions options) throws IOException {
        if (group.getPeers().size() != 3 || group.getPeers().stream().anyMatch(peer -> !peer.getAddress().startsWith("127.0.0.1:"))
                || group.getPeer(id) == null) {
            throw new IllegalArgumentException("initial Raft integration requires three loopback peers");
        }
        for (var store : stores.values()) {
            store.validateProduction();
        }
        return new RatisWalNode(directory, id, group, shards, stores, options, null, WalBackupOptions.defaults());
    }

    /**
     * An snapshot recycle can be installed using a stand-alone generation plant, with no hidden upgrade of the fixed backend entrance.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisWalNode openGenerationalLoopback(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                        final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits) throws IOException {
        return openGenerationalForTest(directory, id, group, shards, factory, options, limits, stage -> {
        });
    }

    /**
     * External peer must provide a two-way TLS identity and authorized policy, with independent snapshot generation still being used at the back end.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @param security security
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisWalNode openAuthenticated(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                 final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits, final RatisTlsSecurity security) throws IOException {
        return openAuthenticatedForTest(directory, id, group, shards, factory, options, limits, security, stage -> {
        });
    }

    /**
     * The production assembly can significantly limit the number of authentication connection and calls, with the original entry using the default bounded value.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @param security security
     * @param transportLimits transport limits
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisWalNode openAuthenticated(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                 final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits, final RatisTlsSecurity security,
                                                 final RatisTransportLimits transportLimits) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), stage -> {
        }, security, transportLimits);
    }

    /**
     * TLS shares a production structure with the real installation of the border movement, and there is no substitute for authentication or durable storage in the package.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @param security security
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisWalNode openAuthenticatedForTest(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                 final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits, final RatisTlsSecurity security,
                                                 final RatisWalState.Hook hook) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook, security);
    }

    /**
     * The hosting assembly test also maintains formal transmission restrictions and hooks are not exposed to the business caller.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @param security security
     * @param transport transport
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static RatisWalNode openAuthenticatedForTest(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                 final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits, final RatisTlsSecurity security,
                                                 final RatisTransportLimits transport,
                                                 final RatisWalState.Hook hook) throws IOException {
        java.util.Objects.requireNonNull(security).validate(group, id);
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook, security, transport);
    }

    /**
     * The border movement is installed in the package only for real clusters failure verify and there are always empty hooks at regular entrances.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param factory factory
     * @param options options
     * @param limits limits
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static RatisWalNode openGenerationalForTest(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                                                final WalReplicaBackendFactory factory, final WalOptions options, final WalBackupOptions limits, final RatisWalState.Hook hook) throws IOException {
        if (group.getPeers().size() < 3 || group.getPeers().size() > 9 || group.getPeer(id) == null
                || group.getPeers().stream().anyMatch(peer -> !peer.getAddress().matches("127\\.0\\.0\\.1:[0-9]{1,5}"))) {
            throw new IllegalArgumentException("generational integration requires bounded loopback peers");
        }
        for (var peer : group.getPeers()) {
            int port = Integer.parseInt(peer.getAddress().substring(10));
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("invalid loopback port");
            }
        }
        return new RatisWalNode(directory, id, group, shards, Map.of(), options, java.util.Objects.requireNonNull(factory), java.util.Objects.requireNonNull(limits), hook);
    }

    /**
     * The local nodes and independent storage are tied before the consensus is activated, and business requests are not served when role is not ready.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param factory factory
     * @param limits limits
     * @throws IOException if the operation cannot complete
     */
    private RatisWalNode(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                         final Map<String, WalMaterializedStore> stores, final WalOptions options, final WalReplicaBackendFactory factory, final WalBackupOptions limits) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, stage -> {
        });
    }

    /**
     * The same durable sex protocol is shared between the official assembly and package, and hooks are not a substitute for any actual file operation.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param factory factory
     * @param limits limits
     * @param hook hook
     * @throws IOException if the operation cannot complete
     */
    private RatisWalNode(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                         final Map<String, WalMaterializedStore> stores, final WalOptions options, final WalReplicaBackendFactory factory, final WalBackupOptions limits,
                         final RatisWalState.Hook hook) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, hook, null);
    }

    /**
     * The authentication configuration is checked before any data is opened, and the mode tags are only published after the generation lock has been obtained.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param factory factory
     * @param limits limits
     * @param hook hook
     * @param security security
     * @throws IOException if the operation cannot complete
     */
    private RatisWalNode(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                         final Map<String, WalMaterializedStore> stores, final WalOptions options, final WalReplicaBackendFactory factory, final WalBackupOptions limits, final RatisWalState.Hook hook,
                         final RatisTlsSecurity security) throws IOException {
        this(directory, id, group, shards, stores, options, factory, limits, hook, security, RatisTransportLimits.defaults());
    }

    /**
     * The certified assembly assigns connection slots to TLS before the old ring reassembly does not change its transmission plant.
     *
     * @param directory directory
     * @param id id
     * @param group group
     * @param shards shards
     * @param stores stores
     * @param options options
     * @param factory factory
     * @param limits limits
     * @param hook hook
     * @param security security
     * @param transportLimits transport limits
     * @throws IOException if the operation cannot complete
     */
    private RatisWalNode(final Path directory, final RaftPeerId id, final RaftGroup group, final Set<String> shards,
                         final Map<String, WalMaterializedStore> stores, final WalOptions options, final WalReplicaBackendFactory factory, final WalBackupOptions limits, final RatisWalState.Hook hook,
                         final RatisTlsSecurity security,
                         final RatisTransportLimits transportLimits) throws IOException {
        this.security = security;
        transport = new RatisTransportBudget(transportLimits);
        this.group = group;
        identity = new WalReplicaIdentity(group.getGroupId().getUuid(), group.getGroupId().getUuid(), id.toString());
        nodeDirectory = directory;
        var healthPolicy = RatisHealthRecord.load(directory, identity);
        healthQuarantined = healthPolicy != null;
        healthCampaignBlocked = healthPolicy != null;
        var gcPolicy = RatisGcPolicy.load(directory, identity);
        if (gcPolicy != null && (healthPolicy == null || factory == null)) {
            throw new IOException("version GC policy requires generational health management");
        }
        var snapshotPolicy = RatisSnapshotPolicy.load(directory, identity);
        snapshotLogPressure = snapshotPolicy != null;
        if (snapshotPolicy != null && (healthPolicy == null || factory == null || !factory.supportsRetirement())) {
            throw new IOException("snapshot policy requires health management and bounded generational retirement");
        }
        if (healthPolicy != null && factory == null) {
            throw new IOException("health policy requires generational backend");
        }
        RatisTransportIdentity.verify(directory, identity, security != null);
        Path wal = directory.resolve("wal");
        Path raft = directory.resolve("raft");
        Path business = directory.resolve("business");
        boolean bootstrap = factory != null && validBootstrap(business, identity);
        boolean recovering = hasRaftMetadata(raft, group.getGroupId(), bootstrap);
        if (bootstrap && recovering) {
            verifyInitialVote(raft.resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta"));
        }
        if (factory == null && Files.exists(business) || factory != null && Files.exists(wal)) {
            throw new IOException("cannot implicitly convert node storage layout");
        }
        boolean evidence = factory == null ? Files.exists(wal.resolve("REPLICA")) : Files.exists(business.resolve("ACTIVE")) || Files.exists(business.resolve("INSTALLING")) || bootstrap;
        if (evidence && !recovering && !bootstrap) {
            throw new IOException("replica Raft voting state missing; rebuild with a new peer identity");
        }
        if (!evidence && recovering) {
            throw new IOException("replica MVCC state missing; snapshot recovery required");
        }
        Authority openedAuthority = null;
        RatisWalState openedState = null;
        RatisWalStateMachine openedMachine = null;
        RaftServer created = null;
        try {
            // The construction of early resources is also in the clean-up range; only objects actually returned are saved, avoiding access to unentrustedfinal fields.
            authority = openedAuthority = new Authority();
            state = openedState = factory == null ? new RatisWalState(WalMvccRuntime.openReplica(wal, shards, stores, options, WalRetentionOptions.preserveAll(), identity, authority))
                    : new RatisWalState(business, identity, shards, options, authority, factory, limits, hook);
            machine = openedMachine = new RatisWalStateMachine(identity, state, authority, limits, () -> !healthQuarantined && !healthCampaignBlocked && !snapshotLogPressure);
            var endpoint = RatisEndpoint.parse(group.getPeer(id).getAddress());
            var properties = properties(raft, endpoint.host(), endpoint.port());
            if (factory != null) {
                RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
                RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
                RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
                org.apache.ratis.RaftConfigKeys.Rpc.setType(properties, new DurableGrpcRpcType());
            }
            // Reads that the complete record is not a substitute for durable confirmation; acquires an exclusive generation lock and resynchronizes the control policy before service starts.
            if (healthPolicy != null) {
                healthPolicy.publish(directory);
            }
            if (gcPolicy != null) {
                gcPolicy.publish(directory);
            }
            if (snapshotPolicy != null) {
                snapshotPolicy.publish(directory);
            }
            if (gcPolicy != null) {
                state.requireBoundedGc();
            }
            if (snapshotPolicy != null) {
                state.requirePinnedSnapshots();
            }
            var parameters = clientParameters();
            var transportServer = new AtomicReference<RaftServer>();
            DurableGrpcRpcType.setCampaignGuard(parameters, this::campaignAllowed);
            if (security != null) {
                RatisTransportIdentity.persist(directory, identity);
                GrpcConfigKeys.setUseEpoll(properties, false);
                // Another old stream may be left in the re-establishment of official split heartbeats, and a single stream avoids exhausting a limited number of HTTP2 places.
                GrpcConfigKeys.Server.setHeartbeatChannel(properties, false);
                GrpcConfigKeys.Server.setServicesCustomizer(parameters, (builder, types) -> {
                    builder.intercept(new RatisTlsInterceptor(security, peer -> authenticatedMember(transportServer.get(), peer)));
                    return transport.configure(builder);
                });
            }
            created = RaftServer.newBuilder().setServerId(id).setGroup(group).setProperties(properties).setParameters(parameters).setStateMachine(machine)
                    .setOption(recovering ? org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER : org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).build();
            server = created;
            transportServer.set(server);
            authority.server.set(server);
            server.start();
            serving = true;
            if (healthPolicy != null) {
                startHealth(healthPolicy.options());
            }
            if (snapshotPolicy != null) {
                startSnapshotLifecycle(snapshotPolicy.options());
            }
            if (gcPolicy != null) {
                startVersionGc(gcPolicy.options());
            }
        } catch (final Throwable failure) {
            serving = false;
            var retainedServer = created;
            var retainedMachine = openedMachine;
            var retainedAuthority = openedAuthority;
            var retainedState = openedState;
            // The storage on which it relies is not shut down when the previous phase has not been emptyed and returns abnormally to clean only the handle for callers following the original stage
            // retry .
            var cleanup = new RatisCloseSequence(this::stopMaintenance,
                    () -> {
                        if (retainedServer != null) {
                            retainedServer.close();
                        }
                    },
                    () -> {
                        if (retainedMachine != null) {
                            retainedMachine.close();
                        }
                    },
                    () -> {
                        if (retainedAuthority != null) {
                            retainedAuthority.closeControl();
                        }
                    },
                    this::closeMembershipExecutor,
                    () -> {
                        if (retainedState != null) {
                            retainedState.close();
                        }
                    });
            throw RatisStartupException.afterFailure(failure, cleanup);
        }
    }

    /**
     * Stop preserving producers and retain the consensus, kernel and back-end resources required for tasks that have not yet been left out.
     *
     * @throws IOException if the operation cannot complete
     */
    private void stopMaintenance() throws IOException {
        if (versionGc != null) {
            versionGc.close();
        }
        if (snapshotController != null) {
            snapshotController.close();
        }
        if (health != null) {
            health.close();
        }
        membershipExecutor.shutdown();
        if (retirementExecutor != null) {
            retirementExecutor.shutdown();
            try {
                if (!retirementExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new IOException("retirement maintenance did not drain");
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("retirement close interrupted", interrupted);
            }
        }
    }

    /**
     * Consensus stop emptied member commands and allow local generation locks and database lease to be released.
     *
     * @throws IOException if the operation cannot complete
     */
    private void closeMembershipExecutor() throws IOException {
        membershipExecutor.shutdown();
        try {
            if (!membershipExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IOException("membership executor did not drain");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("membership executor close interrupted", interrupted);
        }
    }

    /**
     * The actual consensus configuration includes former members of the joint phase and non-voting members, and the removal of old certificates after completion is also disqualified from
     * RPC.
     *
     * @param current current
     * @param peer peer
     * @return the operation result
     */
    private boolean authenticatedMember(final RaftServer current, final String peer) {
        if (current == null) {
            return false;
        }
        try {
            var configuration = current.getDivision(group.getGroupId()).getRaftConf();
            var id = RaftPeerId.valueOf(peer);
            return java.util.stream.Stream.of(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER, org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)
                    .anyMatch(role -> configuration.getCurrentPeers(role).stream().anyMatch(value -> value.getId().equals(id))
                            || configuration.getPreviousPeers(role).stream().anyMatch(value -> value.getId().equals(id)));
        } catch (final IOException notInitialized) {
            return false;
        }
    }

    /**
     * Probe is also used for the same TLS identity and does not create a specified client.
     *
     * @return the operation result
     */
    private org.apache.ratis.conf.Parameters clientParameters() {
        return security == null ? new org.apache.ratis.conf.Parameters() : security.parameters();
    }

    /**
     * Accurately check the voting metadata of this group and refuse to use group files or empty format old ballots identity.
     *
     * @param directory directory
     * @param group group
     * @param bootstrap bootstrap
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static boolean hasRaftMetadata(final Path directory, final RaftGroupId group, final boolean bootstrap) throws IOException {
        Path groupDirectory = directory.resolve(group.getUuid().toString());
        Path current = groupDirectory.resolve("current");
        Path metadata = current.resolve("raft-meta");
        for (Path path : List.of(directory, groupDirectory, current, metadata)) {
            if (Files.isSymbolicLink(path)) {
                throw new IOException("Raft voting path contains symbolic link");
            }
        }
        boolean found = Files.isRegularFile(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        if (Files.exists(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isDirectory(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Raft storage is not a directory");
            }
            try (var entries = Files.list(directory)) {
                for (Path path : entries.toList()) {
                    if (!path.equals(groupDirectory) || !found && !bootstrap) {
                        throw new IOException("Raft storage does not contain this group's voting metadata");
                    }
                }
            }
        }
        return found;
    }

    /**
     * Only explicit initializations that have not been serviced can be retry; empty formatting is still strictly rejected without this tag in the old run directory.
     *
     * @param business business
     * @param identity identity
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static boolean validBootstrap(final Path business, final WalReplicaIdentity identity) throws IOException {
        Path marker = business.resolve("BOOTSTRAPPING");
        if (!Files.exists(marker, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        var record = RatisGenerationRecord.read(marker);
        if (!record.identity().equals(identity) || record.term() != -1 || Files.exists(business.resolve("INSTALLING"))) {
            throw new IOException("invalid bootstrap ownership or phase");
        }
        if (Files.exists(business.resolve("ACTIVE")) && !record.equals(RatisGenerationRecord.read(business.resolve("ACTIVE")))) {
            throw new IOException("bootstrap active identity mismatch");
        }
        return true;
    }

    /**
     * Unfinished initialization cannot have an election term or a vote and avoids the initialization of retry being used to remove identity participants in consensus.
     *
     * @param metadata metadata
     * @throws IOException if the operation cannot complete
     */
    private static void verifyInitialVote(final Path metadata) throws IOException {
        if (!Files.isRegularFile(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS) || Files.size(metadata) > 65_536) {
            throw new IOException("bootstrap voting metadata invalid");
        }
        var properties = new java.util.Properties();
        try (var input = Files.newInputStream(metadata, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            properties.load(input);
        }
        if (!"0".equals(properties.getProperty("term")) || !"".equals(properties.getProperty("votedFor"))) {
            throw new IOException("bootstrap node already participated in voting");
        }
    }

    /**
     * Configure a reliable log brush, read index and limited buffer, turn off the autolog snapshot cleanup before complete state snapshot.
     *
     * @param directory directory
     * @param host host
     * @param port port
     * @return the operation result
     */
    private static RaftProperties properties(final Path directory, final String host, final int port) {
        var props = new RaftProperties();
        RaftServerConfigKeys.setStorageDir(props, List.of(directory.toFile()));
        RaftServerConfigKeys.Read.setOption(props, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Log.setForceSyncNum(props, 1);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(props, false);
        RaftServerConfigKeys.Log.setSegmentSizeMax(props, SizeInBytes.valueOf("8MB"));
        RaftServerConfigKeys.Log.setWriteBufferSize(props, SizeInBytes.valueOf("32MB"));
        RaftServerConfigKeys.Log.setQueueElementLimit(props, 64);
        RaftServerConfigKeys.Log.setQueueByteLimit(props, SizeInBytes.valueOf("32MB"));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(props, SizeInBytes.valueOf("16MB"));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, false);
        RaftServerConfigKeys.Rpc.setTimeoutMin(props, TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(props, TimeDuration.valueOf(2000, TimeUnit.MILLISECONDS));
        GrpcConfigKeys.Server.setHost(props, host);
        GrpcConfigKeys.Server.setPort(props, port);
        GrpcConfigKeys.Server.setBossGroupSize(props, 1);
        GrpcConfigKeys.Server.setWorkerGroupSize(props, 2);
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolCached(props, false);
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(props, 4);
        GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(props, 8);
        GrpcConfigKeys.setMessageSizeMax(props, SizeInBytes.valueOf("32MB"));
        return props;
    }

    /**
     * Read connection budget without network I/O, the number of this group for which income is specified remains zero.
     *
     * @return the operation result
     */
    public TransportStats transportStats() {
        return transport.stats();
    }

    /**
     * The local role is used only to find out that reading and writing must still be checked for the term of the consensus port and barrier.
     *
     * @return the operation result
     */
    public long leaderEpoch() {
        return authority.epoch();
    }

    /**
     * Start transaction snapshot builds after the majority read barrier.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    public WalTransaction begin(final IsolationLevel isolation) {
        return business().begin(isolation);
    }

    /**
     * Host session fixed initial possession of the transaction kernel and closed only to clean up its unexposed commit local write set.
     *
     * @param isolation global transaction isolation level
     * @return the operation result
     */
    RatisTransaction beginManagedTransaction(final IsolationLevel isolation) {
        var original = business();
        return new RatisTransaction(this, original, original.begin(isolation));
    }

    /**
     * Most parties have recently proven that they are only called by the hosting observation line and do not create or relocate transaction operations.
     *
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    long probeManagedLeader(final long timeoutMillis) throws Exception {
        if (!ready()) {
            throw new IOException("managed replica not ready");
        }
        return authority.readBarrier(timeoutMillis);
    }

    /**
     * Returns a fixed generation statistical sample without converting the surveillance query to an operational data query.
     *
     * @return the operation result
     */
    RatisWalState.LocalSample managedSample() {
        return state.localSample();
    }

    /**
     * Each statement uses the same barrier frame snapshot.
     *
     * @param tx transaction context
     * @return the operation result
     */
    public WalReadSnapshot beginReadStatement(final WalTransaction tx) {
        return business().beginReadStatement(tx);
    }

    /**
     * Query book transaction and the same snapshot is visible version and cannot be exposed directly to the core around the role validate.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     * @param snapshot snapshot
     * @return the operation result
     */
    public Optional<byte[]> get(final WalTransaction tx, final String shard, final String key, final WalReadSnapshot snapshot) {
        return business().get(tx, shard, key, snapshot);
    }

    /**
     * There are no business changes between local buffered write set, replica.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     * @param value value to write
     */
    public void put(final WalTransaction tx, final String shard, final String key, final byte[] value) {
        business().put(tx, shard, key, value);
    }

    /**
     * Buffer tombstone, only after consensus commit.
     *
     * @param tx transaction context
     * @param shard logical shard identifier
     * @param key business key
     */
    public void delete(final WalTransaction tx, final String shard, final String key) {
        business().delete(tx, shard, key);
    }

    /**
     * Waiting for full majority and local installation, timeout is still working on UNKNOWN.
     *
     * @param tx transaction context
     * @return the operation result
     */
    public WalReceipt commit(final WalTransaction tx) {
        return business().commit(tx);
    }

    /**
     * Only the local transaction that has not entered decision is cleared.
     *
     * @param tx transaction context
     */
    public void rollback(final WalTransaction tx) {
        business().rollback(tx);
    }

    /**
     * The transaction result query also passed the majority barrier, missing unsupported rollback.
     *
     * @param transaction transaction context
     * @return the operation result
     */
    public WalCommitLookup lookupCommit(final UUID transaction) {
        return business().lookupCommit(transaction);
    }

    /**
     * Returns the local operational boundary that has been applied to provide for flat observation, which is not a substitute for linearization reading.
     *
     * @return the operation result
     */
    public long appliedSequence() {
        return state.core().stats().publishedSequence();
    }

    /**
     * Local Actual Raft application position for catch-up observation, unlike business commit serial number, does not replace majority read barrier.
     *
     * @return the operation result
     */
    public long appliedRaftIndex() {
        var position = machine.getLastAppliedTermIndex();
        return position == null ? -1 : position.getIndex();
    }

    /**
     * Node local observations do not constitute an linearization business query to be used for maintenance and levelling checks.
     *
     * @return the operation result
     */
    public WalMvccRuntime.Stats localStats() {
        return state.core().stats();
    }

    /**
     * Only materialization local WAL can be stopped, and this cannot be used to clean up the Raft log or declare a complete cluster snapshot.
     *
     * @return the operation result
     */
    public WalMvccRuntime.CheckpointResult checkpointLocal() {
        return business().checkpoint();
    }

    /**
     * Ready to complete generation has been opened and the role must still be confirmed by majority barrier.
     *
     * @return the operation result
     */
    public boolean ready() {
        return serving && !healthQuarantined && !snapshotLogPressure && state.ready() && state.coreHealthy() && !machine.failed() && (security == null || security.localIdentityValid());
    }

    /**
     * Creates a complete mirror in the actual application queue, and currently does not automatically clean up duplicate logs.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public long snapshot() throws IOException {
        return machine.takeSnapshot();
    }

    /**
     * Local has been placed in verify snapshot position for catch-up observation, instead of business read barrier.
     *
     * @return the operation result
     */
    public long snapshotIndex() {
        var snapshot = machine.getLatestSnapshot();
        return snapshot == null ? -1 : snapshot.getIndex();
    }

    /**
     * Durable Activate a health policy, rebooting will automatically manage recovery and close the campaign and business qualifications first.
     *
     * @param options options
     * @throws IOException if the operation cannot complete
     */
    public void enableHealthManagement(final RatisHealthOptions options) throws IOException {
        java.util.Objects.requireNonNull(options);
        closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) {
                throw new IOException("health management requires an open generational node");
            }
            if (health != null) {
                if (!health.options().equals(options)) {
                    throw new IOException("health management already configured differently");
                }
                return;
            }
            new RatisHealthRecord(identity, options).publish(nodeDirectory);
            startHealth(options);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * The durable policy and first-time startup use the same task. A health configuration cannot be omitted when restarting.
     *
     * @param options options
     */
    private void startHealth(final RatisHealthOptions options) {
        machine.configureProbeBudget(options.probeMillis());
        health = new RatisHealthController(this, options);
        health.start();
    }

    /**
     * Returns desensitization statistics, and maintains the original assembly syntax when not enabled.
     *
     * @return the operation result
     */
    public HealthStats healthStats() {
        var current = health;
        return current == null ? new HealthStats(false, healthQuarantined, healthCampaignBlocked, "DISABLED", 0, 0, 0, 0, 0, 0, null) : current.stats();
    }

    /**
     * Fixed kernel generation performs a physical backend check and verify generation is not switched before returning.
     *
     * @param options options
     * @return the operation result
     */
    HealthVerdict healthProbe(final RatisHealthOptions options) {
        if (!serving || !state.ready()) {
            return HealthVerdict.BUSY;
        }
        if (snapshotLogPressure) {
            return HealthVerdict.BAD;
        }
        var core = state.core();
        var status = core.lifecycle();
        if (status.backup()) {
            return HealthVerdict.BUSY;
        }
        if (machine.failed() || status.fatal() != null || status.closed() || !status.accepting()) {
            return HealthVerdict.BAD;
        }
        boolean backend = state.backendHealthy(core, options.probeMillis());
        if (!state.ready() || state.core() != core) {
            return HealthVerdict.BUSY;
        }
        var after = core.lifecycle();
        return backend && after.fatal() == null && after.materializationFailure() == null && !after.proposalBlocked() ? HealthVerdict.GOOD : HealthVerdict.BAD;
    }

    /**
     * Closes business eligibility immediately and repeats the old retirement handle; does not wait for database or consensus locks.
     */
    void healthIsolate() {
        healthQuarantined = true;
        healthCampaignBlocked = true;
        try {
            long term = server.getDivision(group.getGroupId()).getInfo().getCurrentTerm();
            healthRetiredTerm = Math.max(healthRetiredTerm, term);
            if (term > 0) {
                authority.obsoleteEpoch.accumulateAndGet(term, Math::max);
                authority.scheduleFence();
            }
        } catch (final IOException stopped) {
            /*
             * Retired divisions are not eligible to operate.
             */ }
    }

    /**
     * Isolated for control tasks and cache probe reading.
     *
     * @return the operation result
     */
    boolean healthQuarantined() {
        return healthQuarantined;
    }

    /**
     * The application of floor control prevents the launching of voting and compulsory campaigning, keeping regular stop-to-station voting, copying and primary election cycles.
     *
     * @return the operation result
     */
    boolean healthCampaignBlocked() {
        return healthCampaignBlocked;
    }

    /**
     * Both the sending of a campaign and the compulsory campaign at the far end were controlled by the same door, refusing to count it for verify.
     *
     * @return the operation result
     */
    private boolean campaignAllowed() {
        boolean allowed = !healthQuarantined && !healthCampaignBlocked && !snapshotLogPressure && state.ready() && state.coreHealthy() && !machine.failed();
        if (!allowed) {
            campaignDenials.incrementAndGet();
        }
        return allowed;
    }

    /**
     * The test in the bag observed that actual voting delivery was refused and did not rely on fixed sleep to extrapolate the candidate stage.
     *
     * @return the operation result
     */
    long campaignDenialsForTest() {
        return campaignDenials.get();
    }

    /**
     * Controlled hand-over at the time of target health does not provide evidence of health or majority isolation and requests for official removal.
     *
     * @param options options
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    boolean healthMaintainIsolation(final RatisHealthOptions options) throws Exception {
        long until = deadline(options.transferMillis());
        healthCampaignBlocked = true;
        if (!server.getDivision(group.getGroupId()).getInfo().isLeader() || !membershipGuard.tryLock()) {
            return false;
        }
        try {
            try {
                var current = membershipAt(until, true);
                var minimum = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value)
                        .get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
                for (var target : current.voters()) {
                    if (target.getId().toString().equals(identity.nodeId()) || !awaitHealthyPeer(currentGroup(current), target.getId(), minimum, until)) {
                        continue;
                    }
                    var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), target.getId(),
                            remaining(until));
                    var reply = control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS);
                    if (reply.isSuccess()) {
                        return true;
                    }
                }
            } catch (final Exception unavailable) {
                /*
                 * Qualifications for operations have been withdrawn and unproven targets cannot be forcibly promoted through management transfers.
                 */ }
            // Deportation is carried out by an official agreement; the remaining health nodes are still to be elected in accordance with the law and there are no false targets available.
            if (server.getDivision(group.getGroupId()).getInfo().isLeader()) {
                var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), null, 1000);
                control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(1000, TimeUnit.MILLISECONDS);
            }
            return false;
        } finally {
            membershipGuard.unlock();
        }
    }

    /**
     * The retirement proposal cannot be reused for the same term of office; followers need to apply the commit boundary.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    boolean healthCanRecover() throws Exception {
        if (!state.ready() || !state.coreHealthy() || machine.failed()) {
            return false;
        }
        var division = server.getDivision(group.getGroupId());
        var info = division.getInfo();
        if (info.isLeader() && info.getCurrentTerm() <= healthRetiredTerm) {
            return false;
        }
        long required = division.getRaftLog().getLastCommittedIndex();
        var status =
                RatisReplicaStatus.decode(machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), required).get(1000, TimeUnit.MILLISECONDS).getContent().toByteArray());
        return status.appliedIndex() >= required;
    }

    /**
     * Once continuous health and applied borders are met, recovery contests are held before operational qualifications are opened.
     *
     * @param options options
     */
    void healthResume(final RatisHealthOptions options) {
        healthCampaignBlocked = false;
        healthQuarantined = false;
    }

    /**
     * Durable Enables automatic mirrors and collection, with health isolation used to protect the transaction term after high log watermark.
     *
     * @param options options
     * @throws IOException if the operation cannot complete
     */
    public void enableSnapshotMaintenance(final RatisSnapshotOptions options) throws IOException {
        java.util.Objects.requireNonNull(options);
        closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots() || !state.supportsRetirement() || !state.supportsPinnedSnapshots()) {
                throw new IOException("snapshot lifecycle requires bounded backend retirement and pinned views");
            }
            if (snapshotController != null) {
                if (!snapshotController.options().equals(options)) {
                    throw new IOException("snapshot lifecycle already configured differently");
                }
                return;
            }
            state.requirePinnedSnapshots();
            if (health == null) {
                enableHealthManagement(RatisHealthOptions.defaults());
            }
            new RatisSnapshotPolicy(identity, options).publish(nodeDirectory);
            startSnapshotLifecycle(options);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Restarts the automatic recovery policy and does not enable an automated export path for the official synchronized application thread.
     *
     * @param options options
     */
    private void startSnapshotLifecycle(final RatisSnapshotOptions options) {
        snapshotController = new RatisSnapshotController(this, options);
        snapshotController.start();
    }

    /**
     * Reads only the cache count and maintains the original entry syntax when not configured.
     *
     * @return the operation result
     */
    public SnapshotStats snapshotStats() {
        var current = snapshotController;
        return current == null ? new SnapshotStats(false, "DISABLED", 0, -1, -1, 0, 0, 0, false, false, null) : current.stats();
    }

    /**
     * Visible durable enabled bounded version collection with no configured node to be automatically converted.
     *
     * @param options options
     * @throws IOException if the operation cannot complete
     */
    public void enableVersionGc(final RatisGcOptions options) throws IOException {
        java.util.Objects.requireNonNull(options);
        closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) {
                throw new IOException("version GC requires an open generational node");
            }
            if (versionGc != null) {
                if (!versionGc.options().equals(options)) {
                    throw new IOException("version GC already configured differently");
                }
                return;
            }
            state.requireBoundedGc();
            if (health == null) {
                enableHealthManagement(RatisHealthOptions.defaults());
            }
            new RatisGcPolicy(identity, options).publish(nodeDirectory);
            startVersionGc(options);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * The first assembly and restart strategy recovery shares the only collection worker.
     *
     * @param options options
     */
    private void startVersionGc(final RatisGcOptions options) {
        versionGc = new RatisGcController(this, options);
        versionGc.start();
    }

    /**
     * Stop collection when installation and application fail. Isolation replica can still clean up its security version.
     *
     * @param options options
     * @return the operation result
     */
    RatisWalState.GcSample collectVersionGarbage(final RatisGcOptions options) {
        if (!serving || !state.ready() || machine.failed()) {
            return null;
        }
        return state.collectVersionGarbage(options);
    }

    /**
     * Returns cache and sampling age, maintenance error or stagnation will not be hidden by probe.
     *
     * @return the operation result
     */
    public GcStats gcStats() {
        var current = versionGc;
        return current == null ? new GcStats(false, "DISABLED", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, null, null) : current.stats();
    }

    /**
     * The log back pressure does not block the reproduction and in-house maintenance required to complete the mirror.
     *
     * @return the operation result
     */
    boolean snapshotMaintenanceReady() {
        return serving && state.ready() && state.coreHealthy() && !machine.failed() && (security == null || security.localIdentityValid());
    }

    /**
     * The back pressure only closed the new business entrance and decision was accepted to continue to comply with the original commit contract.
     *
     * @param pressure pressure
     */
    void snapshotLogPressure(final boolean pressure) {
        snapshotLogPressure = pressure;
    }

    /**
     * The current watermark status is available for health control and cache monitoring.
     *
     * @return the operation result
     */
    boolean snapshotLogPressure() {
        return snapshotLogPressure;
    }

    /**
     * The mirror mission is a status machine, and the dispatcher retains only one observation handle.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    CompletableFuture<Long> requestSnapshotImage() throws IOException {
        return machine.requestSnapshot();
    }

    /**
     * Bounded Statistics own Raft segment files without scanning operations database or the entire work area.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    long measureRaftLogBytes() throws IOException {
        Path current = nodeDirectory.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current");
        long total = 0;
        int inspected = 0;
        try (var files = java.nio.file.Files.list(current)) {
            var iterator = files.iterator();
            while (iterator.hasNext()) {
                if (++inspected > 4096) {
                    throw new IOException("Raft log catalog exceeds maintenance budget");
                }
                var file = iterator.next();
                String name = file.getFileName().toString();
                if (!name.matches("log_(inprogress_[0-9]+|[0-9]+-[0-9]+)")) {
                    continue;
                }
                if (!java.nio.file.Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("Raft log segment is not regular");
                }
                try {
                    total = Math.addExact(total, java.nio.file.Files.size(file));
                } catch (final ArithmeticException invalid) {
                    throw new IOException("Raft log byte counter overflow", invalid);
                }
            }
        }
        return total;
    }

    /**
     * Only re- verify and fully activated snapshot borders are used for each deletion, keeping tails and all unoverrided records.
     *
     * @param snapshot snapshot
     * @param preserve preserve
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    CompletableFuture<Long> purgeCoveredSnapshot(final long snapshot, final long preserve) throws IOException {
        if (!snapshotMaintenanceReady()) {
            throw new IOException("node not ready for snapshot purge");
        }
        long complete = machine.durableSnapshotBoundary(snapshot);
        var division = server.getDivision(group.getGroupId());
        long limit = Math.min(Math.min(snapshot, complete) - preserve, Math.min(appliedRaftIndex(), division.getRaftLog().getLastCommittedIndex()));
        if (limit < 0) {
            return CompletableFuture.completedFuture(-1L);
        }
        state.lifecycleStage(RatisWalState.Stage.BEFORE_LOG_PURGE, division.getRaftLog().getStartIndex(), limit);
        return division.getRaftLog().purge(limit).thenApply(purged -> {
            try {
                state.lifecycleStage(RatisWalState.Stage.AFTER_LOG_PURGE, division.getRaftLog().getStartIndex(), limit);
            } catch (final IOException failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
            return purged;
        });
    }

    /**
     * Common limited maintenance count for old generation, old snapshot and interrupted mirrors.
     *
     * @throws IOException if the operation cannot complete
     */
    void maintainSnapshotFiles() throws IOException {
        if (!snapshotMaintenanceReady()) {
            return;
        }
        machine.maintainFiles(new RatisRetirementWork(RatisRetirementOptions.defaults()));
    }

    /**
     * Records the snapshot complete location of the durable sex check for failure acceptance and monitoring.
     *
     * @return the operation result
     */
    public long durableSnapshotAckIndex() {
        return machine.durableSnapshotAckIndex();
    }

    /**
     * Bounded collection Old backend, old directories and old snapshot, current generation and incomplete installation are always protected.
     *
     * @param options options
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RetirementProgress retireOldState(final RatisRetirementOptions options) throws IOException {
        if (!ready() || !state.supportsSnapshots()) {
            throw new IOException("node not ready for generation maintenance");
        }
        var work = new RatisRetirementWork(options);
        boolean maintained = machine.maintainFiles(work);
        return new RetirementProgress(work.inspected, work.dropped, work.removed, work.completed, !maintained || state.retirementPending(), work.exhausted);
    }

    /**
     * Background collection is separately visible and does not automatically open the snapshot or Consensus Log cleanup.
     *
     * @param options options
     * @param intervalMillis interval millis
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public void enableRetirementMaintenance(final RatisRetirementOptions options, final long intervalMillis) throws IOException {
        if (intervalMillis < 100 || intervalMillis > 60_000) {
            throw new IllegalArgumentException("retirement interval must be 100..60000ms");
        }
        java.util.Objects.requireNonNull(options);
        closeGuard.lock();
        try {
            if (closed || !state.supportsSnapshots()) {
                throw new IOException("node cannot start retirement maintenance");
            }
            if (retirementExecutor != null) {
                throw new IOException("retirement maintenance already configured");
            }
            retirementExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("wal-retirement-" + identity.nodeId()).factory());
            retirementExecutor.scheduleWithFixedDelay(() -> {
                if (!ready()) {
                    return;
                }
                try {
                    var progress = retireOldState(options);
                    lastRetirement = progress;
                    retirementRuns.incrementAndGet();
                    retiredGenerations.addAndGet(progress.completedGenerations());
                    retiredSchemas.addAndGet(progress.droppedSchemas());
                    retiredEntries.addAndGet(progress.removedEntries());
                    retirementFailure = null;
                } catch (final Exception failure) {
                    retirementFailure = failure.getClass().getSimpleName();
                }
            }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * Returns the type of recent maintenance progress and desensitization anomalies.
     *
     * @return the operation result
     */
    public RetirementStats retirementStats() {
        return new RetirementStats(retirementRuns.get(), retiredGenerations.get(), retiredSchemas.get(), retiredEntries.get(), lastRetirement, retirementFailure);
    }

    /**
     * Read members from the actual commit and applied configuration, unstable joint configuration does not serve as a starting point for new operations.
     *
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RatisMembershipView membership(final long timeoutMillis) throws IOException {
        return membershipAt(deadline(timeoutMillis));
    }

    /**
     * Generates new UUID nodes identity and fixed source configuration. Reuse operational instruments do not regenerate identity.
     *
     * @param removed removed
     * @param address address
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public RatisMemberReplacement planReplacement(final RaftPeerId removed, final String address, final long timeoutMillis) throws IOException {
        var original = membership(timeoutMillis);
        UUID operation = UUID.randomUUID();
        var peer = RaftPeer.newBuilder().setId("r-" + operation).setAddress(address).setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
        return new RatisMemberReplacement(operation, identity.groupId(), original, removed, peer);
    }

    /**
     * In the first phase, only non-voting members were admitted and the three old voting members and the majority demanded that they be retained.
     *
     * @param plan plan
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public ReplacementProgress beginReplacement(final RatisMemberReplacement plan, final long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis);
        if (!membershipGuard.tryLock()) {
            throw new IOException("another local membership operation is running");
        }
        try {
            return beginReplacementLocked(plan, until);
        } finally {
            membershipGuard.unlock();
        }
    }

    /**
     * The application location and kernel health were confirmed by replica, before a single vote was replaced by both CAS and the official joint consensus.
     *
     * @param plan plan
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public ReplacementProgress completeReplacement(final RatisMemberReplacement plan, final long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis);
        if (!membershipGuard.tryLock()) {
            throw new IOException("another local membership operation is running");
        }
        try {
            var progress = beginReplacementLocked(plan, until);
            if (progress.phase() == ReplacementPhase.COMPLETE) {
                return progress;
            }
            control(() -> authority.readBarrier(remaining(until))).get(remaining(until), TimeUnit.MILLISECONDS);
            var status = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value)
                    .get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
            if (!status.healthy()) {
                throw new IOException("coordinator business kernel is not healthy");
            }
            boolean caughtUp = awaitHealthyPeer(currentGroup(progress.membership()), plan.replacement().getId(), status, until);
            if (!caughtUp) {
                return new ReplacementProgress(ReplacementPhase.WAITING_FOR_LEARNER, membershipAt(until));
            }
            var current = membershipAt(until);
            int phase = phase(plan, current);
            if (phase == 2) {
                return new ReplacementProgress(ReplacementPhase.COMPLETE, current);
            }
            if (phase != 1) {
                throw new IOException("replacement learner configuration no longer current");
            }
            if (plan.removed().toString().equals(identity.nodeId())) {
                throw new IOException("transfer leadership to a retained voter before removing this node");
            }
            changeMembership(current, plan.completed(current.index()), until);
            return new ReplacementProgress(ReplacementPhase.COMPLETE, membershipAt(until));
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("replacement wait interrupted; retain ticket", failure);
        } catch (final IOException failure) {
            throw failure;
        } catch (final Exception failure) {
            throw new IOException("replacement progress unresolved; retain and retry the same ticket", failure);
        } finally {
            membershipGuard.unlock();
        }
    }

    /**
     * Managing masters allow only existing voting members who have been evened and certifying nodes do not require the opening of remote original management services.
     *
     * @param target target
     * @param timeoutMillis timeout millis
     * @throws IOException if the operation cannot complete
     */
    public void transferLeadership(final RaftPeerId target, final long timeoutMillis) throws IOException {
        long until = deadline(timeoutMillis);
        if (!membershipGuard.tryLock()) {
            throw new IOException("another local management operation is running");
        }
        try {
            var current = membershipAt(until);
            if (target.toString().equals(identity.nodeId()) || current.voters().stream().noneMatch(peer -> peer.getId().equals(target))) {
                throw new IOException("leadership target must be another current voter");
            }
            var minimum = RatisReplicaStatus.decode(control(() -> machine.query(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())))).thenCompose(value -> value)
                    .get(remaining(until), TimeUnit.MILLISECONDS).getContent().toByteArray());
            if (!minimum.healthy() || !awaitHealthyPeer(currentGroup(current), target, minimum, until)) {
                throw new IOException("leadership target not caught up and healthy");
            }
            var request = new TransferLeadershipRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), target, remaining(until));
            var reply = control(() -> server.transferLeadershipAsync(request)).thenCompose(value -> value).get(remaining(until), TimeUnit.MILLISECONDS);
            if (!reply.isSuccess()) {
                throw new IOException("leadership transfer rejected", reply.getException());
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("leadership transfer interrupted; inspect actual leader", interrupted);
        } catch (final IOException failure) {
            throw failure;
        } catch (final Exception failure) {
            throw new IOException("leadership transfer unresolved; inspect actual leader", failure);
        } finally {
            membershipGuard.unlock();
        }
    }

    /**
     * Fixed-fixing of boundaries and reuse of a certified client, with the number of enquiries shared with the duration of the entire management operation.
     *
     * @param current current
     * @param peer peer
     * @param minimum minimum
     * @param until until
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private boolean awaitHealthyPeer(final RaftGroup current, final RaftPeerId peer, final RatisReplicaStatus minimum, final long until) throws Exception {
        long queryMillis = Math.min(health == null ? 1000 : health.options().probeMillis() + 500, remaining(until));
        var properties = new RaftProperties();
        org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(queryMillis, TimeUnit.MILLISECONDS));
        long grace = Math.min(until, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
        try (
                var client = org.apache.ratis.client.RaftClient.newBuilder().setRaftGroup(current).setLeaderId(RaftPeerId.valueOf(identity.nodeId())).setProperties(properties)
                        .setParameters(clientParameters()).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build()) {
            while (grace - System.nanoTime() >= TimeUnit.MILLISECONDS.toNanos(queryMillis)) {
                try {
                    var reply = client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisReplicaStatus.request())), minimum.appliedIndex(), peer);
                    if (reply.isSuccess() && reply.getMessage() != null) {
                        var target = RatisReplicaStatus.decode(reply.getMessage().getContent().toByteArray());
                        if (target.group().equals(identity.groupId()) && target.node().equals(peer.toString()) && target.healthy()
                                && target.appliedIndex() >= minimum.appliedIndex() && target.publishedSequence() >= minimum.publishedSequence()) {
                            return true;
                        }
                    }
                } catch (final IOException notReady) {
                    /*
                     * bounded tried not to be even, keeping the current role and operating instrument.
                     */ }
                TimeUnit.MILLISECONDS.sleep(30);
            }
        }
        return false;
    }

    /**
     * To continue at any stage would be to accept only the same group and the original, intermediate and final three precise configurations.
     *
     * @param plan plan
     * @param until until
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private ReplacementProgress beginReplacementLocked(final RatisMemberReplacement plan, final long until) throws IOException {
        if (!state.supportsSnapshots() || !plan.groupId().equals(identity.groupId())) {
            throw new IOException("replacement requires matching snapshot-capable group");
        }
        if (security != null) {
            security.requirePeer(plan.replacement());
        } else if (plan.joiningGroup().getPeers().stream().anyMatch(peer -> !peer.getAddress().matches("127\\.0\\.0\\.1:[0-9]{1,5}"))) {
            throw new IOException("plaintext member replacement restricted to loopback");
        }
        var current = membershipAt(until);
        int phase = phase(plan, current);
        if (phase == 2) {
            return new ReplacementProgress(ReplacementPhase.COMPLETE, current);
        }
        if (phase == 0) {
            changeMembership(current, plan.staged(current.index()), until);
            current = membershipAt(until);
        }
        return new ReplacementProgress(ReplacementPhase.LEARNER_ADDED, current);
    }

    /**
     * Local Role Change or Configuration has not yet been applied commit Refusal to manage observation and does not repeat the old majority confirmation.
     *
     * @param until until
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisMembershipView membershipAt(final long until) throws IOException {
        return membershipAt(until, false);
    }

    /**
     * The health path observation configuration must also pass the fresh majority fence.
     *
     * @param until until
     * @param management management
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private RatisMembershipView membershipAt(final long until, final boolean management) throws IOException {
        try {
            long epoch = control(() -> authority.barrier(remaining(until), management)).get(remaining(until), TimeUnit.MILLISECONDS);
            var division = server.getDivision(group.getGroupId());
            var configuration = division.getRaftConf();
            if (!configuration.getPreviousPeers().isEmpty() || !configuration.getPreviousPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).isEmpty()
                    || configuration.getLogEntryIndex() > division.getRaftLog().getLastCommittedIndex() || configuration.getLogEntryIndex() > machine.getLastAppliedTermIndex().getIndex()
                    || epoch != (management ? authority.consensusEpoch() : authority.epoch())) {
                throw new IOException("membership is not yet stable and applied");
            }
            return new RatisMembershipView(configuration.getLogEntryIndex(), List.copyOf(configuration.getCurrentPeers()),
                    List.copyOf(configuration.getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)));
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("membership observation interrupted", failure);
        } catch (final IOException failure) {
            throw failure;
        } catch (final Exception failure) {
            throw new IOException("cannot confirm current membership", failure);
        }
    }

    /**
     * CAS prevents expired administrators from overwhelming another operation; timeout does not cancel configuration requests that have reached consensus.
     *
     * @param previous previous
     * @param next next
     * @param until until
     * @throws IOException if the operation cannot complete
     */
    private void changeMembership(final RatisMembershipView previous, final RatisMembershipView next, final long until) throws IOException {
        var arguments = new SetConfigurationRequest.Arguments.Builder().setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
                .setServersInCurrentConf(previous.voters()).setListenersInCurrentConf(previous.learners()).setServersInNewConf(next.voters()).setListenersInNewConf(next.learners()).build();
        var request = new SetConfigurationRequest(membershipClient, RaftPeerId.valueOf(identity.nodeId()), group.getGroupId(), membershipCalls.incrementAndGet(), arguments);
        try {
            var future = control(() -> {
                remaining(until);
                var actual = server.getDivision(group.getGroupId()).getRaftConf();
                if (authority.epoch() <= 0 || actual.getLogEntryIndex() != previous.index()
                        || !RatisMembershipView.same(List.copyOf(actual.getCurrentPeers()), previous.voters())
                        || !RatisMembershipView.same(List.copyOf(actual.getCurrentPeers(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)), previous.learners())) {
                    throw new IOException("full membership changed before acceptance");
                }
                // No Ratis internal monitor calls the walk-in entrance; inside that portal is waiting to execute a line acceptance request.
                return server.setConfigurationAsync(request);
            }).thenCompose(value -> value).thenApplyAsync(reply -> {
                try {
                    if (!reply.isSuccess()) {
                        throw new IOException("membership request rejected", reply.getException());
                    }
                    RaftDurability.current(server, group.getGroupId());
                    return reply;
                } catch (final IOException failure) {
                    throw new java.util.concurrent.CompletionException(failure);
                }
            }, membershipExecutor);
            future.get(remaining(until), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IOException("membership wait interrupted; retain ticket", failure);
        } catch (final Exception failure) {
            throw new IOException("membership outcome unresolved; inspect configuration and retry the same ticket", failure);
        }
    }

    /**
     * The target group is taken from the actual configuration instead of the initial static node list.
     *
     * @param view view
     * @return the operation result
     */
    private RaftGroup currentGroup(final RatisMembershipView view) {
        var peers = new java.util.ArrayList<>(view.voters());
        peers.addAll(view.learners());
        return RaftGroup.valueOf(group.getGroupId(), peers);
    }

    /**
     * The configuration mismatch is a condition of rejection and does not automatically rewrite the operating instrument.
     *
     * @param plan plan
     * @param current current
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static int phase(final RatisMemberReplacement plan, final RatisMembershipView current) throws IOException {
        try {
            return plan.phase(current);
        } catch (final IllegalStateException mismatch) {
            throw new IOException("membership no longer matches replacement ticket", mismatch);
        }
    }

    /**
     * The management operation uses a single-to-total duration and does not allow for unlimited waiting periods.
     *
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static long deadline(final long timeoutMillis) {
        if (timeoutMillis < 1 || timeoutMillis > 120_000) {
            throw new IllegalArgumentException("membership timeout must be 1..120000ms");
        }
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    /**
     * Each phase is shared at the same time budget and expired without new configuration requests.
     *
     * @param deadline deadline
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static long remaining(final long deadline) throws IOException {
        long value = deadline - System.nanoTime();
        if (value <= 0) {
            throw new IOException("membership operation timed out; retain ticket");
        }
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(value));
    }

    /**
     * Even the method-access synchronization waiting and synchronisation of files is isolated to a limited implementer, with the total caller period not dependent on an original walk name.
     *
     * @param <T> value type
     * @param task task
     * @return the operation result
     */
    private <T> CompletableFuture<T> control(final Control<T> task) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return task.run();
            } catch (final Exception failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
        }, membershipExecutor);
    }

    /**
     * All operations are rejected at installation or closure and cannot be returned to the old kernel reference.
     *
     * @return the operation result
     * @throws WalMvccException if the operation cannot complete
     */
    private WalMvccRuntime business() {
        if (!ready()) {
            throw new WalMvccException(com.flyingzc.azir.common.ErrorCode.STORAGE_UNAVAILABLE, "replica business generation not ready", null, null);
        }
        return state.core();
    }

    /**
     * The failure acceptance and inspection in the package only allows for clean-up at a full snapshot position to force the trigger of real backward replica installation.
     *
     * @param index index
     * @return the operation result
     * @throws Exception if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    long purgeForTest(final long index) throws Exception {
        var snapshot = machine.getLatestSnapshot();
        if (snapshot == null || index > snapshot.getIndex()) {
            throw new IOException("no durable snapshot covers purge");
        }
        return server.getDivision(group.getGroupId()).getRaftLog().purge(index).get(30, TimeUnit.SECONDS);
    }

    /**
     * The verify actual log gap in the package cannot be extrapolated from the purge return value of replica only. snapshot must be installed.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    long firstLogIndexForTest() throws IOException {
        return server.getDivision(group.getGroupId()).getRaftLog().getStartIndex();
    }

    /**
     * Discontinue access and consensus, then release local WAL and not close the passage in advance.
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
            serving = false;
            stopMaintenance();
            try {
                long epoch = server.getDivision(group.getGroupId()).getInfo().getCurrentTerm();
                if (epoch > 0) {
                    state.fence(epoch);
                }
            } catch (final IOException alreadyStopped) {
                /*
                 * The previous closure had stopped consensus, but the generation resources still needed to be safely closed.
                 */ }
            server.close();
            machine.close();
            authority.closeControl();
            closeMembershipExecutor();
            state.close();
            closed = true;
        } finally {
            closeGuard.unlock();
        }
    }

    /**
     * The current, peak and rejection count of the authentication transfer does not include certificates, network addresses or business content.
     *
     * @param connections connections
     * @param peakConnections peak connections
     * @param rejectedConnections rejected connections
     * @param calls calls
     * @param peakCalls peak calls
     * @param rejectedCalls rejected calls
     */
    public record TransportStats(int connections, int peakConnections, long rejectedConnections, int calls, int peakCalls, long rejectedCalls) {
    }

    /**
     * Health-controlled caches, which do not trigger database I/O in surveillance reading.
     *
     * @param enabled enabled
     * @param quarantined quarantined
     * @param electionPaused election paused
     * @param state state
     * @param failures failures
     * @param successes successes
     * @param rounds rounds
     * @param isolations isolations
     * @param transfers transfers
     * @param recoveries recoveries
     * @param error error
     */
    public record HealthStats(boolean enabled, boolean quarantined, boolean electionPaused, String state, int failures, int successes,
            long rounds, long isolations, long transfers, long recoveries, String error) {

        /**
         * Effective campaign gate control; the old electionPause field remains compatible but no longer calls official Pause with recovery gaps.
         *
         * @return the operation result
         */
        public boolean campaignBlocked() {
            return electionPaused;
        }
    }

    /**
     * The pause is not miscalculated as database failure during installation and backup.
     */
    enum HealthVerdict {
        BUSY, GOOD, BAD
    }

    /**
     * Automatic mirror and log collection, Cache status, watermark is the collaborative back pressure threshold.
     *
     * @param enabled enabled
     * @param state state
     * @param raftLogBytes raft log bytes
     * @param firstLogIndex first log index
     * @param snapshotIndex snapshot index
     * @param rounds rounds
     * @param snapshots snapshots
     * @param purges purges
     * @param exporting exporting
     * @param backpressure backpressure
     * @param error error
     */
    public record SnapshotStats(boolean enabled, String state, long raftLogBytes, long firstLogIndex, long snapshotIndex, long rounds,
            long snapshots, long purges, boolean exporting, boolean backpressure, String error) {
    }

    /**
     * Version collection indicator is a fixed generation Cache, and probe does not access database.
     *
     * @param enabled enabled
     * @param state state
     * @param rounds rounds
     * @param failures failures
     * @param inspectedKeys inspected keys
     * @param removedRows removed rows
     * @param safePoint oldest retained snapshot timestamp
     * @param publishedSequence published sequence
     * @param retiredMemoryVersions retired memory versions
     * @param durationMillis duration millis
     * @param overruns overruns
     * @param sampleAgeMillis sample age millis
     * @param stale stale
     * @param error error
     * @param generation generation
     */
    public record GcStats(boolean enabled, String state, long rounds, long failures, long inspectedKeys, long removedRows,
            long safePoint, long publishedSequence, long retiredMemoryVersions, long durationMillis, long overruns,
            long sampleAgeMillis, boolean stale, String error, UUID generation) {
    }

    /**
     * A real collection count round; time budget collaboration, single file system and drive call cannot be cancelled by interruption.
     *
     * @param backendShards backend shards
     * @param droppedSchemas dropped schemas
     * @param removedEntries removed entries
     * @param completedGenerations completed generations
     * @param pending pending
     * @param budgetExhausted budget exhausted
     */
    public record RetirementProgress(int backendShards, int droppedSchemas, int removedEntries, int completedGenerations, boolean pending, boolean budgetExhausted) {
    }

    /**
     * Watch read-only for cache count, not perform database queries in probe or wait to maintain locks.
     *
     * @param runs runs
     * @param generations generations
     * @param schemas schemas
     * @param entries entries
     * @param last last
     * @param failure failure
     */
    public record RetirementStats(long runs, long generations, long schemas, long entries, RetirementProgress last, String failure) {
    }

    /**
     * Operating results distinguish between non-voting replica, waiting for fix and finished, not explaining timeout to rollback.
     */
    public enum ReplacementPhase {
        LEARNER_ADDED, WAITING_FOR_LEARNER, COMPLETE
    }

    /**
     * The current result binds the majority barrier stable configuration, and the operator must keep the original operating instrument to continue.
     *
     * @param phase phase
     * @param membership membership
     */
    public record ReplacementProgress(ReplacementPhase phase, RatisMembershipView membership) {
    }

    /**
     * Manage tasks to transmit I/O anomalies, timeout only stops callers waiting and does not interrupt operations that have reached consensus.
     *
     * @param <T> value type
     */
    @FunctionalInterface
    private interface Control<T> {

        /**
         * Runs in this node limited management implementer.
         *
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        T run() throws Exception;
    }

    /**
     * The only proposal fingerprint of the current leader, the original Raft client cannot forge unprepared business batch.
     *
     * @param epoch epoch
     * @param digest digest
     */
    private record Pending(long epoch, String digest) {
    }

    /**
     * Local commit for consensus adapter does not automatically re-direct and prevents the previous assignment of serial numbers from being reintroduced.
     */
    final class Authority implements WalCommitAuthority {

        final AtomicReference<RaftServer> server = new AtomicReference<>();

        final AtomicReference<Pending> pending = new AtomicReference<>();

        private final java.util.concurrent.ConcurrentHashMap<String, FencePermit> readFences = new java.util.concurrent.ConcurrentHashMap<>();

        private final java.util.concurrent.Semaphore readPermits = new java.util.concurrent.Semaphore(64);

        private final AtomicLong calls = new AtomicLong();

        private final ClientId client = ClientId.randomId();

        private final java.util.concurrent.ExecutorService control =
                java.util.concurrent.Executors.newSingleThreadExecutor(Thread.ofPlatform().name("wal-raft-control-" + identity.nodeId()).factory());

        private final AtomicLong obsoleteEpoch = new AtomicLong();

        private final java.util.concurrent.atomic.AtomicBoolean fencing = new java.util.concurrent.atomic.AtomicBoolean();

        /**
         * Role information is derived from the actual Raft partition, which is zero when it has not been started or closed.
         *
         * @return the operation result
         */
        @Override
        public long epoch() {
            return healthQuarantined ? 0 : consensusEpoch();
        }

        /**
         * Regulatory licences only observe actual consensus eligibility, and business segregation does not create an ordinary writing for registered health control fence.
         *
         * @return the operation result
         */
        private long consensusEpoch() {
            var current = server.get();
            if (!serving || !state.ready() || !state.coreHealthy() || machine.failed() || current == null || security != null && !security.localIdentityValid()) {
                return 0;
            }
            try {
                var division = current.getDivision(group.getGroupId());
                var info = division.getInfo();
                return info.isLeaderReady() && division.getRaftConf().getCurrentPeers().stream().anyMatch(peer -> peer.getId().toString().equals(identity.nodeId())) ? info.getCurrentTerm() : 0;
            } catch (final IOException failure) {
                return 0;
            }
        }

        /**
         * Operations fence require in-house operational qualifications and failure cannot be returned to management clearance.
         *
         * @param timeoutMillis timeout millis
         * @return the operation result
         * @throws Exception if the operation cannot complete
         */
        @Override
        public long readBarrier(final long timeoutMillis) throws Exception {
            return barrier(timeoutMillis, false);
        }

        /**
         * Health isolation pathways use a separate registered permit, and users of reading and writing are still segregated.
         *
         * @param timeoutMillis timeout millis
         * @param management management
         * @return the operation result
         * @throws Exception if the operation cannot complete
         * @throws IOException if the operation cannot complete
         */
        private long barrier(final long timeoutMillis, final boolean management) throws Exception {
            long term = management ? consensusEpoch() : epoch();
            if (term <= 0) {
                throw new IOException("not leader");
            }
            if (!readPermits.tryAcquire()) {
                throw new IOException("read fence queue full");
            }
            String hash = null;
            try {
                byte[] data = new RatisReadFence(term, identity, calls.incrementAndGet()).encode();
                hash = digest(data);
                readFences.put(hash, new FencePermit(term, management));
                var reply = server.get().submitClientRequestAsync(request(Message.valueOf(ByteString.copyFrom(data)), false, timeoutMillis)).get(timeoutMillis, TimeUnit.MILLISECONDS);
                check(reply);
                if (term != (management ? consensusEpoch() : epoch())) {
                    throw new IOException("leader epoch changed during read barrier");
                }
                return term;
            } finally {
                if (hash != null) {
                    readFences.remove(hash);
                }
                readPermits.release();
            }
        }

        /**
         * The future is only successful after the Raft Majority commit and status machine applications, with RPC not canceling log decision.
         *
         * @param term term
         * @param commits commits
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        @Override
        public CompletionStage<Void> submit(final long term, final List<WalCommit> commits) {
            try {
                if (term != epoch()) {
                    throw new IOException("proposal leader changed");
                }
                var batch = new WalReplicationBatch(term, identity, commits);
                byte[] data = batch.encode();
                var expected = new Pending(term, batch.digest());
                if (!pending.compareAndSet(null, expected)) {
                    throw new IOException("previous proposal unresolved");
                }
                var future = server.get().submitClientRequestAsync(request(Message.valueOf(ByteString.copyFrom(data)), false, 3000))
                        .thenApply(reply -> {
                            check(reply);
                            return (Void) null;
                        }).orTimeout(3000, TimeUnit.MILLISECONDS);
                future.whenComplete((ignored, failure) -> pending.compareAndSet(expected, null));
                return future;
            } catch (final Exception failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        /**
         * The original proposal for the current role, term and local preparation is repeated on a serial log.
         *
         * @param batch batch
         * @throws IOException if the operation cannot complete
         */
        void validate(final WalReplicationBatch batch) throws IOException {
            var expected = pending.get();
            if (!batch.origin().equals(identity) || batch.epoch() != epoch() || expected == null
                    || expected.epoch() != batch.epoch() || !expected.digest().equals(batch.digest())) {
                throw new IOException("unprepared or fenced proposal");
            }
        }

        /**
         * Reading fence is also registered by local and validate for a term, and any network client cannot falsify source identity.
         *
         * @param fence fence
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        void validate(final RatisReadFence fence, final byte[] bytes) throws IOException {
            var permit = readFences.get(digest(bytes));
            if (!fence.origin().equals(identity) || permit == null || permit.epoch() != fence.epoch() || fence.epoch() != (permit.management() ? consensusEpoch() : epoch())) {
                throw new IOException("unregistered or fenced read barrier");
            }
        }

        /**
         * A fixed summary does not contain a user receipt and does not depend on the probability filter.
         *
         * @param bytes bytes
         * @return the operation result
         * @throws IOException if the operation cannot complete
         */
        private String digest(final byte[] bytes) throws IOException {
            try {
                return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (final java.security.NoSuchAlgorithmException failure) {
                throw new IOException(failure);
            }
        }

        /**
         * The request for the current new term cannot be cleared with late notification, which only cleans up the handle of the old term.
         */
        void lostLeadership() {
            try {
                var info = server.get().getDivision(group.getGroupId()).getInfo();
                long term = info.getCurrentTerm();
                long obsolete = info.isLeaderReady() ? term - 1 : term;
                if (obsolete > 0) {
                    obsoleteEpoch.accumulateAndGet(obsolete, Math::max);
                    scheduleFence();
                }
            } catch (final IOException ignored) {
                /*
                 * Closed partitions no longer require services.
                 */ }
        }

        /**
         * The role callback cannot wait for the database lock at the Raft event thread, with up to one consolidated control task.
         */
        private void scheduleFence() {
            if (!fencing.compareAndSet(false, true)) {
                return;
            }
            control.execute(() -> {
                long handled = 0;
                try {
                    do {
                        handled = obsoleteEpoch.get();
                        state.fence(handled);
                        var old = pending.get();
                        if (old != null && old.epoch() <= handled) {
                            pending.compareAndSet(old, null);
                        }
                    } while (obsoleteEpoch.get() > handled);
                } finally {
                    fencing.set(false);
                    if (obsoleteEpoch.get() > handled) {
                        scheduleFence();
                    }
                }
            });
        }

        /**
         * Raft has stopped creating an event back-to-back air controller and does not interrupt inbound engine operations.
         *
         * @throws IOException if the operation cannot complete
         */
        void closeControl() throws IOException {
            control.shutdown();
            try {
                if (!control.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new IOException("replica control did not drain");
                }
            } catch (final InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
        }

        /**
         * Constructs a request to deliver only the local node, defaulting that the leader of the library client retry does not participate in the chain.
         *
         * @param message message
         * @param read read
         * @param timeout timeout
         * @return the operation result
         */
        private RaftClientRequest request(final Message message, final boolean read, final long timeout) {
            return RaftClientRequest.newBuilder().setClientId(client).setServerId(RaftPeerId.valueOf(identity.nodeId())).setGroupId(group.getGroupId())
                    .setCallId(calls.incrementAndGet()).setMessage(message).setType(read ? RaftClientRequest.readRequestType() : RaftClientRequest.writeRequestType()).setTimeoutMs(timeout).build();
        }

        /**
         * Clear agreements are out of reach as requests fail and unusual responses are not considered to be successful as consensus.
         *
         * @param reply reply
         * @throws java.util.concurrent.CompletionException if the operation cannot complete
         */
        private static void check(final RaftClientReply reply) {
            if (!reply.isSuccess()) {
                throw new java.util.concurrent.CompletionException(reply.getException() == null ? new IOException("Raft request unsuccessful") : reply.getException());
            }
        }

        /**
         * The licence type is only registered in this machine and does not originate from the RPC payload or client declaration.
         *
         * @param epoch epoch
         * @param management management
         */
        private record FencePermit(long epoch, boolean management) {
        }
    }
}
