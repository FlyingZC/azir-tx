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

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.rpc.RpcFactory;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * Keeps the official gRPC transfer and fills with durable sex fence required before a complete snapshot response is successful.
 */
public final class DurableGrpcRpcType implements RpcType {

    private static final String CAMPAIGN_PARAMETER = "azir.wal.campaign.guard";

    /**
     * The eligibility of the machine is derived from a credible assembly and cannot be changed by network parameters.
     *
     * @param parameters parameters
     * @param guard guard
     */
    static void setCampaignGuard(final Parameters parameters, final java.util.function.BooleanSupplier guard) {
        parameters.put(CAMPAIGN_PARAMETER, guard, java.util.function.BooleanSupplier.class);
    }

    /**
     * Ratis constructs an open and uninvolved reflection to load a custom RPC type.
     */
    public DurableGrpcRpcType() {
    }

    /**
     * The type name can be re-exampled by RpcType.valueOf without covering the official GRPC name.
     *
     * @return the operation result
     */
    @Override
    public String name() {
        return DurableGrpcRpcType.class.getName();
    }

    /**
     * Consensus, log additions, elections, clients and certifications are still being used for official realization.
     *
     * @param parameters parameters
     * @return the operation result
     */
    @Override
    public RpcFactory newFactory(final Parameters parameters) {
        return new Factory(parameters);
    }

    /**
     * Only snapshot of the proxy entry server protocol completes the reply and does not rewrite the Ratis election or log protocol.
     */
    private static final class Factory extends GrpcFactory {

        private org.apache.ratis.grpc.server.GrpcServicesImpl service;

        private final java.util.function.BooleanSupplier campaign;

        /**
         * The original parameter contains the future TLS settings, which are transmitted in full by the official plant.
         *
         * @param parameters parameters
         */
        Factory(final Parameters parameters) {
            super(parameters);
            var configured = parameters == null ? null : parameters.get(CAMPAIGN_PARAMETER, java.util.function.BooleanSupplier.class);
            campaign = configured == null ? () -> true : configured;
        }

        /**
         * The official supplement requires a specific gRPC service to keep its original streaming.
         *
         * @param division division
         * @param leader leader
         * @param follower follower
         * @return the operation result
         */
        @Override
        public org.apache.ratis.server.leader.LogAppender newLogAppender(final RaftServer.Division division,
                                                                         final org.apache.ratis.server.leader.LeaderState leader, final org.apache.ratis.server.leader.FollowerInfo follower) {
            return new org.apache.ratis.grpc.server.GrpcLogAppender(division, leader, follower) {

                /**
                 * The type conversion of the interface proxy only bypasses the outstation, and entry durable sex fence is still performed by the server agent.
                 *
                 * @return the operation result
                 */
                @Override
                public org.apache.ratis.grpc.server.GrpcServicesImpl getServerRpc() {
                    return service;
                }
            };
        }

        /**
         * Successfully completed single file snapshot must have a prefix to recovery durable; failed to disseminate as RPC failed.
         *
         * @param server server
         * @return the operation result
         */
        @Override
        public GrpcServices newRaftServerRpc(final RaftServer server) {
            var proxy = (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[]{RaftServer.class}, (wrapper, method, arguments) -> {
                if (method.getDeclaringClass() == Object.class && method.getName().equals("equals")) {
                    return wrapper == arguments[0];
                }
                if (method.getDeclaringClass() == Object.class && method.getName().equals("hashCode")) {
                    return System.identityHashCode(wrapper);
                }
                if (method.getName().equals("startLeaderElection") && !campaign.getAsBoolean()) {
                    throw new java.io.IOException("backend health campaign gate closed");
                }
                if (method.getName().equals("setConfigurationAsync")) {
                    // External original configuration cannot bypass the identity validate alignment and integrity, formally manage the UTM interface.
                    return java.util.concurrent.CompletableFuture.failedFuture(new java.io.IOException("use WAL membership replacement API"));
                }
                Object result;
                try {
                    result = method.invoke(server, arguments);
                } catch (final InvocationTargetException failure) {
                    throw failure.getCause();
                }
                if (method.getName().equals("requestVote") && arguments[0] instanceof RaftProtos.RequestVoteRequestProto request) {
                    RaftDurability.current(server, group(request.getServerRequest()));
                }
                if (method.getName().equals("appendEntriesAsync") && arguments[0] instanceof RaftProtos.AppendEntriesRequestProto request
                        && result instanceof java.util.concurrent.CompletableFuture<?> future) {
                    return future.thenApply(reply -> {
                        try {
                            var group = group(request.getServerRequest());
                            if (reply instanceof RaftProtos.AppendEntriesReplyProto response && response.getResult() == RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS
                                    && server.getDivision(group).getStateMachine() instanceof RatisWalStateMachine machine) {
                                machine.durableSnapshotBaseForAppend();
                            }
                            RaftDurability.current(server, group);
                            return reply;
                        } catch (final java.io.IOException failure) {
                            throw new java.util.concurrent.CompletionException(failure);
                        }
                    });
                }
                if (method.getName().equals("installSnapshot") && arguments[0] instanceof RaftProtos.InstallSnapshotRequestProto request
                        && request.hasSnapshotChunk() && request.getSnapshotChunk().getDone()
                        && result instanceof RaftProtos.InstallSnapshotReplyProto reply && reply.getResult() == RaftProtos.InstallSnapshotResult.SUCCESS) {
                    var group = group(request.getServerRequest());
                    if (!(server.getDivision(group).getStateMachine() instanceof RatisWalStateMachine machine)) {
                        throw new java.io.IOException("durable snapshot guard requires WAL state machine");
                    }
                    machine.durableSnapshotReceived(TermIndex.valueOf(request.getSnapshotChunk().getTermIndex()));
                }
                return result;
            });
            service = (org.apache.ratis.grpc.server.GrpcServicesImpl) super.newRaftServerRpc(proxy);
            return (GrpcServices) Proxy.newProxyInstance(GrpcServices.class.getClassLoader(), new Class<?>[]{GrpcServices.class}, (wrapper, method, arguments) -> {
                if (method.getDeclaringClass() == Object.class && method.getName().equals("equals")) {
                    return wrapper == arguments[0];
                }
                if (method.getDeclaringClass() == Object.class && method.getName().equals("hashCode")) {
                    return System.identityHashCode(wrapper);
                }
                // Synchronization of the terms of office and the list of voters prior to the issuance of a request for an election cannot protect only remote ballot papers.
                if (method.getName().equals("requestVote") && arguments[0] instanceof RaftProtos.RequestVoteRequestProto request) {
                    if (!campaign.getAsBoolean()) {
                        throw new java.io.IOException("backend health campaign gate closed");
                    }
                    RaftDurability.current(server, group(request.getServerRequest()));
                }
                try {
                    return method.invoke(service, arguments);
                } catch (final InvocationTargetException failure) {
                    throw failure.getCause();
                }
            });
        }

        /**
         * Each entry request uses the group UUID in the protocol and does not guess by string or deployment path.
         *
         * @param request request
         * @return the operation result
         */
        private static org.apache.ratis.protocol.RaftGroupId group(final RaftProtos.RaftRpcRequestProto request) {
            return org.apache.ratis.protocol.RaftGroupId.valueOf(request.getRaftGroupId().getId());
        }
    }
}
