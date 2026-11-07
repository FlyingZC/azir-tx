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

/** 保留官方 gRPC 传输，在完整快照成功回复前补上应用所需的持久性栅栏。 */
public final class DurableGrpcRpcType implements RpcType {
    private static final String CAMPAIGN_PARAMETER = "azir.wal.campaign.guard";
    /** 本机竞选资格来自可信装配，不能通过网络参数更改。 */
    static void setCampaignGuard(Parameters parameters, java.util.function.BooleanSupplier guard) { parameters.put(CAMPAIGN_PARAMETER, guard, java.util.function.BooleanSupplier.class); }
    /** Ratis 通过公开无参构造反射加载自定义 RPC 类型。 */
    public DurableGrpcRpcType() { }

    /** 类型名可由 RpcType.valueOf 重新实例化，不覆盖官方 GRPC 名称。 */
    @Override public String name() { return DurableGrpcRpcType.class.getName(); }

    /** 共识、日志追加、选举、客户端和认证仍使用官方实现。 */
    @Override public RpcFactory newFactory(Parameters parameters) { return new Factory(parameters); }

    /** 仅代理入站服务器协议的快照完成回复，不改写 Ratis 的选举或日志协议。 */
    private static final class Factory extends GrpcFactory {
        private org.apache.ratis.grpc.server.GrpcServicesImpl service;
        private final java.util.function.BooleanSupplier campaign;
        /** 原参数包含将来的 TLS 设置，按官方工厂完整传递。 */
        Factory(Parameters parameters) { super(parameters); var configured = parameters == null ? null : parameters.get(CAMPAIGN_PARAMETER, java.util.function.BooleanSupplier.class); campaign = configured == null ? () -> true : configured; }

        /** 官方追加器需要具体 gRPC 服务，保留其原始流式传输实现。 */
        @Override public org.apache.ratis.server.leader.LogAppender newLogAppender(RaftServer.Division division,
                org.apache.ratis.server.leader.LeaderState leader, org.apache.ratis.server.leader.FollowerInfo follower) {
            return new org.apache.ratis.grpc.server.GrpcLogAppender(division, leader, follower) {
                /** 仅绕过出站接口代理的类型转换，入站持久性栅栏仍由服务的服务器代理执行。 */
                @Override public org.apache.ratis.grpc.server.GrpcServicesImpl getServerRpc() { return service; }
            };
        }

        /** 成功完成的单文件快照须有可恢复持久前缀；失败传播为 RPC 失败。 */
        @Override public GrpcServices newRaftServerRpc(RaftServer server) {
            var proxy = (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[]{RaftServer.class}, (wrapper, method, arguments) -> {
                if (method.getDeclaringClass() == Object.class && method.getName().equals("equals")) { return wrapper == arguments[0]; }
                if (method.getDeclaringClass() == Object.class && method.getName().equals("hashCode")) { return System.identityHashCode(wrapper); }
                if (method.getName().equals("startLeaderElection") && !campaign.getAsBoolean()) { throw new java.io.IOException("backend health campaign gate closed"); }
                if (method.getName().equals("setConfigurationAsync")) {
                    // 对外原始改配置不能绕过追平和完整身份校验，正式管理统一调用本机框架接口。
                    return java.util.concurrent.CompletableFuture.failedFuture(new java.io.IOException("use WAL membership replacement API"));
                }
                Object result;
                try { result = method.invoke(server, arguments); } catch (InvocationTargetException failure) { throw failure.getCause(); }
                if (method.getName().equals("requestVote") && arguments[0] instanceof RaftProtos.RequestVoteRequestProto request) {
                    RaftDurability.current(server, group(request.getServerRequest()));
                }
                if (method.getName().equals("appendEntriesAsync") && arguments[0] instanceof RaftProtos.AppendEntriesRequestProto request
                        && result instanceof java.util.concurrent.CompletableFuture<?> future) {
                    return future.thenApply(reply -> {
                        try {
                            var group = group(request.getServerRequest());
                            if (reply instanceof RaftProtos.AppendEntriesReplyProto response && response.getResult() == RaftProtos.AppendEntriesReplyProto.AppendResult.SUCCESS
                                    && server.getDivision(group).getStateMachine() instanceof RatisWalStateMachine machine) { machine.durableSnapshotBaseForAppend(); }
                            RaftDurability.current(server, group); return reply;
                        } catch (java.io.IOException failure) { throw new java.util.concurrent.CompletionException(failure); }
                    });
                }
                if (method.getName().equals("installSnapshot") && arguments[0] instanceof RaftProtos.InstallSnapshotRequestProto request
                        && request.hasSnapshotChunk() && request.getSnapshotChunk().getDone()
                        && result instanceof RaftProtos.InstallSnapshotReplyProto reply && reply.getResult() == RaftProtos.InstallSnapshotResult.SUCCESS) {
                    var group = group(request.getServerRequest());
                    if (!(server.getDivision(group).getStateMachine() instanceof RatisWalStateMachine machine)) { throw new java.io.IOException("durable snapshot guard requires WAL state machine"); }
                    machine.durableSnapshotReceived(TermIndex.valueOf(request.getSnapshotChunk().getTermIndex()));
                }
                return result;
            });
            service = (org.apache.ratis.grpc.server.GrpcServicesImpl) super.newRaftServerRpc(proxy);
            return (GrpcServices) Proxy.newProxyInstance(GrpcServices.class.getClassLoader(), new Class<?>[]{GrpcServices.class}, (wrapper, method, arguments) -> {
                if (method.getDeclaringClass() == Object.class && method.getName().equals("equals")) { return wrapper == arguments[0]; }
                if (method.getDeclaringClass() == Object.class && method.getName().equals("hashCode")) { return System.identityHashCode(wrapper); }
                // 发出选举请求之前同步自己的任期和自投票目录项，不能只保护远端选票。
                if (method.getName().equals("requestVote") && arguments[0] instanceof RaftProtos.RequestVoteRequestProto request) { if (!campaign.getAsBoolean()) { throw new java.io.IOException("backend health campaign gate closed"); } RaftDurability.current(server, group(request.getServerRequest())); }
                try { return method.invoke(service, arguments); } catch (InvocationTargetException failure) { throw failure.getCause(); }
            });
        }

        /** 每种入站请求都使用协议中的组 UUID，不通过字符串或部署路径猜测。 */
        private static org.apache.ratis.protocol.RaftGroupId group(RaftProtos.RaftRpcRequestProto request) { return org.apache.ratis.protocol.RaftGroupId.valueOf(request.getRaftGroupId().getId()); }
    }
}
