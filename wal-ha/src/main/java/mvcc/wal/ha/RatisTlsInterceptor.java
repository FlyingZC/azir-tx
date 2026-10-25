package mvcc.wal.ha;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;
import org.apache.ratis.thirdparty.io.grpc.*;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.function.Predicate;

/** 认证握手之后逐条绑定组、接收者与发送者，原始远程管理与业务写入统一拒绝。 */
final class RatisTlsInterceptor implements ServerInterceptor {
    private final RatisTlsSecurity security;
    private final Predicate<String> activeMember;
    /** 成员资格取实际配置，旧成员不能靠尚未过期的 TLS 连接继续发送协议。 */
    RatisTlsInterceptor(RatisTlsSecurity security, Predicate<String> activeMember) { this.security = security; this.activeMember = activeMember; }

    /** 全部服务使用同一拦截器，缺失双向 TLS 会在请求解析前拒绝。 */
    @Override public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
        final X509Certificate certificate;
        try {
            if (!security.localIdentityValid()) { throw new java.security.cert.CertificateException("local TLS identity expired"); }
            var session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            if (session == null || !(session.getPeerCertificates()[0] instanceof X509Certificate leaf)) { throw new java.security.cert.CertificateException("TLS peer missing"); }
            certificate = leaf; security.authenticate(certificate);
        } catch (Exception failure) { call.close(Status.UNAUTHENTICATED.withDescription("authenticated WAL peer required"), new Metadata()); return new ServerCall.Listener<>() { }; }
        String service = call.getMethodDescriptor().getServiceName();
        if (service == null || !(service.endsWith(".RaftServerProtocolService") || service.endsWith(".RaftClientProtocolService"))) {
            call.close(Status.PERMISSION_DENIED.withDescription("use local WAL management API"), new Metadata()); return new ServerCall.Listener<>() { };
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
            private boolean denied;
            /** 流式日志与快照每个消息重新检查资格及指纹，不能只检查第一条。 */
            @Override public void onMessage(Q message) {
                try { if (!security.localIdentityValid()) { throw new java.security.cert.CertificateException("local TLS identity expired"); } validate(message, security.authenticate(certificate)); }
                catch (Exception failure) { denied = true; call.close(Status.PERMISSION_DENIED.withDescription("WAL RPC identity or operation rejected"), new Metadata()); return; }
                if (!denied) { super.onMessage(message); }
            }
            /** 拒绝后不能再把半关闭通知交给可能执行管理动作的服务。 */
            @Override public void onHalfClose() { if (!denied) { super.onHalfClose(); } }
        };
    }

    /** 协议消息本身携带的身份必须与已经认证的叶证书匹配。 */
    private void validate(Object message, String peer) throws java.security.cert.CertificateException {
        if (!activeMember.test(peer)) { throw new java.security.cert.CertificateException("peer outside current membership"); }
        RaftProtos.RaftRpcRequestProto rpc = null; boolean server = true;
        if (message instanceof RaftProtos.RaftClientRequestProto client) {
            server = false; rpc = client.getRpcRequest();
            if (!client.hasStaleRead() || !client.hasMessage() || !Arrays.equals(client.getMessage().getContent().toByteArray(), RatisReplicaStatus.request())) { throw new java.security.cert.CertificateException("remote business request prohibited"); }
        } else if (message instanceof Message proto) {
            for (var value : proto.getAllFields().values()) { if (value instanceof RaftProtos.RaftRpcRequestProto request) { if (rpc != null) { throw new java.security.cert.CertificateException("ambiguous RPC identity"); } rpc = request; } }
        }
        if (rpc == null || !rpc.hasRaftGroupId() || !RaftGroupId.valueOf(rpc.getRaftGroupId().getId()).getUuid().equals(security.group())
                || !rpc.getReplyId().toStringUtf8().equals(security.node()) || server && !rpc.getRequestorId().toStringUtf8().equals(peer)) { throw new java.security.cert.CertificateException("RPC identity mismatch"); }
    }
}
