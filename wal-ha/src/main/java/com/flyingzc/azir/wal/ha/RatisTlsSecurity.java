package com.flyingzc.azir.wal.ha;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.*;
import org.apache.ratis.protocol.*;
import javax.net.ssl.*;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;

/** 双向 TLS 的组、节点、证书和端点绑定；配置与密钥只在显式认证入口中使用。 */
public final class RatisTlsSecurity {
    private final UUID group;
    private final String node;
    private final X509Certificate localCertificate;
    private final GrpcTlsConfig tls;
    private volatile Map<String, PeerTrust> peers;

    /** 一节点一端点，允许当前与下一张证书重叠，证书签名仍须通过专属 CA 验证。 */
    public record PeerTrust(String node, String address, Set<String> certificateSha256) {
        /** 指纹必须显式配置，不能首次连接自动信任远端证书。 */
        public PeerTrust {
            if (node == null || !node.matches("[A-Za-z0-9._-]{1,64}")) { throw new IllegalArgumentException("invalid TLS node identity"); }
            RatisEndpoint.parse(address); certificateSha256 = Set.copyOf(certificateSha256);
            if (certificateSha256.isEmpty() || certificateSha256.size() > 2 || certificateSha256.stream().anyMatch(value -> !value.matches("[a-f0-9]{64}"))) { throw new IllegalArgumentException("explicit peer certificate pins required"); }
        }
    }

    /** 密钥和 CA 均使用有界 PKCS12 文件，密码不存为字符串或暴露在诊断中。 */
    public static RatisTlsSecurity load(UUID group, String node, Path identity, char[] identityPassword, Path roots, char[] rootsPassword, Collection<PeerTrust> peers) throws IOException {
        try {
            var keys = store(identity, identityPassword, true); var trust = store(roots, rootsPassword, false);
            var aliases = Collections.list(keys.aliases()); if (aliases.size() != 1 || !keys.isKeyEntry(aliases.getFirst())) { throw new GeneralSecurityException("exactly one private key required"); }
            String alias = aliases.getFirst(); var chain = keys.getCertificateChain(alias);
            if (chain == null || chain.length < 1 || chain.length > 8 || !(chain[0] instanceof X509Certificate leaf)) { throw new GeneralSecurityException("invalid identity certificate chain"); }
            for (String entry : Collections.list(trust.aliases())) {
                if (trust.isKeyEntry(entry) || !(trust.getCertificate(entry) instanceof X509Certificate ca) || ca.getBasicConstraints() < 0) { throw new GeneralSecurityException("trust store must contain only CA certificates"); }
                ca.checkValidity();
            }
            if (trust.size() < 1 || trust.size() > 16) { throw new GeneralSecurityException("bounded explicit CA trust required"); }
            var key = (PrivateKey) keys.getKey(alias, identityPassword); verifyPrivateKey(key, leaf);
            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); kmf.init(keys, identityPassword);
            var tmf = TrustManagerFactory.getInstance("PKIX"); tmf.init(trust);
            var delegate = Arrays.stream(tmf.getTrustManagers()).filter(X509ExtendedTrustManager.class::isInstance).map(X509ExtendedTrustManager.class::cast).findFirst().orElseThrow();
            var keyManager = Arrays.stream(kmf.getKeyManagers()).filter(X509ExtendedKeyManager.class::isInstance).map(X509ExtendedKeyManager.class::cast).findFirst().orElseThrow();
            delegate.checkClientTrusted(Arrays.copyOf(chain, chain.length, X509Certificate[].class), leaf.getPublicKey().getAlgorithm());
            return new RatisTlsSecurity(group, node, leaf, keyManager, delegate, peers);
        } catch (GeneralSecurityException | IllegalArgumentException failure) { throw new IOException("invalid WAL TLS credentials or identity policy", failure); }
    }

    /** 初始化即核对自己的身份和指纹，禁止配置其他节点的私钥启动。 */
    private RatisTlsSecurity(UUID group, String node, X509Certificate local, X509ExtendedKeyManager keys, X509ExtendedTrustManager trust, Collection<PeerTrust> initial) throws CertificateException {
        this.group = Objects.requireNonNull(group); this.node = node; this.localCertificate = local; var catalog = new HashMap<String, PeerTrust>();
        for (var peer : initial) { if (catalog.put(peer.node(), peer) != null) { throw new IllegalArgumentException("duplicate trusted node"); } }
        validateCatalog(catalog); peers = Map.copyOf(catalog); if (!node.equals(authenticate(local))) { throw new CertificateException("local certificate node mismatch"); }
        tls = GrpcTlsConfig.newBuilder().setName("azir-wal-authenticated").setKeyManager(keys).setTrustManager(new PeerManager(trust)).setMutualTls(true)
                .setSslProvider(org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.JDK).setProtocols("TLSv1.3", "TLSv1.2").build();
    }

    /** 操作者先在全部保留节点登记新身份，再通过正式成员变更加入，重启配置也须保存此策略。 */
    public synchronized void authorizePeer(PeerTrust peer) {
        try { if (node.equals(peer.node()) && !peer.certificateSha256().contains(fingerprint(localCertificate))) { throw new IllegalArgumentException("running local certificate must remain authorized"); } }
        catch (CertificateException invalid) { throw new IllegalArgumentException("local certificate invalid", invalid); }
        var next = new HashMap<>(peers); var previous = next.put(peer.node(), peer);
        if (previous != null && !RatisEndpoint.parse(previous.address()).equals(RatisEndpoint.parse(peer.address()))) { throw new IllegalArgumentException("change endpoint with a fresh peer identity"); }
        validateCatalog(next); peers = Map.copyOf(next);
    }

    /** 本地策略快照不含私钥或密码，供部署配置持久保存。 */
    public Collection<PeerTrust> trustedPeers() { return List.copyOf(peers.values()); }

    /** 本机证书到期供告警使用，探针不读取密钥文件或等待网络。 */
    public java.time.Instant certificateExpiresAt() { return localCertificate.getNotAfter().toInstant(); }

    /** 本机证书过期或授权不再匹配时立即取消业务就绪，不能等待现有连接自然断开。 */
    boolean localIdentityValid() { try { localCertificate.checkValidity(); return true; } catch (CertificateException invalid) { return false; } }

    /** CA 和证书轮换采用配置重叠后滚动重启，禁止把热更接口误当作连接密钥撤销。 */
    public synchronized void revokePeer(String peer) { if (node.equals(peer)) { throw new IllegalArgumentException("cannot revoke running local identity"); } var next = new HashMap<>(peers); next.remove(peer); peers = Map.copyOf(next); }

    /** 绑定初始组和全部端点，额外管理及客户端端口暂不支持。 */
    void validate(RaftGroup configuration, RaftPeerId local) throws IOException {
        if (!group.equals(configuration.getGroupId().getUuid()) || !node.equals(local.toString()) || configuration.getPeer(local) == null || configuration.getPeers().size() < 3 || configuration.getPeers().size() > 9) { throw new IOException("TLS group or local node mismatch"); }
        try { new RatisMembershipView(-1, configuration.getPeers().stream().filter(peer -> peer.getStartupRole() == org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).toList(), configuration.getPeers().stream().filter(peer -> peer.getStartupRole() == org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).toList()); }
        catch (IllegalArgumentException invalid) { throw new IOException("authenticated group requires three voters and bounded learners", invalid); }
        if (configuration.getPeers().stream().anyMatch(peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER && peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)) { throw new IOException("invalid authenticated startup role"); }
        for (var peer : configuration.getPeers()) { requirePeer(peer); }
    }

    /** 新成员必须提前登记精确端点，不接受 CA 有效但未经授权的节点。 */
    void requirePeer(RaftPeer peer) throws IOException {
        var configured = peers.get(peer.getId().toString());
        if (configured == null || !RatisEndpoint.parse(configured.address()).equals(RatisEndpoint.parse(peer.getAddress()))
                || peer.getAdminAddress() != null && !peer.getAdminAddress().equals(peer.getAddress()) || peer.getClientAddress() != null && !peer.getClientAddress().equals(peer.getAddress())
                || peer.getDataStreamAddress() != null) { throw new IOException("member endpoint not authorized by TLS policy"); }
    }

    /** 所有官方 gRPC 服务共用同一强制双向认证配置，不能留下明文旁路。 */
    Parameters parameters() { var result = new Parameters(); GrpcConfigKeys.TLS.setConf(result, tls); return result; }

    /** 本地证书身份供磁盘模式标记使用，不返回证书或私钥。 */
    UUID group() { return group; }
    /** 节点标识不是用户可提供的 RPC 声明。 */
    String node() { return node; }

    /** SAN URI 精确绑定组与节点，CA 签名和指纹必须同时成立。 */
    String authenticate(X509Certificate certificate) throws CertificateException {
        certificate.checkValidity(); if (certificate.getBasicConstraints() >= 0) { throw new CertificateException("CA certificate cannot authenticate a peer"); }
        var eku = certificate.getExtendedKeyUsage(); if (eku == null || !eku.containsAll(List.of("1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2"))) { throw new CertificateException("peer requires server and client EKU"); }
        String prefix = "urn:azir:wal:" + group + ":"; String id = null; var names = certificate.getSubjectAlternativeNames();
        if (names != null) { for (var name : names) { if (name.getFirst().equals(6) && name.get(1) instanceof String uri && uri.startsWith("urn:azir:wal:")) {
            if (id != null || !uri.startsWith(prefix)) { throw new CertificateException("ambiguous or foreign WAL certificate identity"); } id = uri.substring(prefix.length());
        } } }
        if (id == null) { throw new CertificateException("WAL certificate identity missing"); }
        var expected = peers.get(id); if (expected == null || !expected.certificateSha256().contains(fingerprint(certificate))) { throw new CertificateException("peer certificate not authorized"); } return id;
    }

    /** 显式证书指纹可从受控 CA 签发产物提取，不依赖第一次网络握手。 */
    public static String fingerprint(X509Certificate certificate) throws CertificateException {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded())); }
        catch (NoSuchAlgorithmException impossible) { throw new CertificateException(impossible); }
    }

    /** 拒绝重复端点和无界授权列表，最多允许三十二个当前或预登记身份。 */
    private static void validateCatalog(Map<String, PeerTrust> catalog) {
        if (catalog.isEmpty() || catalog.size() > 32 || catalog.values().stream().map(peer -> RatisEndpoint.parse(peer.address())).distinct().count() != catalog.size()) { throw new IllegalArgumentException("TLS peer catalog invalid or exceeds budget"); }
    }

    /** 密钥文件不可通过链接或宽松 Unix 权限暴露，读取前后也有固定字节上限。 */
    private static KeyStore store(Path path, char[] password, boolean secret) throws IOException, GeneralSecurityException {
        if (password == null || password.length == 0) { throw new IOException("explicit TLS store password required"); }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 1_048_576) { throw new IOException("TLS store missing or exceeds budget"); }
        if (secret && Files.getFileStore(path).supportsFileAttributeView("posix")) {
            for (var permission : Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)) { if (permission.name().startsWith("GROUP_") || permission.name().startsWith("OTHERS_")) { throw new IOException("private key store permissions must be owner only"); } }
        }
        var result = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) { byte[] bytes = input.readNBytes(1_048_577); try { if (bytes.length > 1_048_576) { throw new IOException("TLS store grew beyond budget"); } result.load(new java.io.ByteArrayInputStream(bytes), password); } finally { Arrays.fill(bytes, (byte) 0); } } return result;
    }

    /** 启动前挑战签名验证私钥与叶证书匹配，避免等到选举握手才发现错误。 */
    private static void verifyPrivateKey(PrivateKey key, X509Certificate certificate) throws GeneralSecurityException {
        if (key instanceof java.security.interfaces.RSAPrivateKey rsa && rsa.getModulus().bitLength() < 2048 || key instanceof java.security.interfaces.ECPrivateKey ec && ec.getParams().getCurve().getField().getFieldSize() < 256) { throw new GeneralSecurityException("peer private key too weak"); }
        String algorithm = switch (key.getAlgorithm()) { case "RSA" -> "SHA256withRSA"; case "EC" -> "SHA256withECDSA"; default -> throw new GeneralSecurityException("unsupported peer private key algorithm"); };
        byte[] challenge = new byte[32]; new SecureRandom().nextBytes(challenge); var signature = Signature.getInstance(algorithm); signature.initSign(key); signature.update(challenge); byte[] proof = signature.sign(); signature.initVerify(certificate.getPublicKey()); signature.update(challenge);
        if (!signature.verify(proof)) { throw new GeneralSecurityException("private key certificate mismatch"); }
    }

    /** 在 PKIX 验证之上绑定双方节点，出站还核对指定目的端点，杜绝可信节点冒充另一台。 */
    private final class PeerManager extends X509ExtendedTrustManager {
        private final X509ExtendedTrustManager delegate;
        /** 使用专属 CA 的标准 PKIX 校验，不关闭主机名检查。 */
        PeerManager(X509ExtendedTrustManager delegate) { this.delegate = delegate; }
        /** 入站证书必须属于显式登记的组内节点。 */
        @Override public void checkClientTrusted(X509Certificate[] chain, String auth) throws CertificateException { delegate.checkClientTrusted(chain, auth); authenticate(chain[0]); }
        /** 没有目标端点的出站回调无法证明节点身份，直接拒绝。 */
        @Override public void checkServerTrusted(X509Certificate[] chain, String auth) throws CertificateException { throw new CertificateException("server endpoint context required"); }
        /** Socket 回调保留标准 TLS 的校验上下文。 */
        @Override public void checkClientTrusted(X509Certificate[] chain, String auth, Socket socket) throws CertificateException { delegate.checkClientTrusted(chain, auth, socket); authenticate(chain[0]); }
        /** Socket 模式同样使用握手中的逻辑目的主机，不用反向 DNS 推断身份。 */
        @Override public void checkServerTrusted(X509Certificate[] chain, String auth, Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, auth, socket); if (!(socket instanceof SSLSocket ssl) || ssl.getHandshakeSession() == null) { throw new CertificateException("TLS socket endpoint missing"); }
            target(chain[0], ssl.getHandshakeSession().getPeerHost(), ssl.getHandshakeSession().getPeerPort());
        }
        /** Netty 使用引擎回调，双向 TLS 验证仍走标准 PKIX。 */
        @Override public void checkClientTrusted(X509Certificate[] chain, String auth, SSLEngine engine) throws CertificateException { delegate.checkClientTrusted(chain, auth, engine); authenticate(chain[0]); }
        /** 出站握手使用 gRPC 真实目标主机和端口定位预登记节点。 */
        @Override public void checkServerTrusted(X509Certificate[] chain, String auth, SSLEngine engine) throws CertificateException { delegate.checkServerTrusted(chain, auth, engine); target(chain[0], engine.getPeerHost(), engine.getPeerPort()); }
        /** 只返回标准 CA 发行者列表，不能把未登记叶证书当作根。 */
        @Override public X509Certificate[] getAcceptedIssuers() { return delegate.getAcceptedIssuers(); }
        /** 同一 CA 下的其他合法节点也不能在错误地址接受连接。 */
        private void target(X509Certificate certificate, String host, int port) throws CertificateException {
            String authenticated = authenticate(certificate); var expected = peers.get(authenticated);
            if (expected == null || !RatisEndpoint.of(host, port).equals(RatisEndpoint.parse(expected.address()))) { throw new CertificateException("authenticated server endpoint mismatch"); }
        }
    }
}
