package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** TLS 凭据、组身份、指纹、端点和私钥权限在节点启动以前拒绝错误配置。 */
class RatisTlsConfigTest {
    @TempDir Path directory;
    /** 专属 CA、组 URI 和指纹匹配时可以装配，不能拿同一证书冒充其他节点。 */
    @Test void identityPinsAndGroupAreRequired() throws Exception {
        UUID group = UUID.randomUUID(); var pki = new RatisTlsTestCertificates(directory); var entry = pki.issue(group, "n0", "127.0.0.1:19001");
        var security = load(pki, entry, group, "n0", List.of(entry.trust())); assertEquals("n0", security.authenticate(entry.certificate()));
        assertThrows(IOException.class, () -> load(pki, entry, group, "n1", List.of(entry.trust())));
        assertThrows(IOException.class, () -> load(pki, entry, UUID.randomUUID(), "n0", List.of(entry.trust())));
        var wrong = new RatisTlsSecurity.PeerTrust("n0", entry.address(), Set.of("0".repeat(64))); assertThrows(IOException.class, () -> load(pki, entry, group, "n0", List.of(wrong)));
    }
    /** 相同名字的另一 CA 不能通过，错误密码及宽松密钥文件权限也不能进入节点启动。 */
    @Test void wrongCaPasswordAndPrivatePermissionsAreRejected() throws Exception {
        UUID group = UUID.randomUUID(); var pki = new RatisTlsTestCertificates(directory.resolve("first")); var other = new RatisTlsTestCertificates(directory.resolve("other")); var entry = pki.issue(group, "n0", "127.0.0.1:19001");
        assertThrows(IOException.class, () -> RatisTlsSecurity.load(group, "n0", entry.identity(), RatisTlsTestCertificates.PASSWORD, other.trust, RatisTlsTestCertificates.PASSWORD, List.of(entry.trust())));
        assertThrows(IOException.class, () -> RatisTlsSecurity.load(group, "n0", entry.identity(), "wrong".toCharArray(), pki.trust, RatisTlsTestCertificates.PASSWORD, List.of(entry.trust())));
        Files.setPosixFilePermissions(entry.identity(), java.nio.file.attribute.PosixFilePermissions.fromString("rw-r-----")); assertThrows(IOException.class, () -> load(pki, entry, group, "n0", List.of(entry.trust())));
    }
    /** 正式端点支持 DNS 与 IPv6，不能夹带用户信息、路径、空端口或通配绑定。 */
    @Test void endpointValidationAndTrustBudgetAreStrict() {
        assertEquals(new RatisEndpoint("db.example.com", 19001), RatisEndpoint.parse("DB.example.com:19001")); assertEquals(RatisEndpoint.parse("[::1]:19001"), RatisEndpoint.of("::1", 19001));
        for (String invalid : List.of("0.0.0.0:19001", "[::]:19001", "n0", "n0:0", "n0:65536", "user@n0:19001", "n0:19001/path", "n0:19001?x")) { assertThrows(IllegalArgumentException.class, () -> RatisEndpoint.parse(invalid)); }
        assertThrows(IllegalArgumentException.class, () -> new RatisTlsSecurity.PeerTrust("n0", "localhost:19001", Set.of()));
    }
    /** 本地授权可以为新节点预登记，已有身份不能改变端点或撤销正在使用的本机证书。 */
    @Test void explicitEnrollmentCannotChangeExistingIdentity() throws Exception {
        UUID group = UUID.randomUUID(); var pki = new RatisTlsTestCertificates(directory); var local = pki.issue(group, "n0", "127.0.0.1:19001"); var next = pki.issue(group, "n1", "127.0.0.1:19002"); var security = load(pki, local, group, "n0", List.of(local.trust()));
        security.authorizePeer(next.trust()); assertEquals("n1", security.authenticate(next.certificate()));
        assertThrows(IllegalArgumentException.class, () -> security.authorizePeer(new RatisTlsSecurity.PeerTrust("n1", "127.0.0.1:19003", next.trust().certificateSha256())));
        assertThrows(IllegalArgumentException.class, () -> security.authorizePeer(new RatisTlsSecurity.PeerTrust("n0", local.address(), Set.of("0".repeat(64)))));
        security.revokePeer("n1"); assertThrows(java.security.cert.CertificateException.class, () -> security.authenticate(next.certificate())); assertThrows(IllegalArgumentException.class, () -> security.revokePeer("n0"));
    }
    /** 过期、仅服务器用途或缺失组 URI 的证书即使有正确指纹也不能启动。 */
    @Test void expiredWrongUsageAndMissingIdentityAreRejected() throws Exception {
        UUID group = UUID.randomUUID(); var pki = new RatisTlsTestCertificates(directory);
        var expired = pki.issue(group, "expired", "localhost:19001", -1, "serverAuth,clientAuth", true);
        var wrongUsage = pki.issue(group, "usage", "localhost:19002", 2, "serverAuth", true);
        var missing = pki.issue(group, "missing", "localhost:19003", 2, "serverAuth,clientAuth", false);
        for (var entry : List.of(expired, wrongUsage, missing)) { assertThrows(IOException.class, () -> load(pki, entry, group, entry.id(), List.of(entry.trust()))); }
    }

    /** 认证模式文件损坏或身份不符不能降级，半写的私有暂存可在独占装配时有界清理。 */
    @Test void transportMarkerRejectsDowngradeCorruptionAndForeignIdentity() throws Exception {
        UUID group = UUID.randomUUID(); var identity = new com.flyingzc.azir.wal.WalReplicaIdentity(group, group, "n0"); Path scratch = directory.resolve(".authenticated-" + UUID.randomUUID()), note = directory.resolve("user-note");
        Files.writeString(scratch, "partial"); Files.writeString(note, "keep"); RatisTransportIdentity.persist(directory, identity); assertFalse(Files.exists(scratch)); assertEquals("keep", Files.readString(note));
        RatisTransportIdentity.verify(directory, identity, true); assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, identity, false));
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, new com.flyingzc.azir.wal.WalReplicaIdentity(group, group, "n1"), true));
        Path marker = directory.resolve("AUTHENTICATED"); byte[] corrupt = Files.readAllBytes(marker); corrupt[10] ^= 1; Files.write(marker, corrupt); assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, identity, true));
    }

    /** 保持所有拒绝发生在工厂创建数据库和本地 WAL 以前。 */
    private static RatisTlsSecurity load(RatisTlsTestCertificates pki, RatisTlsTestCertificates.Entry entry, UUID group, String node, Collection<RatisTlsSecurity.PeerTrust> trust) throws IOException { return RatisTlsSecurity.load(group, node, entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust); }
}
