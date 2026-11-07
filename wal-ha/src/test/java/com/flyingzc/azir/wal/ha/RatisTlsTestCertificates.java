package com.flyingzc.azir.wal.ha;

import java.nio.file.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** 每个验收目录生成独立短期 CA 与节点密钥，不把测试私钥保存到仓库。 */
final class RatisTlsTestCertificates {
    static final char[] PASSWORD = "wal-fixture".toCharArray();
    final Path directory, ca, trust;
    private final Path caKey;
    private long serial = 10;
    /** 测试 CA 仅在本次 JUnit 私有临时目录中存在。 */
    RatisTlsTestCertificates(Path directory) throws Exception {
        this.directory = directory; Files.createDirectories(directory); ca = directory.resolve("ca.pem"); caKey = directory.resolve("ca.key"); trust = directory.resolve("roots.p12");
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", caKey.toString(), "-out", ca.toString(), "-days", "2", "-subj", "/CN=WAL-Test-CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign");
        var roots = KeyStore.getInstance("PKCS12"); roots.load(null, PASSWORD); roots.setCertificateEntry("ca", certificate(ca)); try (var output = Files.newOutputStream(trust)) { roots.store(output, PASSWORD); }
    }
    /** 独立节点证书携带精确组 URI、双用途 EKU 和回环主机名 SAN。 */
    Entry issue(UUID group, String id, String address) throws Exception {
        return issue(group, id, address, 2, "serverAuth,clientAuth", true);
    }
    /** 负向验收签发过期、错误用途或缺失身份 URI 的真实证书。 */
    Entry issue(UUID group, String id, String address, int days, String usages, boolean identityUri) throws Exception {
        Path root = directory.resolve(id); Files.createDirectories(root); Path key = root.resolve("node.key"), csr = root.resolve("node.csr"), cert = root.resolve("node.pem"), identity = root.resolve("identity.p12"), extensions = root.resolve("extensions.conf");
        String host = RatisEndpoint.parse(address).host(); String advertised = (host.contains(":") || host.matches("[0-9.]+") ? "IP:" : "DNS:") + host;
        Files.writeString(extensions, "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=" + usages + "\nsubjectAltName=" + (identityUri ? "URI:urn:azir:wal:" + group + ":" + id + "," : "") + "IP:127.0.0.1,DNS:localhost," + advertised + "\n");
        run("openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", key.toString(), "-out", csr.toString(), "-subj", "/CN=" + id);
        if (days < 0) {
            Path index = root.resolve("ca-index"), sequence = root.resolve("ca-serial"), config = root.resolve("ca.conf"), expiredExtensions = root.resolve("expired.conf"); Files.writeString(index, ""); Files.writeString(sequence, String.format("%02x%n", serial++));
            Files.writeString(config, "[ca]\ndefault_ca=wal\n[wal]\ndatabase=" + index + "\nserial=" + sequence + "\nnew_certs_dir=" + root + "\ncertificate=" + ca + "\nprivate_key=" + caKey + "\ndefault_md=sha256\npolicy=policy\n[policy]\ncommonName=supplied\n");
            Files.writeString(expiredExtensions, "[expired]\n" + Files.readString(extensions));
            run("openssl", "ca", "-batch", "-notext", "-config", config.toString(), "-in", csr.toString(), "-out", cert.toString(), "-startdate", "20000101000000Z", "-enddate", "20010101000000Z", "-extfile", expiredExtensions.toString(), "-extensions", "expired");
        } else { run("openssl", "x509", "-req", "-in", csr.toString(), "-CA", ca.toString(), "-CAkey", caKey.toString(), "-set_serial", Long.toString(serial++), "-out", cert.toString(), "-days", Integer.toString(days), "-extfile", extensions.toString()); }
        run("openssl", "pkcs12", "-export", "-inkey", key.toString(), "-in", cert.toString(), "-certfile", ca.toString(), "-name", "identity", "-out", identity.toString(), "-passout", "pass:wal-fixture");
        Files.setPosixFilePermissions(identity, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); return new Entry(id, address, identity, cert, key, certificate(cert));
    }
    /** 本地签发证书和授权指纹均由受控文件提取，不通过网络获取。 */
    record Entry(String id, String address, Path identity, Path certificateFile, Path keyFile, X509Certificate certificate) {
        /** 为各节点生成无私钥的明确授权描述。 */
        RatisTlsSecurity.PeerTrust trust() throws CertificateException { return new RatisTlsSecurity.PeerTrust(id, address, Set.of(RatisTlsSecurity.fingerprint(certificate))); }
    }
    /** 使用真实 JDK X509 解析器验证签发产物。 */
    private static X509Certificate certificate(Path path) throws Exception { try (var input = Files.newInputStream(path)) { return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input); } }
    /** 子进程仅生成本次测试密钥，超时只终止此自有进程。 */
    private static void run(String... command) throws Exception {
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!process.waitFor(10, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new java.io.IOException("test certificate command timed out"); }
        String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (process.exitValue() != 0) { throw new java.io.IOException("test certificate generation failed: " + output); }
    }
}
