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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.UUID;

import java.util.concurrent.TimeUnit;

/**
 * Each receiving and inspection directory generates an independent short-term CA and node key without saving the test private key to the repository.
 */
final class RatisTlsTestCertificates {

    static final char[] PASSWORD = "wal-fixture".toCharArray();

    final Path directory, ca, trust;

    private final Path caKey;

    private long serial = 10;

    /**
     * Test CA only exists in this Junit private temporary directory.
     *
     * @param directory directory
     * @throws Exception if the operation cannot complete
     */
    RatisTlsTestCertificates(final Path directory) throws Exception {
        this.directory = directory;
        Files.createDirectories(directory);
        ca = directory.resolve("ca.pem");
        caKey = directory.resolve("ca.key");
        trust = directory.resolve("roots.p12");
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", caKey.toString(), "-out", ca.toString(), "-days", "2", "-subj",
                "/CN=WAL-Test-CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign");
        var roots = KeyStore.getInstance("PKCS12");
        roots.load(null, PASSWORD);
        roots.setCertificateEntry("ca", certificate(ca));
        try (var output = Files.newOutputStream(trust)) {
            roots.store(output, PASSWORD);
        }
    }

    /**
     * The independent node certificate carries the exact group URI, dual-use Eku and returnring host name San.
     *
     * @param group group
     * @param id id
     * @param address address
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    Entry issue(final UUID group, final String id, final String address) throws Exception {
        return issue(group, id, address, 2, "serverAuth,clientAuth", true);
    }

    /**
     * Negative receipt and inspection issues expired, misused or missing real certificates for identity URI.
     *
     * @param group group
     * @param id id
     * @param address address
     * @param days days
     * @param usages usages
     * @param identityUri identity uri
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    Entry issue(final UUID group, final String id, final String address, final int days, final String usages, final boolean identityUri) throws Exception {
        Path root = directory.resolve(id);
        Files.createDirectories(root);
        Path key = root.resolve("node.key"), csr = root.resolve("node.csr"), cert = root.resolve("node.pem"), identity = root.resolve("identity.p12"), extensions = root.resolve("extensions.conf");
        String host = RatisEndpoint.parse(address).host();
        String advertised = (host.contains(":") || host.matches("[0-9.]+") ? "IP:" : "DNS:") + host;
        Files.writeString(extensions, "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=" + usages + "\nsubjectAltName="
                + (identityUri ? "URI:urn:azir:wal:" + group + ":" + id + "," : "") + "IP:127.0.0.1,DNS:localhost," + advertised + "\n");
        run("openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes", "-keyout", key.toString(), "-out", csr.toString(), "-subj", "/CN=" + id);
        if (days < 0) {
            Path index = root.resolve("ca-index"), sequence = root.resolve("ca-serial"), config = root.resolve("ca.conf"), expiredExtensions = root.resolve("expired.conf");
            Files.writeString(index, "");
            Files.writeString(sequence, String.format("%02x%n", serial++));
            Files.writeString(config, "[ca]\ndefault_ca=wal\n[wal]\ndatabase=" + index + "\nserial=" + sequence + "\nnew_certs_dir=" + root + "\ncertificate=" + ca + "\nprivate_key=" + caKey
                    + "\ndefault_md=sha256\npolicy=policy\n[policy]\ncommonName=supplied\n");
            Files.writeString(expiredExtensions, "[expired]\n" + Files.readString(extensions));
            run("openssl", "ca", "-batch", "-notext", "-config", config.toString(), "-in", csr.toString(), "-out", cert.toString(), "-startdate", "20000101000000Z", "-enddate", "20010101000000Z",
                    "-extfile", expiredExtensions.toString(), "-extensions", "expired");
        } else {
            run("openssl", "x509", "-req", "-in", csr.toString(), "-CA", ca.toString(), "-CAkey", caKey.toString(), "-set_serial", Long.toString(serial++), "-out", cert.toString(), "-days",
                    Integer.toString(days), "-extfile", extensions.toString());
        }
        run("openssl", "pkcs12", "-export", "-inkey", key.toString(), "-in", cert.toString(), "-certfile", ca.toString(), "-name", "identity", "-out", identity.toString(), "-passout",
                "pass:wal-fixture");
        Files.setPosixFilePermissions(identity, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        return new Entry(id, address, identity, cert, key, certificate(cert));
    }

    /**
     * Validate issued certificates with the actual JDK X509 parser.
     *
     * @param path path
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private static X509Certificate certificate(final Path path) throws Exception {
        try (var input = Files.newInputStream(path)) {
            return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
        }
    }

    /**
     * Subprocesses generate only this test key, and timeout terminates only this own process.
     *
     * @param command command
     * @throws Exception if the operation cannot complete
     * @throws java.io.IOException if the operation cannot complete
     */
    private static void run(final String... command) throws Exception {
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new java.io.IOException("test certificate command timed out");
        }
        String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            throw new java.io.IOException("test certificate generation failed: " + output);
        }
    }

    /**
     * Local Certificates and authorized fingerprints are extracted from controlled documents and not obtained through the Internet.
     *
     * @param id id
     * @param address address
     * @param identity identity
     * @param certificateFile certificate file
     * @param keyFile key file
     * @param certificate certificate
     */
    record Entry(String id, String address, Path identity, Path certificateFile, Path keyFile, X509Certificate certificate) {

        /**
         * Generates a clear mandate description of the privacy key for each node.
         *
         * @return the operation result
         * @throws CertificateException if the operation cannot complete
         */
        RatisTlsSecurity.PeerTrust trust() throws CertificateException {
            return new RatisTlsSecurity.PeerTrust(id, address, Set.of(RatisTlsSecurity.fingerprint(certificate)));
        }
    }
}
