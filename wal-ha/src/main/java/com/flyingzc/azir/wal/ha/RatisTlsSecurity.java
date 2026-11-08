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

import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.ratis.conf.Parameters;

import java.io.IOException;
import java.net.Socket;

/**
 * Double-direction TLS groups, nodes, certificates and endpoints are bound; configurations and keys are used only at the visible authentication entrance.
 */
public final class RatisTlsSecurity {

    private final UUID group;

    private final String node;

    private final X509Certificate localCertificate;

    private final GrpcTlsConfig tls;

    private volatile Map<String, PeerTrust> peers;

    /**
     * Both the key and CA use bounded PKCS12 files, and the password is not stored as a string or exposed to diagnosis.
     *
     * @param group group
     * @param node node
     * @param identity identity
     * @param identityPassword identity password
     * @param roots roots
     * @param rootsPassword roots password
     * @param peers peers
     * @return the operation result
     * @throws GeneralSecurityException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    public static RatisTlsSecurity load(final UUID group, final String node, final Path identity, final char[] identityPassword, final Path roots, final char[] rootsPassword,
                                        final Collection<PeerTrust> peers) throws IOException {
        try {
            var keys = store(identity, identityPassword, true);
            var trust = store(roots, rootsPassword, false);
            var aliases = Collections.list(keys.aliases());
            if (aliases.size() != 1 || !keys.isKeyEntry(aliases.getFirst())) {
                throw new GeneralSecurityException("exactly one private key required");
            }
            String alias = aliases.getFirst();
            var chain = keys.getCertificateChain(alias);
            if (chain == null || chain.length < 1 || chain.length > 8 || !(chain[0] instanceof X509Certificate leaf)) {
                throw new GeneralSecurityException("invalid identity certificate chain");
            }
            for (String entry : Collections.list(trust.aliases())) {
                if (trust.isKeyEntry(entry) || !(trust.getCertificate(entry) instanceof X509Certificate ca) || ca.getBasicConstraints() < 0) {
                    throw new GeneralSecurityException("trust store must contain only CA certificates");
                }
                ca.checkValidity();
            }
            if (trust.size() < 1 || trust.size() > 16) {
                throw new GeneralSecurityException("bounded explicit CA trust required");
            }
            var key = (PrivateKey) keys.getKey(alias, identityPassword);
            verifyPrivateKey(key, leaf);
            var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keys, identityPassword);
            var tmf = TrustManagerFactory.getInstance("PKIX");
            tmf.init(trust);
            var delegate = Arrays.stream(tmf.getTrustManagers()).filter(X509ExtendedTrustManager.class::isInstance).map(X509ExtendedTrustManager.class::cast).findFirst().orElseThrow();
            var keyManager = Arrays.stream(kmf.getKeyManagers()).filter(X509ExtendedKeyManager.class::isInstance).map(X509ExtendedKeyManager.class::cast).findFirst().orElseThrow();
            delegate.checkClientTrusted(Arrays.copyOf(chain, chain.length, X509Certificate[].class), leaf.getPublicKey().getAlgorithm());
            return new RatisTlsSecurity(group, node, leaf, keyManager, delegate, peers);
        } catch (final GeneralSecurityException | IllegalArgumentException failure) {
            throw new IOException("invalid WAL TLS credentials or identity policy", failure);
        }
    }

    /**
     * Initialization is the checking of your identity and fingerprints, which precludes setting up private keys for other nodes.
     *
     * @param group group
     * @param node node
     * @param local local
     * @param keys keys
     * @param trust trust
     * @param initial initial
     * @throws CertificateException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private RatisTlsSecurity(final UUID group, final String node, final X509Certificate local, final X509ExtendedKeyManager keys, final X509ExtendedTrustManager trust,
                             final Collection<PeerTrust> initial) throws CertificateException {
        this.group = Objects.requireNonNull(group);
        this.node = node;
        this.localCertificate = local;
        var catalog = new HashMap<String, PeerTrust>();
        for (var peer : initial) {
            if (catalog.put(peer.node(), peer) != null) {
                throw new IllegalArgumentException("duplicate trusted node");
            }
        }
        validateCatalog(catalog);
        peers = Map.copyOf(catalog);
        if (!node.equals(authenticate(local))) {
            throw new CertificateException("local certificate node mismatch");
        }
        tls = GrpcTlsConfig.newBuilder().setName("azir-wal-authenticated").setKeyManager(keys).setTrustManager(new PeerManager(trust)).setMutualTls(true)
                .setSslProvider(org.apache.ratis.thirdparty.io.netty.handler.ssl.SslProvider.JDK).setProtocols("TLSv1.3", "TLSv1.2").build();
    }

    /**
     * The operator first registers a new identity at all reserved nodes, then joins through a change of full membership. Restarting the configuration also has to save this policy.
     *
     * @param peer peer
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized void authorizePeer(final PeerTrust peer) {
        try {
            if (node.equals(peer.node()) && !peer.certificateSha256().contains(fingerprint(localCertificate))) {
                throw new IllegalArgumentException("running local certificate must remain authorized");
            }
        } catch (final CertificateException invalid) {
            throw new IllegalArgumentException("local certificate invalid", invalid);
        }
        var next = new HashMap<>(peers);
        var previous = next.put(peer.node(), peer);
        if (previous != null && !RatisEndpoint.parse(previous.address()).equals(RatisEndpoint.parse(peer.address()))) {
            throw new IllegalArgumentException("change endpoint with a fresh peer identity");
        }
        validateCatalog(next);
        peers = Map.copyOf(next);
    }

    /**
     * Local policy snapshot does not contain a private key or password for deployment configuration durable.
     *
     * @return the operation result
     */
    public Collection<PeerTrust> trustedPeers() {
        return List.copyOf(peers.values());
    }

    /**
     * Probe does not read key files or wait for the network.
     *
     * @return the operation result
     */
    public java.time.Instant certificateExpiresAt() {
        return localCertificate.getNotAfter().toInstant();
    }

    /**
     * Do not wait for the existing connection to be automatically disconnected.
     *
     * @return the operation result
     */
    boolean localIdentityValid() {
        try {
            localCertificate.checkValidity();
            return true;
        } catch (final CertificateException invalid) {
            return false;
        }
    }

    /**
     * CA and Certificate Rotation use an overlapping configuration to scroll and restart, and it is prohibited to use thermal increase interface as a wrong connection key.
     *
     * @param peer peer
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public synchronized void revokePeer(final String peer) {
        if (node.equals(peer)) {
            throw new IllegalArgumentException("cannot revoke running local identity");
        }
        var next = new HashMap<>(peers);
        next.remove(peer);
        peers = Map.copyOf(next);
    }

    /**
     * Ties the initial group and all endpoints, extra management and client port is not supported.
     *
     * @param configuration configuration
     * @param local local
     * @throws IOException if the operation cannot complete
     */
    void validate(final RaftGroup configuration, final RaftPeerId local) throws IOException {
        if (!group.equals(configuration.getGroupId().getUuid()) || !node.equals(local.toString()) || configuration.getPeer(local) == null || configuration.getPeers().size() < 3
                || configuration.getPeers().size() > 9) {
            throw new IOException("TLS group or local node mismatch");
        }
        try {
            new RatisMembershipView(-1, configuration.getPeers().stream().filter(peer -> peer.getStartupRole() == org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).toList(),
                    configuration.getPeers().stream().filter(peer -> peer.getStartupRole() == org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).toList());
        } catch (final IllegalArgumentException invalid) {
            throw new IOException("authenticated group requires three voters and bounded learners", invalid);
        }
        if (configuration.getPeers().stream().anyMatch(
                peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER && peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)) {
            throw new IOException("invalid authenticated startup role");
        }
        for (var peer : configuration.getPeers()) {
            requirePeer(peer);
        }
    }

    /**
     * New members must register an accurate endpoint in advance and do not accept CA valid but unauthorized nodes.
     *
     * @param peer peer
     * @throws IOException if the operation cannot complete
     */
    void requirePeer(final RaftPeer peer) throws IOException {
        var configured = peers.get(peer.getId().toString());
        if (configured == null || !RatisEndpoint.parse(configured.address()).equals(RatisEndpoint.parse(peer.getAddress()))
                || peer.getAdminAddress() != null && !peer.getAdminAddress().equals(peer.getAddress()) || peer.getClientAddress() != null && !peer.getClientAddress().equals(peer.getAddress())
                || peer.getDataStreamAddress() != null) {
            throw new IOException("member endpoint not authorized by TLS policy");
        }
    }

    /**
     * All official gRPC services share the same mandatory two-way authentication configuration, which does not leave an explicit sideline.
     *
     * @return the operation result
     */
    Parameters parameters() {
        var result = new Parameters();
        GrpcConfigKeys.TLS.setConf(result, tls);
        return result;
    }

    /**
     * Local certificate identity for disk mode tags, does not return the certificate or private key.
     *
     * @return the operation result
     */
    UUID group() {
        return group;
    }

    /**
     * Node identification is not a RPC declaration that can be provided by the user.
     *
     * @return the operation result
     */
    String node() {
        return node;
    }

    /**
     * SAN URI Accurate binding groups and nodes, the CA signature and fingerprints must be set up simultaneously.
     *
     * @param certificate certificate
     * @return the operation result
     * @throws CertificateException if the operation cannot complete
     */
    String authenticate(final X509Certificate certificate) throws CertificateException {
        certificate.checkValidity();
        if (certificate.getBasicConstraints() >= 0) {
            throw new CertificateException("CA certificate cannot authenticate a peer");
        }
        var eku = certificate.getExtendedKeyUsage();
        if (eku == null || !eku.containsAll(List.of("1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2"))) {
            throw new CertificateException("peer requires server and client EKU");
        }
        String prefix = "urn:azir:wal:" + group + ":";
        String id = null;
        var names = certificate.getSubjectAlternativeNames();
        if (names != null) {
            for (var name : names) {
                if (name.getFirst().equals(6) && name.get(1) instanceof String uri && uri.startsWith("urn:azir:wal:")) {
                    if (id != null || !uri.startsWith(prefix)) {
                        throw new CertificateException("ambiguous or foreign WAL certificate identity");
                    }
                    id = uri.substring(prefix.length());
                }
            }
        }
        if (id == null) {
            throw new CertificateException("WAL certificate identity missing");
        }
        var expected = peers.get(id);
        if (expected == null || !expected.certificateSha256().contains(fingerprint(certificate))) {
            throw new CertificateException("peer certificate not authorized");
        }
        return id;
    }

    /**
     * Visible certificate fingerprints can be extracted from controlled CA-issued products, without relying on the first network handshake.
     *
     * @param certificate certificate
     * @return the operation result
     * @throws CertificateException if the operation cannot complete
     */
    public static String fingerprint(final X509Certificate certificate) throws CertificateException {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
        } catch (final NoSuchAlgorithmException impossible) {
            throw new CertificateException(impossible);
        }
    }

    /**
     * Refusal to repeat peer and unbounded authorization lists, allowing a maximum of thirty-two current or pre-registered identity.
     *
     * @param catalog catalog
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void validateCatalog(final Map<String, PeerTrust> catalog) {
        if (catalog.isEmpty() || catalog.size() > 32 || catalog.values().stream().map(peer -> RatisEndpoint.parse(peer.address())).distinct().count() != catalog.size()) {
            throw new IllegalArgumentException("TLS peer catalog invalid or exceeds budget");
        }
    }

    /**
     * Key files may not be exposed by link or loose Unix privileges, and there is a fixed byte limit before and after reading.
     *
     * @param path path
     * @param password password
     * @param secret secret
     * @return the operation result
     * @throws GeneralSecurityException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     */
    private static KeyStore store(final Path path, final char[] password, final boolean secret) throws IOException, GeneralSecurityException {
        if (password == null || password.length == 0) {
            throw new IOException("explicit TLS store password required");
        }
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 1_048_576) {
            throw new IOException("TLS store missing or exceeds budget");
        }
        if (secret && Files.getFileStore(path).supportsFileAttributeView("posix")) {
            for (var permission : Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS)) {
                if (permission.name().startsWith("GROUP_") || permission.name().startsWith("OTHERS_")) {
                    throw new IOException("private key store permissions must be owner only");
                }
            }
        }
        var result = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] bytes = input.readNBytes(1_048_577);
            try {
                if (bytes.length > 1_048_576) {
                    throw new IOException("TLS store grew beyond budget");
                }
                result.load(new java.io.ByteArrayInputStream(bytes), password);
            } finally {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        return result;
    }

    /**
     * Before you can start, challenge the verify private key to match a leaf certificate so that you do not find an error until the election handshake.
     *
     * @param key business key
     * @param certificate certificate
     * @throws GeneralSecurityException if the operation cannot complete
     */
    private static void verifyPrivateKey(final PrivateKey key, final X509Certificate certificate) throws GeneralSecurityException {
        if (key instanceof java.security.interfaces.RSAPrivateKey rsa && rsa.getModulus().bitLength() < 2048
                || key instanceof java.security.interfaces.ECPrivateKey ec && ec.getParams().getCurve().getField().getFieldSize() < 256) {
            throw new GeneralSecurityException("peer private key too weak");
        }
        String algorithm = switch (key.getAlgorithm()) {
            case "RSA" -> "SHA256withRSA";
            case "EC" -> "SHA256withECDSA";
            default -> throw new GeneralSecurityException("unsupported peer private key algorithm");
        };
        byte[] challenge = new byte[32];
        new SecureRandom().nextBytes(challenge);
        var signature = Signature.getInstance(algorithm);
        signature.initSign(key);
        signature.update(challenge);
        byte[] proof = signature.sign();
        signature.initVerify(certificate.getPublicKey());
        signature.update(challenge);
        if (!signature.verify(proof)) {
            throw new GeneralSecurityException("private key certificate mismatch");
        }
    }

    /**
     * A section, one end point, allows the current overlap with the next certificate and the signature of the certificate must remain exclusive CA verify.
     *
     * @param node node
     * @param address address
     * @param certificateSha256 certificate sha256
     */
    public record PeerTrust(String node, String address, Set<String> certificateSha256) {

        /**
         * Fingerprints must be clearly configured and cannot automatically trust remote certificates for the first time.
         *
         * @param node node
         * @param address address
         * @param certificateSha256 certificate sha256
         * @throws IllegalArgumentException if the operation cannot complete
         */
        public PeerTrust {
            if (node == null || !node.matches("[A-Za-z0-9._-]{1,64}")) {
                throw new IllegalArgumentException("invalid TLS node identity");
            }
            RatisEndpoint.parse(address);
            certificateSha256 = Set.copyOf(certificateSha256);
            if (certificateSha256.isEmpty() || certificateSha256.size() > 2 || certificateSha256.stream().anyMatch(value -> !value.matches("[a-f0-9]{64}"))) {
                throw new IllegalArgumentException("explicit peer certificate pins required");
            }
        }
    }

    /**
     * Tie the two nodes above PKIX verify, and check the endpoint of the given destination to prevent a credible node from impersonating another one.
     */
    private final class PeerManager extends X509ExtendedTrustManager {

        private final X509ExtendedTrustManager delegate;

        /**
         * Use standard PKIX validate for exclusive CA without closing hostname check.
         *
         * @param delegate delegate
         */
        PeerManager(final X509ExtendedTrustManager delegate) {
            this.delegate = delegate;
        }

        /**
         * A stop-in certificate must belong to the inner group node of a visible registration.
         *
         * @param chain chain
         * @param auth auth
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String auth) throws CertificateException {
            delegate.checkClientTrusted(chain, auth);
            authenticate(chain[0]);
        }

        /**
         * Nodes identity cannot be supported without a stop call from the target end, which is refused directly.
         *
         * @param chain chain
         * @param auth auth
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String auth) throws CertificateException {
            throw new CertificateException("server endpoint context required");
        }

        /**
         * Socket returns the validate context of the TLS standard.
         *
         * @param chain chain
         * @param auth auth
         * @param socket socket
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String auth, final Socket socket) throws CertificateException {
            delegate.checkClientTrusted(chain, auth, socket);
            authenticate(chain[0]);
        }

        /**
         * Socket mode also uses a logical purpose host in hand, without extrapolating identity to DNS.
         *
         * @param chain chain
         * @param auth auth
         * @param socket socket
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String auth, final Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, auth, socket);
            if (!(socket instanceof SSLSocket ssl) || ssl.getHandshakeSession() == null) {
                throw new CertificateException("TLS socket endpoint missing");
            }
            target(chain[0], ssl.getHandshakeSession().getPeerHost(), ssl.getHandshakeSession().getPeerPort());
        }

        /**
         * Netty uses the engine to turn back and two-way TLS verify still goes standard PKIX.
         *
         * @param chain chain
         * @param auth auth
         * @param engine engine
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkClientTrusted(final X509Certificate[] chain, final String auth, final SSLEngine engine) throws CertificateException {
            delegate.checkClientTrusted(chain, auth, engine);
            authenticate(chain[0]);
        }

        /**
         * The handshake of the exit station uses gRPC 's real target host and port to pre-register node.
         *
         * @param chain chain
         * @param auth auth
         * @param engine engine
         * @throws CertificateException if the operation cannot complete
         */
        @Override
        public void checkServerTrusted(final X509Certificate[] chain, final String auth, final SSLEngine engine) throws CertificateException {
            delegate.checkServerTrusted(chain, auth, engine);
            target(chain[0], engine.getPeerHost(), engine.getPeerPort());
        }

        /**
         * Only the standard CA issuer list is returned. Unregistered leaf certificates cannot be used as root.
         *
         * @return the operation result
         */
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }

        /**
         * Other valid nodes under the same CA cannot accept connection at the wrong address.
         *
         * @param certificate certificate
         * @param host host
         * @param port port
         * @throws CertificateException if the operation cannot complete
         */
        private void target(final X509Certificate certificate, final String host, final int port) throws CertificateException {
            String authenticated = authenticate(certificate);
            var expected = peers.get(authenticated);
            if (expected == null || !RatisEndpoint.of(host, port).equals(RatisEndpoint.parse(expected.address()))) {
                throw new CertificateException("authenticated server endpoint mismatch");
            }
        }
    }
}
