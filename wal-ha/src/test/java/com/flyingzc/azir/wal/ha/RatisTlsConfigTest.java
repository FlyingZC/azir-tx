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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;

/**
 * TLS receipt, group identity, fingerprints, endpoints and private key privileges are denied the wrong configuration before the node starts.
 */
class RatisTlsConfigTest {

    @TempDir
    Path directory;

    /**
     * The exclusive CA, group URI and fingerprint matching can be assembled and cannot be impersonated with the same certificate as any other node.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void identityPinsAndGroupAreRequired() throws Exception {
        UUID group = UUID.randomUUID();
        var pki = new RatisTlsTestCertificates(directory);
        var entry = pki.issue(group, "n0", "127.0.0.1:19001");
        var security = load(pki, entry, group, "n0", List.of(entry.trust()));
        Assertions.assertEquals("n0", security.authenticate(entry.certificate()));
        assertThrows(IOException.class, () -> load(pki, entry, group, "n1", List.of(entry.trust())));
        assertThrows(IOException.class, () -> load(pki, entry, UUID.randomUUID(), "n0", List.of(entry.trust())));
        var wrong = new RatisTlsSecurity.PeerTrust("n0", entry.address(), Set.of("0".repeat(64)));
        assertThrows(IOException.class, () -> load(pki, entry, group, "n0", List.of(wrong)));
    }

    /**
     * Another CA with the same name cannot pass, and error passwords and loose key file permissions cannot enter node to start.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void wrongCaPasswordAndPrivatePermissionsAreRejected() throws Exception {
        UUID group = UUID.randomUUID();
        var pki = new RatisTlsTestCertificates(directory.resolve("first"));
        var other = new RatisTlsTestCertificates(directory.resolve("other"));
        var entry = pki.issue(group, "n0", "127.0.0.1:19001");
        assertThrows(IOException.class,
                () -> RatisTlsSecurity.load(group, "n0", entry.identity(), RatisTlsTestCertificates.PASSWORD, other.trust, RatisTlsTestCertificates.PASSWORD, List.of(entry.trust())));
        assertThrows(IOException.class, () -> RatisTlsSecurity.load(group, "n0", entry.identity(), "wrong".toCharArray(), pki.trust, RatisTlsTestCertificates.PASSWORD, List.of(entry.trust())));
        Files.setPosixFilePermissions(entry.identity(), java.nio.file.attribute.PosixFilePermissions.fromString("rw-r-----"));
        assertThrows(IOException.class, () -> load(pki, entry, group, "n0", List.of(entry.trust())));
    }

    /**
     * The official endpoint supports DNS and IPv6, which cannot bind with user information, path, empty port or coding.
     */
    @Test
    void endpointValidationAndTrustBudgetAreStrict() {
        Assertions.assertEquals(new RatisEndpoint("db.example.com", 19001), RatisEndpoint.parse("DB.example.com:19001"));
        Assertions.assertEquals(RatisEndpoint.parse("[::1]:19001"), RatisEndpoint.of("::1", 19001));
        for (String invalid : List.of("0.0.0.0:19001", "[::]:19001", "n0", "n0:0", "n0:65536", "user@n0:19001", "n0:19001/path", "n0:19001?x")) {
            assertThrows(IllegalArgumentException.class, () -> RatisEndpoint.parse(invalid));
        }
        assertThrows(IllegalArgumentException.class, () -> new RatisTlsSecurity.PeerTrust("n0", "localhost:19001", Set.of()));
    }

    /**
     * Local is authorized to pre-register for new nodes, and identity cannot change the endpoint or revoke the current machine certificate in use.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void explicitEnrollmentCannotChangeExistingIdentity() throws Exception {
        UUID group = UUID.randomUUID();
        var pki = new RatisTlsTestCertificates(directory);
        var local = pki.issue(group, "n0", "127.0.0.1:19001");
        var next = pki.issue(group, "n1", "127.0.0.1:19002");
        var security = load(pki, local, group, "n0", List.of(local.trust()));
        security.authorizePeer(next.trust());
        Assertions.assertEquals("n1", security.authenticate(next.certificate()));
        assertThrows(IllegalArgumentException.class, () -> security.authorizePeer(new RatisTlsSecurity.PeerTrust("n1", "127.0.0.1:19003", next.trust().certificateSha256())));
        assertThrows(IllegalArgumentException.class, () -> security.authorizePeer(new RatisTlsSecurity.PeerTrust("n0", local.address(), Set.of("0".repeat(64)))));
        security.revokePeer("n1");
        assertThrows(java.security.cert.CertificateException.class, () -> security.authenticate(next.certificate()));
        assertThrows(IllegalArgumentException.class, () -> security.revokePeer("n0"));
    }

    /**
     * The expired, server-only or missing group URI certificates cannot be activated even if they have the correct fingerprints.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredWrongUsageAndMissingIdentityAreRejected() throws Exception {
        UUID group = UUID.randomUUID();
        var pki = new RatisTlsTestCertificates(directory);
        var expired = pki.issue(group, "expired", "localhost:19001", -1, "serverAuth,clientAuth", true);
        var wrongUsage = pki.issue(group, "usage", "localhost:19002", 2, "serverAuth", true);
        var missing = pki.issue(group, "missing", "localhost:19003", 2, "serverAuth,clientAuth", false);
        for (var entry : List.of(expired, wrongUsage, missing)) {
            assertThrows(IOException.class, () -> load(pki, entry, group, entry.id(), List.of(entry.trust())));
        }
    }

    /**
     * The authentication mode file is corrupted or identity mismatches cannot be downgraded, and semi-written private temporary storage can be cleared with bounded when the assembly is
     * monopolized.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void transportMarkerRejectsDowngradeCorruptionAndForeignIdentity() throws Exception {
        UUID group = UUID.randomUUID();
        var identity = new com.flyingzc.azir.wal.WalReplicaIdentity(group, group, "n0");
        Path scratch = directory.resolve(".authenticated-" + UUID.randomUUID()), note = directory.resolve("user-note");
        Files.writeString(scratch, "partial");
        Files.writeString(note, "keep");
        RatisTransportIdentity.persist(directory, identity);
        assertFalse(Files.exists(scratch));
        Assertions.assertEquals("keep", Files.readString(note));
        RatisTransportIdentity.verify(directory, identity, true);
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, identity, false));
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, new com.flyingzc.azir.wal.WalReplicaIdentity(group, group, "n1"), true));
        Path marker = directory.resolve("AUTHENTICATED");
        byte[] corrupt = Files.readAllBytes(marker);
        corrupt[10] ^= 1;
        Files.write(marker, corrupt);
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, identity, true));
    }

    /**
     * Keeps all rejections before factories create database and local WAL.
     *
     * @param pki pki
     * @param entry entry
     * @param group group
     * @param node node
     * @param trust trust
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static RatisTlsSecurity load(final RatisTlsTestCertificates pki, final RatisTlsTestCertificates.Entry entry, final UUID group, final String node,
                                         final Collection<RatisTlsSecurity.PeerTrust> trust) throws IOException {
        return RatisTlsSecurity.load(group, node, entry.identity(), RatisTlsTestCertificates.PASSWORD, pki.trust, RatisTlsTestCertificates.PASSWORD, trust);
    }
}
