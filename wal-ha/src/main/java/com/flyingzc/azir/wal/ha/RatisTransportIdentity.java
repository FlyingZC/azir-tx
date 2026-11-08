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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;

import java.util.zip.CRC32C;
import com.flyingzc.azir.wal.WalReplicaIdentity;

/**
 * Durable Record authentication mode, restarting cannot inadvertently downgrade the same replica to an express service.
 */
final class RatisTransportIdentity {

    /**
     * The tool class does not allow the creation of examples.
     */
    private RatisTransportIdentity() {
    }

    /**
     * Checks only before start-up and does not write a file when the root directory lock has not been obtained.
     *
     * @param directory directory
     * @param identity identity
     * @param authenticated authenticated
     * @throws IOException if the operation cannot complete
     */
    static void verify(final Path directory, final WalReplicaIdentity identity, final boolean authenticated) throws IOException {
        Path marker = directory.resolve("AUTHENTICATED");
        if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (!authenticated || !Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) || Files.size(marker) > 256) {
            throw new IOException("authenticated replica cannot downgrade transport");
        }
        byte[] actual = Files.readAllBytes(marker);
        if (!Arrays.equals(actual, encode(identity))) {
            throw new IOException("authenticated transport identity mismatch");
        }
    }

    /**
     * Durable is released after the root directory lock has been obtained and the correct tags have been synchronized. Starting failure cannot circumvent the ban on downgrading.
     *
     * @param directory directory
     * @param identity identity
     * @throws IOException if the operation cannot complete
     */
    static void persist(final Path directory, final WalReplicaIdentity identity) throws IOException {
        persist(directory, identity, RatisPolicyFile.REAL);
    }

    /**
     * I/O in the package injects a genuine sync and renaming protocol with an official authentication mark.
     *
     * @param directory directory
     * @param identity identity
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    static void persist(final Path directory, final WalReplicaIdentity identity, final RatisPolicyFile.Io io) throws IOException {
        verify(directory, identity, true);
        RatisPolicyFile.publish(directory, RatisPolicyFile.Kind.AUTHENTICATED, encode(identity), io);
    }

    /**
     * The fixed code contains the complete group and node identity, while CRC protects authentication mode version.
     *
     * @param identity identity
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static byte[] encode(final WalReplicaIdentity identity) throws IOException {
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x415A544C);
        out.writeInt(1);
        out.writeUTF(identity.engineId().toString());
        out.writeUTF(identity.groupId().toString());
        out.writeUTF(identity.nodeId());
        out.flush();
        var crc = new CRC32C();
        crc.update(bytes.toByteArray());
        out.writeInt((int) crc.getValue());
        out.flush();
        return bytes.toByteArray();
    }
}
