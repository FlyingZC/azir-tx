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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;

import java.util.UUID;

/**
 * The release of the authentication mode tag retry must be confirmed by durable and does not allow for express downgrading after an uncertain synchronisation.
 */
class RatisTransportIdentityIoTest {

    @TempDir
    Path directory;

    /**
     * Atomic moves that have taken place cannot be deleted because of sync anomalies, and retry performs the same for real files and directories.
     *
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(RatisPolicyIoTest.Failure.class)
    void authenticationMarkerRetriesEveryIoFailureWithoutDowngrade(final RatisPolicyIoTest.Failure failure) throws Exception {
        UUID group = UUID.randomUUID();
        var owner = new WalReplicaIdentity(group, group, "n0");
        var broken = new RatisPolicyIoTest.Fault(failure);
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, broken));
        assertTrue(broken.fired);
        Path marker = directory.resolve("AUTHENTICATED");
        byte[] previous = Files.exists(marker) ? Files.readAllBytes(marker) : null;
        if (previous != null) {
            assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false));
        }
        var retry = new RatisPolicyIoTest.Fault(null);
        RatisTransportIdentity.persist(directory, owner, retry);
        assertTrue(retry.operations.contains("FILE_FORCE"));
        assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
        RatisTransportIdentity.verify(directory, owner, true);
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false));
        if (previous != null) {
            assertArrayEquals(previous, Files.readAllBytes(marker));
            assertFalse(retry.operations.contains("WRITE"));
            assertFalse(retry.operations.contains("MOVE"));
        }
    }

    /**
     * Synchronization may still fail when the complete tag already exists and cannot be successfully returned by verify.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void existingAuthenticationMarkerCannotSkipAnotherSyncFailure() throws Exception {
        UUID group = UUID.randomUUID();
        var owner = new WalReplicaIdentity(group, group, "n0");
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, new RatisPolicyIoTest.Fault(RatisPolicyIoTest.Failure.BEFORE_DIRECTORY_FORCE)));
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, new RatisPolicyIoTest.Fault(RatisPolicyIoTest.Failure.BEFORE_FILE_FORCE)));
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false));
        RatisTransportIdentity.persist(directory, owner);
    }
}
