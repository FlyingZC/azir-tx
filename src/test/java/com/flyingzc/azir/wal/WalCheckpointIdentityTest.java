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

package com.flyingzc.azir.wal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.common.IsolationLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import static com.flyingzc.azir.wal.WalMvccRuntimeTest.*;

/**
 * Durable recovery identity is missing or damaged only by preserving evidence and refusing to start, and the initialisation of an empty engine cannot be mistaken.
 */
class WalCheckpointIdentityTest {

    @TempDir
    Path directory;

    /**
     * Compatible directory of check points not yet marked by ENGINE must also recognize missing non-empty CURRENT data.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingCurrentWithoutMarkerCannotDestroyCheckpointFiles() throws Exception {
        checkpoint();
        Files.delete(directory.resolve("CURRENT"));
        Files.deleteIfExists(directory.resolve("ENGINE"));
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
        assertFalse(Files.exists(directory.resolve("transactions.wal")));
    }

    /**
     * Even if the checkpoint file is removed, durable engine identity will prevent the old directory from being re-initiated.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void durableEngineMarkerPreventsEmptyReinitialization() throws Exception {
        checkpoint();
        Path saved = directory.resolve("saved-files");
        Files.createDirectory(saved);
        try (var files = Files.list(directory)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                if (name.startsWith("checkpoint-") || name.startsWith("receipts-")) {
                    Files.move(path, saved.resolve(name));
                }
            }
        }
        Files.delete(directory.resolve("CURRENT"));
        assertTrue(Files.exists(directory.resolve("ENGINE")));
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
    }

    /**
     * Original WAL is missing and cannot initialize a new empty log under the same durable identity.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingLegacyWalWithEngineMarkerFailsClosed() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
        }
        Files.delete(directory.resolve("transactions.wal"));
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
    }

    /**
     * ENGINE damage cannot be concealed by normal WAL, let alone cover the original tag to continue service.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void corruptEngineIdentityIsNotOverwritten() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
        }
        Path marker = directory.resolve("ENGINE");
        byte[] original = Files.readAllBytes(marker);
        byte[] bad = original.clone();
        bad[20] ^= 1;
        Files.write(marker, bad);
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
        Files.write(marker, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.stats().publishedSequence());
        }
    }

    /**
     * An empty file does not represent a new engine, which must be refused before rewrite the head when identity is in place.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void emptyLegacyWalWithIdentityFailsBeforeHeaderRewrite() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
        }
        Path wal = directory.resolve("transactions.wal");
        byte[] original = Files.readAllBytes(wal);
        Files.write(wal, new byte[0]);
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
        Files.write(wal, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.stats().publishedSequence());
        }
    }

    /**
     * The current endbook must fail when it is missing and cannot generate an empty tail through CREATE.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void missingCurrentTailIsNotCreatedAgain() throws Exception {
        checkpoint();
        Path tail;
        try (var files = Files.list(directory)) {
            tail = files.filter(Files::isDirectory).findFirst().orElseThrow().resolve("tail.wal");
        }
        byte[] original = Files.readAllBytes(tail);
        Files.delete(tail);
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        Assertions.assertEquals(before, hashes());
        assertFalse(Files.exists(tail));
        Files.write(tail, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.stats().publishedSequence());
        }
    }

    /**
     * CURRENT is not yet published and the old WAL is still in its entirety, allowing the unpublished candidate to be discarded. The normal crash recovery cannot be rejected by mistake.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void orphanCandidateWithValidLegacyWalRemainsRecoverable() throws Exception {
        UUID receipt;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            receipt = tx.id();
            runtime.put(tx, "a", "k", bytes("safe"));
            runtime.commit(tx);
        }
        Path candidate = directory.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectory(candidate);
        Files.write(candidate.resolve("snapshot.bin"), new byte[]{1, 2, 3});
        Files.write(candidate.resolve("tail.wal"), new byte[]{4, 5});
        Path archive = directory.resolve("receipts-" + UUID.randomUUID() + ".sst");
        Files.write(archive, new byte[]{6});
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.findCommitted(receipt).orElseThrow().commitSequence());
        }
        assertFalse(Files.exists(candidate));
        assertFalse(Files.exists(archive));
    }

    /**
     * An old WAL can be used to write durable identity in situ. The patch cannot change the UUID or commit history of the engine.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void validLegacyWalCanAcquireMissingEngineMarker() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
        }
        byte[] expected = Files.readAllBytes(directory.resolve("ENGINE"));
        Files.delete(directory.resolve("ENGINE"));
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.stats().publishedSequence());
        }
        assertArrayEquals(expected, Files.readAllBytes(directory.resolve("ENGINE")));
    }

    /**
     * Clears the candidate ' s list of recovery files outside the directory without following a symbol link.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void orphanCleanupDoesNotFollowGenerationSymlinks() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
        }
        Path external = directory.resolve("external-backup");
        Files.createDirectory(external);
        Files.write(external.resolve("snapshot.bin"), new byte[]{1});
        Files.write(external.resolve("tail.wal"), new byte[]{2});
        Path link = directory.resolve("checkpoint-" + UUID.randomUUID());
        Files.createSymbolicLink(link, external);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            Assertions.assertEquals(1, restored.stats().publishedSequence());
        }
        assertTrue(Files.exists(external.resolve("snapshot.bin")));
        assertTrue(Files.exists(external.resolve("tail.wal")));
        assertTrue(Files.isSymbolicLink(link));
    }

    /**
     * Finish a checkable transaction, then create the only recovery generation.
     */
    private void checkpoint() {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            write(runtime, "a", "k", "safe");
            assertTrue(runtime.checkpoint().created());
        }
    }

    /**
     * Checks all file contents before and after the failed start-up to prevent any recovery evidence that is still needed for the wrong branch to clean up.
     *
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    private Map<String, String> hashes() throws Exception {
        var hashes = new TreeMap<String, String>();
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                hashes.put(directory.relativize(path).toString(), WalCodec.digest(Files.readAllBytes(path)));
            }
        }
        return hashes;
    }
}
