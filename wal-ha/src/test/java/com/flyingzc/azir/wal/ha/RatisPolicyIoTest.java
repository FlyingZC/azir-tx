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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.stream.Stream;

/**
 * Uncertainty results of true parts written, synchronized and renamed by three control strategies cannot skip retry as a file.
 */
class RatisPolicyIoTest {

    @TempDir
    Path directory;

    /**
     * Implements the same external nonvariant for all policies and all I/O borders.
     *
     * @return the operation result
     */
    static Stream<Object[]> cases() {
        return Stream.of(RatisPolicyFile.Kind.HEALTH, RatisPolicyFile.Kind.SNAPSHOT, RatisPolicyFile.Kind.GC)
                .flatMap(kind -> Arrays.stream(Failure.values()).map(failure -> new Object[]{kind, failure}));
    }

    /**
     * Clears the private semi-written input safely when there is no complete official record, and retry must be synchronized again without changing content when a full record is available.
     *
     * @param kind kind
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("cases")
    void failuresNeverBecomeSuccessUntilRealFileAndDirectorySync(final RatisPolicyFile.Kind kind, final Failure failure) throws Exception {
        var owner = owner();
        var broken = new Fault(failure);
        Files.writeString(directory.resolve("user-note"), "keep");
        assertThrows(IOException.class, () -> publish(kind, owner, broken));
        assertTrue(broken.fired);
        Path target = directory.resolve(kind.file);
        boolean present = Files.exists(target);
        byte[] before = present ? Files.readAllBytes(target) : null;
        Object inode = present ? Files.readAttributes(target, java.nio.file.attribute.BasicFileAttributes.class).fileKey() : null;
        Assertions.assertEquals(failure == Failure.AFTER_MOVE || failure == Failure.BEFORE_DIRECTORY_FORCE || failure == Failure.AFTER_DIRECTORY_FORCE, present);
        Assertions.assertEquals(present, loaded(kind, owner));
        var retry = new Fault(null);
        publish(kind, owner, retry);
        assertTrue(retry.operations.contains("FILE_FORCE"));
        assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
        assertTrue(loaded(kind, owner));
        if (present) {
            assertArrayEquals(before, Files.readAllBytes(target));
            Assertions.assertEquals(inode, Files.readAttributes(target, java.nio.file.attribute.BasicFileAttributes.class).fileKey());
            assertFalse(retry.operations.contains("WRITE"));
            assertFalse(retry.operations.contains("MOVE"));
        } else {
            assertTrue(retry.operations.contains("WRITE"));
            assertTrue(retry.operations.contains("MOVE"));
        }
        Assertions.assertEquals("keep", Files.readString(directory.resolve("user-note")));
        try (var files = Files.list(directory)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(kind.scratch)));
        }
    }

    /**
     * After the first renaming had taken place and the synchronisation failed, the second synchronisation failed without returning to success because of the existence of a complete file.
     *
     * @param kind kind
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = RatisPolicyFile.Kind.class, names = {"HEALTH", "SNAPSHOT", "GC"})
    void uncertainExistingRecordMustNotBypassSecondSyncFailure(final RatisPolicyFile.Kind kind) throws Exception {
        var owner = owner();
        assertThrows(IOException.class, () -> publish(kind, owner, new Fault(Failure.BEFORE_DIRECTORY_FORCE)));
        assertTrue(loaded(kind, owner));
        var retry = new Fault(Failure.BEFORE_FILE_FORCE);
        assertThrows(IOException.class, () -> publish(kind, owner, retry));
        assertTrue(retry.fired);
        assertTrue(loaded(kind, owner));
        publish(kind, owner, new Fault(null));
    }

    /**
     * The policy identity is the current new temporary directory.
     *
     * @return the operation result
     */
    private static WalReplicaIdentity owner() {
        UUID group = UUID.randomUUID();
        return new WalReplicaIdentity(group, group, "node");
    }

    /**
     * Use a policy format that is not independently modelled through the actual recording class code.
     *
     * @param kind kind
     * @param owner owner
     * @param io io
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private void publish(final RatisPolicyFile.Kind kind, final WalReplicaIdentity owner, final RatisPolicyFile.Io io) throws IOException {
        switch (kind) {
            case HEALTH -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).publish(directory, io);
            case SNAPSHOT -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).publish(directory, io);
            case GC -> new RatisGcPolicy(owner, RatisGcOptions.defaults()).publish(directory, io);
            default -> throw new IllegalArgumentException("not a maintenance policy");
        }
    }

    /**
     * Official decode verify CRC, identity, budget and complete records.
     *
     * @param kind kind
     * @param owner owner
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private boolean loaded(final RatisPolicyFile.Kind kind, final WalReplicaIdentity owner) throws IOException {
        return switch (kind) {
            case HEALTH -> new RatisHealthRecord(owner, RatisHealthOptions.defaults()).equals(RatisHealthRecord.load(directory, owner));
            case SNAPSHOT -> new RatisSnapshotPolicy(owner, RatisSnapshotOptions.defaults()).equals(RatisSnapshotPolicy.load(directory, owner));
            case GC -> new RatisGcPolicy(owner, RatisGcOptions.defaults()).equals(RatisGcPolicy.load(directory, owner));
            default -> throw new IllegalArgumentException("not a maintenance policy");
        };
    }

    /**
     * Failure occurs before the operation or after the actual operation has been completed, which cannot be inferred that the operation was not performed.
     */
    enum Failure {
        PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_MOVE, AFTER_MOVE, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE
    }

    /**
     * A single failure packaged real file system, and retry action records are independent of internal strategy judgement.
     */
    static final class Fault implements RatisPolicyFile.Io {

        final Failure failure;

        final List<String> operations = new ArrayList<>();

        boolean fired;

        /**
         * No failure instance is used to prove that retry actually executes the file and directory sync.
         *
         * @param failure failure
         */
        Fault(final Failure failure) {
            this.failure = failure;
        }

        /**
         * Exact once failure, after the operation if an error has been made to preserve the actual effect of the reservation.
         *
         * @param point point
         * @throws IOException if the operation cannot complete
         */
        private void hit(final Failure point) throws IOException {
            if (!fired && failure == point) {
                fired = true;
                throw new IOException("injected policy IO failure: " + point);
            }
        }

        /**
         * If one half of the true byte is reported, official documents should remain non-existent.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final FileChannel channel, final byte[] bytes) throws IOException {
            operations.add("WRITE");
            if (failure == Failure.PARTIAL_WRITE) {
                RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes, 0, bytes.length / 2));
                hit(Failure.PARTIAL_WRITE);
            }
            RatisPolicyFile.Io.super.write(channel, bytes);
        }

        /**
         * Back and forth do not cover call returns with unsynchronised and unsync results, respectively.
         *
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceFile(final FileChannel channel) throws IOException {
            operations.add("FILE_FORCE");
            hit(Failure.BEFORE_FILE_FORCE);
            RatisPolicyFile.Io.super.forceFile(channel);
            hit(Failure.AFTER_FILE_FORCE);
        }

        /**
         * Atomic Rename real execution, after an anomaly cannot be rollback or delete the full official record.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void move(final Path source, final Path target) throws IOException {
            operations.add("MOVE");
            hit(Failure.BEFORE_MOVE);
            RatisPolicyFile.Io.super.move(source, target);
            hit(Failure.AFTER_MOVE);
        }

        /**
         * Both private temporary clean-up and official release are synchronized with the same actual directory.
         *
         * @param root root
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceDirectory(final Path root) throws IOException {
            operations.add("DIRECTORY_FORCE");
            hit(Failure.BEFORE_DIRECTORY_FORCE);
            RatisPolicyFile.Io.super.forceDirectory(root);
            hit(Failure.AFTER_DIRECTORY_FORCE);
        }
    }
}
