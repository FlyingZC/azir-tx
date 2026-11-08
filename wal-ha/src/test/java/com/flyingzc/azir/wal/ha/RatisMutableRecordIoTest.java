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
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.stream.Stream;

/**
 * Active pointer and retirement progress true part writing, atomic replacement, deletion and directory confirmation failure.
 */
class RatisMutableRecordIoTest {

    @TempDir
    Path directory;

    /**
     * Replace borders to cover the integrity of old and new records.
     *
     * @return the operation result
     */
    static Stream<Object[]> publications() {
        return Arrays.stream(Kind.values()).flatMap(kind -> Stream.of(Failure.PARTIAL_WRITE, Failure.BEFORE_FILE_FORCE, Failure.AFTER_FILE_FORCE, Failure.BEFORE_REPLACE, Failure.AFTER_REPLACE,
                Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE).map(failure -> new Object[]{kind, failure}));
    }

    /**
     * Delete the need to keep retry directories synchronized.
     *
     * @return the operation result
     */
    static Stream<Object[]> removals() {
        return Arrays.stream(Kind.values())
                .flatMap(kind -> Stream.of(Failure.BEFORE_DELETE, Failure.AFTER_DELETE, Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE).map(failure -> new Object[]{kind, failure}));
    }

    /**
     * After any failure, the official record remains a complete old or new value and retry must confirm the target ' s new value.
     *
     * @param kind kind
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("publications")
    void atomicReplacementNeverExposesPartialRecord(final Kind kind, final Failure failure) throws Exception {
        var owner = owner();
        Path target = directory.resolve(kind == Kind.GENERATION ? "ACTIVE" : "RETIRING");
        publish(kind, target, owner, false, new Fault(null));
        var broken = new Fault(failure);
        assertThrows(IOException.class, () -> publish(kind, target, owner, true, broken));
        assertTrue(broken.fired);
        Assertions.assertEquals(failure == Failure.AFTER_REPLACE || failure == Failure.BEFORE_DIRECTORY_FORCE || failure == Failure.AFTER_DIRECTORY_FORCE, isNew(kind, target));
        var retry = new Fault(null);
        publish(kind, target, owner, true, retry);
        assertTrue(isNew(kind, target));
        assertTrue(retry.operations.contains("FILE_FORCE"));
        assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
    }

    /**
     * The records that have been deleted must also be synchronized again, and the second sync failure cannot be short-circuited.
     *
     * @param kind kind
     * @param failure failure
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("removals")
    void uncertainDeletionStillRequiresDirectorySyncOnRetry(final Kind kind, final Failure failure) throws Exception {
        Path target = directory.resolve(kind == Kind.GENERATION ? "INSTALLING" : "RETIRING");
        publish(kind, target, owner(), false, new Fault(null));
        var broken = new Fault(failure);
        assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target, broken));
        assertTrue(broken.fired);
        Assertions.assertEquals(failure == Failure.BEFORE_DELETE, Files.exists(target));
        var another = new Fault(Failure.BEFORE_DIRECTORY_FORCE);
        assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target, another));
        assertTrue(another.fired);
        assertFalse(Files.exists(target));
        var retry = new Fault(null);
        RatisGenerationRecord.remove(target, retry);
        assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
    }

    /**
     * Variable records cannot replace or delete a user file to which the link is directed.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void linksCannotRedirectRecordUpdateOrDeletion() throws Exception {
        Path user = directory.resolve("user-note"), target = directory.resolve("ACTIVE");
        Files.writeString(user, "keep");
        Files.createSymbolicLink(target, user);
        assertThrows(IOException.class, () -> publish(Kind.GENERATION, target, owner(), true, new Fault(null)));
        assertThrows(IOException.class, () -> RatisGenerationRecord.remove(target));
        Assertions.assertEquals("keep", Files.readString(user));
        assertTrue(Files.isSymbolicLink(target));
    }

    /**
     * This temporary directory is unique to identity.
     *
     * @return the operation result
     */
    private static WalReplicaIdentity owner() {
        UUID group = UUID.randomUUID();
        return new WalReplicaIdentity(group, group, "n0");
    }

    /**
     * Official records coding and release path determination data, testing without hand-made formal bytes.
     *
     * @param kind kind
     * @param target target
     * @param owner owner
     * @param newer newer
     * @param io io
     * @throws IOException if the operation cannot complete
     */
    private static void publish(final Kind kind, final Path target, final WalReplicaIdentity owner, final boolean newer, final RatisRecordFile.Io io) throws IOException {
        var record = new RatisGenerationRecord(owner, new UUID(1, 2), 3, newer && kind == Kind.GENERATION ? 11 : 10, newer && kind == Kind.GENERATION ? 2 : 1,
                (newer && kind == Kind.GENERATION ? "b" : "a").repeat(64));
        if (kind == Kind.GENERATION) {
            record.publish(target, io);
        } else {
            new RatisRetirementRecord(record, newer ? "done" : "", newer).publish(target, io);
        }
    }

    /**
     * The formal decoded validate ownership blocks and records CRC, semi-recordings inevitably lead to a failure of the assertion.
     *
     * @param kind kind
     * @param target target
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static boolean isNew(final Kind kind, final Path target) throws IOException {
        return kind == Kind.GENERATION ? RatisGenerationRecord.read(target).sequence() == 2 : RatisRetirementRecord.read(target).backendDone();
    }

    /**
     * Both types of records remain in their original format and cannot be replaced by one test.
     */
    enum Kind {
        GENERATION, RETIREMENT
    }

    /**
     * Pre- and post-operation anomalies indicate failure and real side effects may have occurred.
     */
    enum Failure {
        PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_REPLACE, AFTER_REPLACE, BEFORE_DELETE, AFTER_DELETE, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE
    }

    /**
     * One-time failure packaging real I/O, without a false file status simulation renaming.
     */
    static final class Fault implements RatisRecordFile.Io {

        final Failure failure;

        final List<String> operations = new ArrayList<>();

        boolean fired;

        /**
         * No failure object for observation did indeed occur to resynchronize.
         *
         * @param failure failure
         */
        Fault(final Failure failure) {
            this.failure = failure;
        }

        /**
         * Saves after-action errors in a state where they actually occurred.
         *
         * @param point point
         * @throws IOException if the operation cannot complete
         */
        private void hit(final Failure point) throws IOException {
            if (!fired && point == failure) {
                fired = true;
                throw new IOException("injected mutable record IO: " + point);
            }
        }

        /**
         * Private temporary storage has been stopped half-way after writing, and official old documents are still complete.
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
            RatisRecordFile.Io.super.write(channel, bytes);
        }

        /**
         * The file synchronises with different uncertainties.
         *
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceFile(final FileChannel channel) throws IOException {
            operations.add("FILE_FORCE");
            hit(Failure.BEFORE_FILE_FORCE);
            RatisRecordFile.Io.super.forceFile(channel);
            hit(Failure.AFTER_FILE_FORCE);
        }

        /**
         * The real atomic cannot be replaced with a new record as if it had not occurred.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void replace(final Path source, final Path target) throws IOException {
            operations.add("REPLACE");
            hit(Failure.BEFORE_REPLACE);
            RatisRecordFile.Io.super.replace(source, target);
            hit(Failure.AFTER_REPLACE);
        }

        /**
         * The error is thrown after the actual deletion, and the next time it must be observed and confirmed.
         *
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void delete(final Path target) throws IOException {
            operations.add("DELETE");
            hit(Failure.BEFORE_DELETE);
            RatisRecordFile.Io.super.delete(target);
            hit(Failure.AFTER_DELETE);
        }

        /**
         * Verify More key direct committee check to use a single bar in query.
         *
         * @param parent parent
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceDirectory(final Path parent) throws IOException {
            operations.add("DIRECTORY_FORCE");
            hit(Failure.BEFORE_DIRECTORY_FORCE);
            RatisRecordFile.Io.super.forceDirectory(parent);
            hit(Failure.AFTER_DIRECTORY_FORCE);
        }
    }
}
