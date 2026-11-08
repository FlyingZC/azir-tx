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

import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * The operator ' s instrument does not cover another operation after a genuine file system border has failed and can confirm that the same operation continues.
 */
class RatisReplacementTicketsIoTest {

    @TempDir
    Path directory;

    /**
     * Official documents can only be non-existent or complete, and successful preservation must reaffirm the document and directory.
     *
     * @param point point
     * @throws Exception if the operation cannot complete
     */
    @ParameterizedTest
    @EnumSource(Failure.class)
    void uncertainPublicationCanOnlyRetryTheOriginalTicket(final Failure point) throws Exception {
        var ticket = ticket(UUID.randomUUID());
        Path file = directory.resolve("replacement.ticket");
        var broken = new Fault(point);
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket, broken));
        assertTrue(broken.fired);
        if (Files.exists(file)) {
            assertArrayEquals(ticket.encode(), Files.readAllBytes(file));
        }
        var retry = new Fault(null);
        RatisReplacementTickets.save(file, ticket, retry);
        assertTrue(retry.operations.contains("FILE_FORCE"));
        assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
        assertArrayEquals(ticket.encode(), RatisReplacementTickets.load(file, ticket.groupId()).encode());
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket(ticket.groupId())));
        assertArrayEquals(ticket.encode(), Files.readAllBytes(file));
        try (var entries = Files.list(directory)) {
            assertTrue(entries.noneMatch(path -> path.getFileName().toString().startsWith(".azir-ticket-stage-")));
        }
    }

    /**
     * Retry cannot skip synchronisation again because official documents are already visible; damage, link and error groups are rejected.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void existingTicketStillNeedsConfirmationAndOwnership() throws Exception {
        var ticket = ticket(UUID.randomUUID());
        Path file = directory.resolve("ticket");
        RatisReplacementTickets.save(file, ticket);
        for (var point : List.of(Failure.BEFORE_FILE_FORCE, Failure.AFTER_FILE_FORCE, Failure.BEFORE_DIRECTORY_FORCE, Failure.AFTER_DIRECTORY_FORCE)) {
            var fault = new Fault(point);
            assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket, fault));
            assertTrue(fault.fired);
        }
        assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, UUID.randomUUID()));
        byte[] damaged = ticket.encode();
        damaged[12] ^= 1;
        Files.write(file, damaged);
        assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId()));
        Path user = directory.resolve("user");
        Files.writeString(user, "keep");
        Files.delete(file);
        Files.createSymbolicLink(file, user);
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, ticket));
        assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId()));
        Assertions.assertEquals("keep", Files.readString(user));
        Files.delete(file);
        Files.write(file, new byte[16_385]);
        assertThrows(IOException.class, () -> RatisReplacementTickets.load(file, ticket.groupId()));
    }

    /**
     * Different instruments that genuinely compete for the same target name can only retain winning operations and cannot be covered by busy people as they are.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void competingWriterCannotReplaceOriginalOperation() throws Exception {
        var first = ticket(UUID.randomUUID());
        var second = ticket(first.groupId());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Path file = directory.resolve("ticket");
        try (var executor = Executors.newSingleThreadExecutor()) {
            var writer = executor.submit(() -> {
                RatisReplacementTickets.save(file, first, new RatisReplacementTickets.Io() {

                    /**
                     * Keeps the file lock until another line tries to save different instruments.
                     *
                     * @param source database connection source
                     * @param target target
                     * @throws IOException if the operation cannot complete
                     */
                    @Override
                    public void link(final Path source, final Path target) throws IOException {
                        entered.countDown();
                        try {
                            if (!release.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("test release timed out");
                            }
                        } catch (final InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IOException(interrupted);
                        }
                        RatisReplacementTickets.Io.super.link(source, target);
                    }
                });
                return null;
            });
            try {
                assertTrue(entered.await(10, TimeUnit.SECONDS));
                assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, second));
            } finally {
                release.countDown();
            }
            writer.get(10, TimeUnit.SECONDS);
        }
        assertArrayEquals(first.encode(), RatisReplacementTickets.load(file, first.groupId()).encode());
        assertThrows(IOException.class, () -> RatisReplacementTickets.save(file, second));
    }

    /**
     * Legitimate instruments are created by a genuine constructor and do not bypass member validate.
     *
     * @param group group
     * @return the operation result
     */
    private static RatisMemberReplacement ticket(final UUID group) {
        var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("localhost:" + (12300 + i)).build());
        }
        UUID operation = UUID.randomUUID();
        return new RatisMemberReplacement(operation, group, new RatisMembershipView(5, peers, List.of()), peers.getFirst().getId(),
                RaftPeer.newBuilder().setId("r-" + operation).setAddress("localhost:12310").setStartupRole(RaftPeerRole.LISTENER).build());
    }

    /**
     * File, name and cleanup sync failed before and after operation, respectively.
     */
    enum Failure {
        PARTIAL_WRITE, BEFORE_FILE_FORCE, AFTER_FILE_FORCE, BEFORE_LINK, AFTER_LINK, BEFORE_DIRECTORY_FORCE, AFTER_DIRECTORY_FORCE, BEFORE_DELETE, AFTER_DELETE
    }

    /**
     * All side effects call for real I/O, one-time failure without falsifying official status.
     */
    static final class Fault implements RatisReplacementTickets.Io {

        final Failure failure;

        final List<String> operations = new ArrayList<>();

        boolean fired;

        /**
         * No failure instance record if retry is really synchronized.
         *
         * @param failure failure
         */
        Fault(final Failure failure) {
            this.failure = failure;
        }

        /**
         * The actual effect of the failed reservation has already occurred after the operation.
         *
         * @param point point
         * @throws IOException if the operation cannot complete
         */
        private void hit(final Failure point) throws IOException {
            if (!fired && point == failure) {
                fired = true;
                throw new IOException("injected ticket IO: " + point);
            }
        }

        /**
         * Part of the writing only pollutes private suspense and does not affect official names.
         *
         * @param channel channel
         * @param bytes bytes
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final FileChannel channel, final byte[] bytes) throws IOException {
            if (failure == Failure.PARTIAL_WRITE) {
                RatisGenerationRecord.write(channel, ByteBuffer.wrap(bytes, 0, bytes.length / 2));
                hit(Failure.PARTIAL_WRITE);
            }
            RatisReplacementTickets.Io.super.write(channel, bytes);
        }

        /**
         * Verify More key direct committee check to use a single bar in query.
         *
         * @param channel channel
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceFile(final FileChannel channel) throws IOException {
            operations.add("FILE_FORCE");
            hit(Failure.BEFORE_FILE_FORCE);
            RatisReplacementTickets.Io.super.forceFile(channel);
            hit(Failure.AFTER_FILE_FORCE);
        }

        /**
         * An error occurred after the atomic link, and the original official document should be confirmed next.
         *
         * @param source database connection source
         * @param target target
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void link(final Path source, final Path target) throws IOException {
            hit(Failure.BEFORE_LINK);
            RatisReplacementTickets.Io.super.link(source, target);
            hit(Failure.AFTER_LINK);
        }

        /**
         * A staging cleanup failure must not make the caller believe the save completed.
         *
         * @param file file
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void delete(final Path file) throws IOException {
            hit(Failure.BEFORE_DELETE);
            RatisReplacementTickets.Io.super.delete(file);
            hit(Failure.AFTER_DELETE);
        }

        /**
         * Directory synchronisation needs retry before and after operation failure.
         *
         * @param parent parent
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void forceDirectory(final Path parent) throws IOException {
            operations.add("DIRECTORY_FORCE");
            hit(Failure.BEFORE_DIRECTORY_FORCE);
            RatisReplacementTickets.Io.super.forceDirectory(parent);
            hit(Failure.AFTER_DIRECTORY_FORCE);
        }
    }
}
