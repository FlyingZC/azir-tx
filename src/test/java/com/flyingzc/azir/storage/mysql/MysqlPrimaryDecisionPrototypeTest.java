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

package com.flyingzc.azir.storage.mysql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import com.zaxxer.hikari.HikariDataSource;
import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.common.TxStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Two real MySQL shard verifications of durable decision and prewrite certificates for main participants in shard.
 */
@Testcontainers
class MysqlPrimaryDecisionPrototypeTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("bootstrap").withUsername("test").withPassword("test");

    private static HikariDataSource primary;

    private static HikariDataSource secondary;

    private MysqlPrimaryDecisionPrototype decisions;

    /**
     * Separate durable database of main shard in the same test MySQL.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeAll
    static void openDatabases() throws Exception {
        createDatabase("primary_decision_one");
        createDatabase("primary_decision_two");
        primary = pool("primary_decision_one");
        secondary = pool("primary_decision_two");
    }

    /**
     * Test account only needs permission to test database.
     *
     * @param name name
     * @throws Exception if the operation cannot complete
     */
    private static void createDatabase(final String name) throws Exception {
        MYSQL.execInContainer("mysql", "-uroot", "-p" + MYSQL.getPassword(), "-e",
                "create database if not exists `" + name + "`; "
                        + "grant all privileges on `" + name + "`.* to '" + MYSQL.getUsername() + "'@'%'; "
                        + "flush privileges;");
    }

    /**
     * JDBC URL replaces only the physical database name of the test.
     *
     * @param database database
     * @return the operation result
     */
    private static HikariDataSource pool(final String database) {
        return (HikariDataSource) MysqlDataSourceFactory.create(
                MYSQL.getJdbcUrl().replace("/bootstrap", "/" + database),
                MYSQL.getUsername(), MYSQL.getPassword());
    }

    /**
     * Return two connection pieces after testing.
     */
    @AfterAll
    static void closeDatabases() {
        if (primary != null) {
            primary.close();
        }
        if (secondary != null) {
            secondary.close();
        }
    }

    /**
     * Each case starts with an empty isolation intent and decision serial numbers.
     *
     * @throws Exception if the operation cannot complete
     */
    @BeforeEach
    void reset() throws Exception {
        MysqlStripedCrossVersionPrototype.initializeIsolated(primary, 4);
        MysqlStripedCrossVersionPrototype.initializeIsolated(secondary, 4);
        decisions = new MysqlPrimaryDecisionPrototype(primary, secondary, 4);
        decisions.initializeIsolated();
        decisions.resetIsolated();
    }

    /**
     * Two pieces of prewrite must be present at the same time, and commit replay cannot advance the main decision serial number again.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void commitRequiresBothDurableProofsAndReplayKeepsOneSequence() throws Exception {
        var registration = registration(101);
        Assertions.assertEquals(TxStatus.ACTIVE, decisions.register(registration).status());
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MvccException missing = assertThrows(MvccException.class, () -> decisions.commit(101));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, missing.errorCode());
        Assertions.assertEquals(0L, decisions.decisionVector().get(2));
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        var first = decisions.commit(101);
        Assertions.assertEquals(TxStatus.COMMITTED, first.status());
        Assertions.assertEquals(1L, first.decisionSequence());
        Assertions.assertEquals(1L, first.primarySequence());
        Assertions.assertEquals(1L, first.secondarySequence());
        Assertions.assertEquals(first, decisions.commit(101));
        Assertions.assertEquals(first, decisions.register(registration));
        Assertions.assertEquals(first, decisions.decision(101).orElseThrow());
        Assertions.assertEquals(1L, decisions.decisionVector().get(2));
        assertThrows(MvccException.class, () -> decisions.abort(101));
    }

    /**
     * Main registration with main prewrite is done in physical transaction at a time, and replay maintains the same number.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void combinedPrimaryPrewriteIsAtomicAndReplayKeepsItsSequence() throws Exception {
        var registration = registration(109);
        long first = decisions.registerAndPrewritePrimary(registration);
        Assertions.assertEquals(1L, first);
        Assertions.assertEquals(TxStatus.ACTIVE, decisions.decision(109).orElseThrow().status());
        Assertions.assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, 109, 0);
        var committed = decisions.commit(registration);
        Assertions.assertEquals(first, committed.primarySequence());
        Assertions.assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.materialize(primary, registration.primaryKey(),
                109, committed.decisionSequence(), this::committedSequence);
        Assertions.assertEquals(first, decisions.registerAndPrewritePrimary(registration));
        MysqlStripedCrossVersionPrototype.materialize(secondary, registration.secondaryKey(),
                109, committed.decisionSequence(), this::committedSequence);
        Assertions.assertEquals(committed, decisions.commit(registration));
    }

    /**
     * The primary key conflict allows the whole call rollback, and cannot leave only ACTIVE with no main intent.
     */
    @Test
    void failedCombinedPrimaryPrewriteRollsBackRegistration() {
        var owner = registration(110);
        decisions.registerAndPrewritePrimary(owner);
        var conflicting = new MysqlPrimaryDecisionPrototype.Registration(111,
                owner.primaryKey(), bytes("other"), "secondary:111", bytes("right"),
                1, 3, 0, 0, deadline(), 2);
        MvccException rejected = assertThrows(MvccException.class,
                () -> decisions.registerAndPrewritePrimary(conflicting));
        Assertions.assertEquals(ErrorCode.LOCK_CONFLICT, rejected.errorCode());
        assertTrue(decisions.decision(111).isEmpty());
        Assertions.assertEquals(1L, decisions.registerAndPrewritePrimary(owner));
    }

    /**
     * The main decision after the new RC saw two pieces at the same time, and the old RR continues to see the old values after the one-side and double-sided materialization.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void oldRrAndNewRcRemainAtomicAcrossOneSidedMaterialization() throws Exception {
        MysqlStripedCrossVersionPrototype.localCommit(primary,
                "primary:106", bytes("old-left"), 1, 0);
        MysqlStripedCrossVersionPrototype.localCommit(secondary,
                "secondary:106", bytes("old-right"), 3, 0);
        var registration = new MysqlPrimaryDecisionPrototype.Registration(106,
                "primary:106", bytes("new-left"), "secondary:106", bytes("new-right"),
                1, 3, 1, 1, deadline(), 2);
        try (
                var reader = new MysqlPrimaryVectorReadPrototype(
                        "s1", primary, "s2", secondary, 4, decisions)) {
            var oldRr = reader.captureUnpublished();
            decisions.register(registration);
            MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                    registration.primaryValue(), 1, 106, 1);
            MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                    registration.secondaryValue(), 3, 106, 1);
            var beforeDecision = reader.captureUnpublished();
            assertArrayEquals(bytes("old-left"),
                    reader.read(beforeDecision, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("old-right"),
                    reader.read(beforeDecision, "s2", registration.secondaryKey(), 3).orElseThrow());
            var committed = decisions.commit(106);
            var newRc = reader.captureUnpublished();
            assertArrayEquals(bytes("old-left"),
                    reader.read(oldRr, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("old-right"),
                    reader.read(oldRr, "s2", registration.secondaryKey(), 3).orElseThrow());
            assertArrayEquals(bytes("new-left"),
                    reader.read(newRc, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
            MysqlStripedCrossVersionPrototype.materialize(primary, registration.primaryKey(),
                    106, committed.decisionSequence(), this::committedSequence);
            assertArrayEquals(bytes("old-left"),
                    reader.read(oldRr, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-left"),
                    reader.read(newRc, "s1", registration.primaryKey(), 1).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
            MysqlStripedCrossVersionPrototype.materialize(secondary, registration.secondaryKey(),
                    106, committed.decisionSequence(), this::committedSequence);
            assertArrayEquals(bytes("old-right"),
                    reader.read(oldRr, "s2", registration.secondaryKey(), 3).orElseThrow());
            assertArrayEquals(bytes("new-right"),
                    reader.read(newRc, "s2", registration.secondaryKey(), 3).orElseThrow());
        }
    }

    /**
     * The same transaction ID cannot be re-registered and inherit the existing master decision.
     */
    @Test
    void registrationReplayRejectsChangedPayload() {
        decisions.register(registration(102));
        var changed = new MysqlPrimaryDecisionPrototype.Registration(102,
                "primary:102", bytes("different"), "secondary:102", bytes("right"),
                1, 3, 0, 0, deadline(), 2);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.register(changed));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
    }

    /**
     * The key value is the same as transaction ID and cannot write a secondary to an unregistered data segment.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void commitRejectsPrewriteOnWrongParticipantStripe() throws Exception {
        var registration = registration(107);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, 107, 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 2, 107, 0);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.commit(107));
        Assertions.assertEquals(ErrorCode.ILLEGAL_STATE, rejected.errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, decisions.decision(107).orElseThrow().status());
        Assertions.assertEquals(0L, decisions.decisionVector().get(2));
    }

    /**
     * Main transaction line locks allow commit and abort only one irreversible terminal state.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void commitAndAbortRaceHasExactlyOneDurableTerminalDecision() throws Exception {
        var registration = registration(103);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var commit = workers.submit(() -> {
                start.await();
                try {
                    return decisions.commit(103).status();
                } catch (final MvccException rejected) {
                    if (rejected.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                        throw rejected;
                    }
                    return TxStatus.ABORTED;
                }
            });
            var abort = workers.submit(() -> {
                start.await();
                try {
                    return decisions.abort(103).status();
                } catch (final MvccException rejected) {
                    if (rejected.errorCode() != ErrorCode.TRANSACTION_STATUS_UNKNOWN) {
                        throw rejected;
                    }
                    return TxStatus.COMMITTED;
                }
            });
            start.countDown();
            TxStatus committedSide = commit.get(30, TimeUnit.SECONDS);
            TxStatus abortedSide = abort.get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(committedSide, abortedSide);
            var durable = decisions.decision(103).orElseThrow();
            Assertions.assertEquals(committedSide, durable.status());
            Assertions.assertEquals(durable.status() == TxStatus.COMMITTED ? 1L : 0L,
                    decisions.decisionVector().get(2));
        }
    }

    /**
     * The durable aborted certificate allowed two pieces to remain tombstone and refused to be late for the prewrite resurrection.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void abortProofRejectsLateParticipantPrewrite() throws Exception {
        var registration = registration(104);
        decisions.register(registration);
        Assertions.assertEquals(TxStatus.ABORTED, decisions.abort(104).status());
        MysqlStripedCrossVersionPrototype.abort(primary, registration.primaryKey(), 104,
                txId -> decisions.decision(txId).orElseThrow().status());
        MysqlStripedCrossVersionPrototype.abort(secondary, registration.secondaryKey(), 104,
                txId -> decisions.decision(txId).orElseThrow().status());
        assertThrows(SQLException.class,
                () -> MysqlStripedCrossVersionPrototype.prewrite(secondary,
                        registration.secondaryKey(), registration.secondaryValue(), 3, 104, 0));
        assertTrue(decisions.decisionVector().stream().allMatch(sequence -> sequence == 0));
    }

    /**
     * The expired main identity cannot be commit even if both prewrite s exist.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void expiredPrimaryIdentityCannotCommit() throws Exception {
        var registration = new MysqlPrimaryDecisionPrototype.Registration(105,
                "primary:105", bytes("left"), "secondary:105", bytes("right"),
                1, 3, 0, 0, System.currentTimeMillis() - 1000, 2);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, registration.txId(), 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, registration.txId(), 0);
        MvccException rejected = assertThrows(MvccException.class, () -> decisions.commit(105));
        Assertions.assertEquals(ErrorCode.TRANSACTION_ABORTED, rejected.errorCode());
        Assertions.assertEquals(TxStatus.ACTIVE, decisions.decision(105).orElseThrow().status());
        Assertions.assertEquals(TxStatus.ABORTED, decisions.abort(105).status());
    }

    /**
     * The main decision Clock Unattainable time series cannot treat unknown intent as visible or aborted.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void secondaryReadFailsClosedWhenPrimaryDecisionIsUnavailable() throws Exception {
        var registration = registration(108);
        decisions.register(registration);
        MysqlStripedCrossVersionPrototype.prewrite(primary, registration.primaryKey(),
                registration.primaryValue(), 1, 108, 0);
        MysqlStripedCrossVersionPrototype.prewrite(secondary, registration.secondaryKey(),
                registration.secondaryValue(), 3, 108, 0);
        decisions.commit(108);
        try (
                var available = new MysqlPrimaryVectorReadPrototype(
                        "s1", primary, "s2", secondary, 4, decisions)) {
            var cut = available.captureUnpublished();
            try (HikariDataSource unavailable = pool("primary_decision_one")) {
                unavailable.close();
                var broken = new MysqlPrimaryDecisionPrototype(unavailable, secondary, 4);
                try (
                        var reader = new MysqlPrimaryVectorReadPrototype(
                                "s1", primary, "s2", secondary, 4, broken)) {
                    MvccException failure = assertThrows(MvccException.class,
                            () -> reader.read(cut, "s2", registration.secondaryKey(), 3));
                    Assertions.assertEquals(ErrorCode.TRANSACTION_STATUS_UNKNOWN, failure.errorCode());
                }
            }
        }
    }

    /**
     * Is the fixed key, value, snapshot boundary and main decision bar for two pieces of construction.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private static MysqlPrimaryDecisionPrototype.Registration registration(final long txId) {
        return new MysqlPrimaryDecisionPrototype.Registration(txId,
                "primary:" + txId, bytes("left"), "secondary:" + txId, bytes("right"),
                1, 3, 0, 0, deadline(), 2);
    }

    /**
     * The test period is sufficient to cover database start and concurrent stagger.
     *
     * @return the operation result
     */
    private static long deadline() {
        return System.currentTimeMillis() + 60_000;
    }

    /**
     * Test key values use repeatable UTF-8 bytes.
     *
     * @param value value to write
     * @return the operation result
     */
    private static byte[] bytes(final String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Prototype materialization accepts only the primary shard durable COMMITTED serial number.
     *
     * @param txId transaction identifier
     * @return the operation result
     */
    private OptionalLong committedSequence(final long txId) {
        var decision = decisions.decision(txId).orElseThrow();
        return decision.status() == TxStatus.COMMITTED
                ? OptionalLong.of(decision.decisionSequence())
                : OptionalLong.empty();
    }
}
