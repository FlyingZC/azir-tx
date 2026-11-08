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

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Path;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The formal acceptance duplicates the true part, the brush is uncertain, the distribution fails and the life cycle competition scene.
 */
class WalFaultMatrixTest {

    @TempDir
    Path directory;

    /**
     * Normal return round, with the formal acceptance system attribute clearly specifying the number of repetitions.
     *
     * @return the operation result
     */
    static Stream<Case> cases() {
        return Stream.of("PARTIAL_HEADER", "PARTIAL_PAYLOAD", "BEFORE_FORCE", "LOST_FORCE_ACK", "HALF_INSTALL",
                "DUPLICATE_COMMIT", "CLOSE_RACE", "EXPIRED_READER").flatMap(fault -> IntStream.range(0, Integer.getInteger("azir.wal.faultRepeats", 1)).mapToObj(n -> new Case(fault, n)));
    }

    /**
     * Repeat the already fixed failure assertion and avoid writing recovery rules that are inconsistent with the original test.
     *
     * @param scenario scenario
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    @ParameterizedTest
    @MethodSource("cases")
    void repeatedFaultsPreserveTheCommitContract(final Case scenario) throws Exception {
        var test = new WalFaultAcceptanceTest();
        test.directory = directory;
        switch (scenario.fault) {
            case "PARTIAL_HEADER" -> test.partialWriteFailureCannotLoseAcknowledgedPrefix(8);
            case "PARTIAL_PAYLOAD" -> test.partialWriteFailureCannotLoseAcknowledgedPrefix(20);
            case "BEFORE_FORCE" -> test.forceFailureHasUnknownOutcomeUntilRecovery();
            case "LOST_FORCE_ACK" -> test.lostForceAcknowledgementMustRecoverTheSameTransaction();
            case "HALF_INSTALL" -> test.failureHalfwayThroughInstallationCannotPublishHalfATransaction();
            case "DUPLICATE_COMMIT" -> test.simultaneousDuplicateCommitSharesOneDecision();
            case "CLOSE_RACE" -> test.closeDrainsAcceptedCommitBeforeReleasingWal();
            case "EXPIRED_READER" -> test.expiredReaderCannotKeepOrReuseAnOldSnapshot();
            default -> throw new IllegalArgumentException(scenario.fault);
        }
    }

    /**
     * The same failure type executes a separate rotation on the isolation directory.
     *
     * @param fault fault
     * @param repetition repetition
     */
    private record Case(String fault, int repetition) {

        /**
         * Keeps the scene identity in the junit report.
         *
         * @return the operation result
         */
        @Override
        public String toString() {
            return fault + "-" + repetition;
        }
    }
}
