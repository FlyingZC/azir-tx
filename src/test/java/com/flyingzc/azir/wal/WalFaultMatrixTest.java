package com.flyingzc.azir.wal;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.nio.file.Path;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** 正式验收重复真实部分写、刷盘不确定、发布失败及生命周期竞争场景。 */
class WalFaultMatrixTest {
    @TempDir Path directory;

    /** 同一故障类型在隔离目录上执行的独立轮次。 */
    private record Case(String fault, int repetition) {
        /** 将场景身份保留在 JUnit 报告中。 */
        @Override public String toString() { return fault + "-" + repetition; }
    }

    /** 普通回归一轮，正式验收用系统属性显式指定重复次数。 */
    static Stream<Case> cases() {
        return Stream.of("PARTIAL_HEADER", "PARTIAL_PAYLOAD", "BEFORE_FORCE", "LOST_FORCE_ACK", "HALF_INSTALL",
                "DUPLICATE_COMMIT", "CLOSE_RACE", "EXPIRED_READER").flatMap(fault ->
                IntStream.range(0, Integer.getInteger("azir.wal.faultRepeats", 1)).mapToObj(n -> new Case(fault, n)));
    }

    /** 复用已经固定的故障断言，避免重复写出与原测试不一致的恢复规则。 */
    @ParameterizedTest @MethodSource("cases")
    void repeatedFaultsPreserveTheCommitContract(Case scenario) throws Exception {
        var test = new WalFaultAcceptanceTest(); test.directory = directory;
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
}
