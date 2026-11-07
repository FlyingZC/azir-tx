package com.flyingzc.azir.wal;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

/** 对完整日志的头、帧头和载荷注入可检测损坏，必须拒绝启动且不截断证据。 */
class WalCorruptionMatrixTest {
    @TempDir Path directory;

    /** 指明损坏所在区域及可复现随机位置。 */
    private record Damage(String region, int repetition) {
        /** 在 JUnit 中保留具体故障身份。 */
        @Override public String toString() { return region + "-" + repetition; }
    }

    /** 正式场景每个区域 100 次，普通回归保持每个区域两次。 */
    static Stream<Damage> cases() {
        return Stream.of("FILE_HEADER", "FRAME_HEADER", "PAYLOAD").flatMap(region ->
                IntStream.range(0, Integer.getInteger("azir.wal.corruptionRepeats", 2)).mapToObj(n -> new Damage(region, n)));
    }

    /** 单比特损坏不能被误当作可丢弃尾部，失败打开后仍可修复并恢复原日志。 */
    @ParameterizedTest @MethodSource("cases")
    void completeRecordDamageFailsClosedWithoutDestroyingEvidence(Damage damage) throws Exception {
        var records = IntStream.rangeClosed(1, 3).mapToObj(sequence -> new WalCommit(UUID.randomUUID(), sequence, sequence - 1,
                List.of(new WalMutation("a", "key:" + sequence, new byte[256]),
                        new WalMutation("b", "key:" + sequence, new byte[256])))).toList();
        try (var log = new WalLog(directory, Set.of("a", "b"), 1_000_000)) { log.append(records); }
        Path file = directory.resolve("transactions.wal"); byte[] intact = Files.readAllBytes(file); byte[] damaged = intact.clone();
        var random = new Random(20261010L + damage.repetition);
        int recordIndex = damage.repetition % records.size(); int offset = 60;
        for (int i = 0; i < recordIndex; i++) { offset += 16 + WalCodec.encode(records.get(i)).length; }
        int position = switch (damage.region) {
            case "FILE_HEADER" -> random.nextInt(60);
            case "FRAME_HEADER" -> offset + random.nextInt(16);
            case "PAYLOAD" -> offset + 16 + random.nextInt(WalCodec.encode(records.get(recordIndex)).length);
            default -> throw new IllegalArgumentException(damage.region);
        };
        damaged[position] ^= (byte) (1 << random.nextInt(8)); Files.write(file, damaged);
        assertThrows(IOException.class, () -> new WalLog(directory, Set.of("a", "b"), 1_000_000));
        assertArrayEquals(damaged, Files.readAllBytes(file), "failed recovery must preserve corrupted evidence");
        Files.write(file, intact);
        try (var repaired = new WalLog(directory, Set.of("a", "b"), 1_000_000)) {
            assertEquals(3, repaired.lastSequence());
            for (WalCommit record : records) { assertEquals(record.digest(), repaired.receipt(record.transactionId()).orElseThrow().digest()); }
        }
    }
}
