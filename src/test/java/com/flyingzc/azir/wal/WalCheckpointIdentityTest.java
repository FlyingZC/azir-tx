package com.flyingzc.azir.wal;

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
import static org.junit.jupiter.api.Assertions.*;

/** 持久恢复身份缺失或损坏时必须保留证据并拒绝启动，不能误初始化空引擎。 */
class WalCheckpointIdentityTest {
    @TempDir Path directory;

    /** 兼容尚未有 ENGINE 标记的检查点目录，也必须识别丢失 CURRENT 的非空数据。 */
    @Test void missingCurrentWithoutMarkerCannotDestroyCheckpointFiles() throws Exception {
        checkpoint(); Files.delete(directory.resolve("CURRENT")); Files.deleteIfExists(directory.resolve("ENGINE"));
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes()); assertFalse(Files.exists(directory.resolve("transactions.wal")));
    }

    /** 即使检查点文件移走，持久引擎身份也能阻止将旧目录重新初始化。 */
    @Test void durableEngineMarkerPreventsEmptyReinitialization() throws Exception {
        checkpoint(); Path saved = directory.resolve("saved-files"); Files.createDirectory(saved);
        try (var files = Files.list(directory)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                if (name.startsWith("checkpoint-") || name.startsWith("receipts-")) { Files.move(path, saved.resolve(name)); }
            }
        }
        Files.delete(directory.resolve("CURRENT")); assertTrue(Files.exists(directory.resolve("ENGINE")));
        var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes());
    }

    /** 原始 WAL 缺失也不能在同一持久身份下初始化新的空日志。 */
    @Test void missingLegacyWalWithEngineMarkerFailsClosed() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); }
        Files.delete(directory.resolve("transactions.wal")); var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes());
    }

    /** ENGINE 损坏不能被正常 WAL 掩盖，更不能覆盖原标记继续提供服务。 */
    @Test void corruptEngineIdentityIsNotOverwritten() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); }
        Path marker = directory.resolve("ENGINE"); byte[] original = Files.readAllBytes(marker); byte[] bad = original.clone(); bad[20] ^= 1; Files.write(marker, bad);
        var before = hashes(); assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes()); Files.write(marker, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(1, restored.stats().publishedSequence()); }
    }

    /** 空文件也不代表新引擎，已有身份时必须在重写头部之前拒绝启动。 */
    @Test void emptyLegacyWalWithIdentityFailsBeforeHeaderRewrite() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); }
        Path wal = directory.resolve("transactions.wal"); byte[] original = Files.readAllBytes(wal); Files.write(wal, new byte[0]); var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes()); Files.write(wal, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(1, restored.stats().publishedSequence()); }
    }

    /** 当前代尾日志缺失时必须失败，不能通过 CREATE 生成一个空尾部。 */
    @Test void missingCurrentTailIsNotCreatedAgain() throws Exception {
        checkpoint(); Path tail;
        try (var files = Files.list(directory)) { tail = files.filter(Files::isDirectory).findFirst().orElseThrow().resolve("tail.wal"); }
        byte[] original = Files.readAllBytes(tail); Files.delete(tail); var before = hashes();
        assertThrows(WalMvccException.class, () -> WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults()));
        assertEquals(before, hashes()); assertFalse(Files.exists(tail)); Files.write(tail, original);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(1, restored.stats().publishedSequence()); }
    }

    /** CURRENT 尚未发布且完整旧 WAL 仍在时，允许丢弃未发布候选，不能误拒绝正常崩溃恢复。 */
    @Test void orphanCandidateWithValidLegacyWalRemainsRecoverable() throws Exception {
        UUID receipt;
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ); receipt = tx.id(); runtime.put(tx, "a", "k", bytes("safe")); runtime.commit(tx);
        }
        Path candidate = directory.resolve("checkpoint-" + UUID.randomUUID()); Files.createDirectory(candidate);
        Files.write(candidate.resolve("snapshot.bin"), new byte[]{1, 2, 3}); Files.write(candidate.resolve("tail.wal"), new byte[]{4, 5});
        Path archive = directory.resolve("receipts-" + UUID.randomUUID() + ".sst"); Files.write(archive, new byte[]{6});
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) {
            assertEquals(1, restored.findCommitted(receipt).orElseThrow().commitSequence());
        }
        assertFalse(Files.exists(candidate)); assertFalse(Files.exists(archive));
    }

    /** 已有旧 WAL 可原地补写持久身份，补标记不能改变引擎 UUID 或提交历史。 */
    @Test void validLegacyWalCanAcquireMissingEngineMarker() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); }
        byte[] expected = Files.readAllBytes(directory.resolve("ENGINE")); Files.delete(directory.resolve("ENGINE"));
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(1, restored.stats().publishedSequence()); }
        assertArrayEquals(expected, Files.readAllBytes(directory.resolve("ENGINE")));
    }

    /** 清理候选代不能跟随符号链接删除目录之外的恢复文件。 */
    @Test void orphanCleanupDoesNotFollowGenerationSymlinks() throws Exception {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); }
        Path external = directory.resolve("external-backup"); Files.createDirectory(external);
        Files.write(external.resolve("snapshot.bin"), new byte[]{1}); Files.write(external.resolve("tail.wal"), new byte[]{2});
        Path link = directory.resolve("checkpoint-" + UUID.randomUUID()); Files.createSymbolicLink(link, external);
        try (var restored = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { assertEquals(1, restored.stats().publishedSequence()); }
        assertTrue(Files.exists(external.resolve("snapshot.bin"))); assertTrue(Files.exists(external.resolve("tail.wal")));
        assertTrue(Files.isSymbolicLink(link));
    }

    /** 先完成一笔可核对事务，再创建唯一恢复代。 */
    private void checkpoint() {
        try (var runtime = WalMvccRuntime.open(directory, Set.of("a"), WalOptions.defaults())) { write(runtime, "a", "k", "safe"); assertTrue(runtime.checkpoint().created()); }
    }

    /** 失败启动前后核对所有文件内容，防止错误分支清理仍然需要的恢复证据。 */
    private Map<String, String> hashes() throws Exception {
        var hashes = new TreeMap<String, String>();
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) { hashes.put(directory.relativize(path).toString(), WalCodec.digest(Files.readAllBytes(path))); }
        }
        return hashes;
    }
}
