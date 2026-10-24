package mvcc.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/** 检查点代际目录与唯一 CURRENT 指针，不通过回退到任意旧代掩盖损坏。 */
final class WalCheckpointFiles {
    /** CURRENT 已验证的代际身份及完整快照。 */
    record Current(Path generation, String imageHash, WalCheckpointState state) { }

    /** 禁止实例化代际文件工具。 */
    private WalCheckpointFiles() { }

    /** 缺少指针时按原始 WAL 启动；指针存在但损坏时必须拒绝启动。 */
    static Current read(Path root) throws IOException {
        Path pointer = root.resolve("CURRENT"); if (!Files.exists(pointer)) { return null; }
        byte[] bytes = WalFiles.readBounded(pointer, 60); var in = ByteBuffer.wrap(bytes);
        if (bytes.length != 60 || in.getInt() != 0x415A4350 || in.getInt() != 1
                || ByteBuffer.wrap(bytes).getInt(56) != WalFiles.crc(bytes, 0, 56)) { throw new IOException("invalid checkpoint pointer"); }
        UUID generation = new UUID(in.getLong(), in.getLong()); byte[] hash = new byte[32]; in.get(hash);
        String expected = java.util.HexFormat.of().formatHex(hash); Path directory = root.resolve("checkpoint-" + generation);
        byte[] image = WalFiles.readBounded(directory.resolve("snapshot.bin"), 128L * 1024 * 1024);
        if (!WalCodec.digest(image).equals(expected)) { throw new IOException("checkpoint image checksum mismatch"); }
        return new Current(directory, expected, WalCheckpointState.decode(image));
    }

    /** 创建已同步的临时指针，调用方随后执行同目录原子改名。 */
    static Path writePointer(Path root, UUID generation, String imageHash, WalLog.IoHook io) throws IOException {
        var data = ByteBuffer.allocate(60).putInt(0x415A4350).putInt(1).putLong(generation.getMostSignificantBits())
                .putLong(generation.getLeastSignificantBits()).put(java.util.HexFormat.of().parseHex(imageHash));
        data.putInt(WalFiles.crc(data.array(), 0, 56)); Path temporary = root.resolve("CURRENT-" + UUID.randomUUID() + ".tmp");
        WalFiles.writeNew(temporary, data.array(), io); return temporary;
    }

    /** 只有已验证当前代且目录发布持久化后才清理本实现拥有的孤立文件。 */
    static void cleanup(Path root, Path current, Set<String> archives) throws IOException {
        try (var entries = Files.list(root)) {
            for (Path path : entries.toList()) {
                String name = path.getFileName().toString();
                if (name.matches("receipts-[a-f0-9-]{36}\\.sst") && !archives.contains(name)
                        || name.matches("CURRENT-[a-f0-9-]{36}\\.tmp")
                        || name.matches("ENGINE-[a-f0-9-]{36}\\.tmp")
                        || current != null && name.equals("transactions.wal")) { Files.deleteIfExists(path); }
                else if (name.matches("checkpoint-[a-f0-9-]{36}") && !path.equals(current)
                        && Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    // 用户放入未知文件时保留整个目录，不能递归删除不属于本实现的内容。
                    try (var contents = Files.list(path)) {
                        var files = contents.toList();
                        if (files.stream().allMatch(file -> Set.of("snapshot.bin", "tail.wal").contains(file.getFileName().toString())
                                && Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS))) {
                            for (Path file : files) { Files.delete(file); } Files.delete(path);
                        }
                    }
                }
            }
        }
        WalFiles.forceDirectory(root);
    }
}
