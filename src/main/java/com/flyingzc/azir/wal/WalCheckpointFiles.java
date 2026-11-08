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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

/**
 * Checkpoint Intergenerational Directory and the only CURRENT pointer, do not cover damage by retreating back to any old generation.
 */
final class WalCheckpointFiles {

    /**
     * Bans case-based intergenerational documentation tools.
     */
    private WalCheckpointFiles() {
    }

    /**
     * Starts with the original WAL when a pointer is missing; starts must be refused if the pointer exists but is damaged.
     *
     * @param root root
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Current read(final Path root) throws IOException {
        Path pointer = root.resolve("CURRENT");
        if (!Files.exists(pointer)) {
            return null;
        }
        byte[] bytes = WalFiles.readBounded(pointer, 60);
        var in = ByteBuffer.wrap(bytes);
        if (bytes.length != 60 || in.getInt() != 0x415A4350 || in.getInt() != 1
                || ByteBuffer.wrap(bytes).getInt(56) != WalFiles.crc(bytes, 0, 56)) {
            throw new IOException("invalid checkpoint pointer");
        }
        UUID generation = new UUID(in.getLong(), in.getLong());
        byte[] hash = new byte[32];
        in.get(hash);
        String expected = java.util.HexFormat.of().formatHex(hash);
        Path directory = root.resolve("checkpoint-" + generation);
        byte[] image = WalFiles.readBounded(directory.resolve("snapshot.bin"), 128L * 1024 * 1024);
        if (!WalCodec.digest(image).equals(expected)) {
            throw new IOException("checkpoint image checksum mismatch");
        }
        return new Current(directory, expected, WalCheckpointState.decode(image));
    }

    /**
     * Creates a synchronized temporary pointer, and the caller then executes the same directory atomic change of name.
     *
     * @param root root
     * @param generation generation
     * @param imageHash image hash
     * @param io io
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static Path writePointer(final Path root, final UUID generation, final String imageHash, final WalLog.IoHook io) throws IOException {
        var data = ByteBuffer.allocate(60).putInt(0x415A4350).putInt(1).putLong(generation.getMostSignificantBits())
                .putLong(generation.getLeastSignificantBits()).put(java.util.HexFormat.of().parseHex(imageHash));
        data.putInt(WalFiles.crc(data.array(), 0, 56));
        Path temporary = root.resolve("CURRENT-" + UUID.randomUUID() + ".tmp");
        WalFiles.writeNew(temporary, data.array(), io);
        return temporary;
    }

    /**
     * Only now verify and when the directory is published durable storage will you clear the isolated files that are owned.
     *
     * @param root root
     * @param current current
     * @param archives archives
     * @throws IOException if the operation cannot complete
     */
    static void cleanup(final Path root, final Path current, final Set<String> archives) throws IOException {
        try (var entries = Files.list(root)) {
            for (Path path : entries.toList()) {
                String name = path.getFileName().toString();
                if (name.matches("receipts-[a-f0-9-]{36}\\.sst") && !archives.contains(name)
                        || name.matches("CURRENT-[a-f0-9-]{36}\\.tmp")
                        || name.matches("ENGINE-[a-f0-9-]{36}\\.tmp")
                        || current != null && name.equals("transactions.wal")) {
                    Files.deleteIfExists(path);
                } else if (name.matches("checkpoint-[a-f0-9-]{36}") && !path.equals(current)
                        && Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    // The user keeps the entire directory when entering an unknown file, which cannot be reversed to remove what does not belong to this.
                    try (var contents = Files.list(path)) {
                        var files = contents.toList();
                        if (files.stream().allMatch(file -> Set.of("snapshot.bin", "tail.wal").contains(file.getFileName().toString())
                                && Files.isRegularFile(file, java.nio.file.LinkOption.NOFOLLOW_LINKS))) {
                            for (Path file : files) {
                                Files.delete(file);
                            }
                            Files.delete(path);
                        }
                    }
                }
            }
        }
        WalFiles.forceDirectory(root);
    }

    /**
     * CURRENT verify intergenerational identity and complete snapshot.
     *
     * @param generation generation
     * @param imageHash image hash
     * @param state state
     */
    record Current(Path generation, String imageHash, WalCheckpointState state) {
    }
}
