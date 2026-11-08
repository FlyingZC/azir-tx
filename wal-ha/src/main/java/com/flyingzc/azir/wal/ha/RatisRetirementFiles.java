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

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;
import java.io.IOException;

/**
 * Deletes only the protocol files in verify old generation, unknown files and links are rejected, directories are cleaned on a wheel-by-wheel basis bounded.
 */
final class RatisRetirementFiles {

    private static final String UUID = "[a-f0-9-]{36}";

    /**
     * Prevent instantiation of this utility class.
     */
    private RatisRetirementFiles() {
    }

    /**
     * The control directory lock collection has interrupted the release of private suspense files and does not require a semi-written document to be valid CRC.
     *
     * @param root root
     * @param snapshots snapshots
     * @param work work
     * @throws IOException if the operation cannot complete
     */
    static void scratch(final Path root, final boolean snapshots, final RatisRetirementWork work) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        if (Files.isSymbolicLink(root)) {
            throw new IOException("scratch root is symbolic");
        }
        try (var entries = Files.list(root)) {
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                work.scan();
                Path path = iterator.next();
                String name = path.getFileName().toString();
                boolean owned = snapshots ? name.matches("\\.bundle-" + UUID + "|snapshot\\.[0-9]+_[0-9]+\\.md5\\.tmp") : name.matches("\\.(record|retirement)-" + UUID);
                if (!owned) {
                    continue;
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("scratch entry is not regular");
                }
                work.deleting();
                Files.delete(path);
                work.removed++;
                RatisGenerationRecord.forceDirectory(root);
            }
        }
    }

    /**
     * Only limited tree prefixes are checked at one time, and repeated calls can continue in a large archive directory.
     *
     * @param root root
     * @param generation generation
     * @param work work
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static boolean clean(final Path root, final boolean generation, final RatisRetirementWork work, final Hook hook) throws IOException {
        return clean(root, generation, generation ? "GENERATION" : "INSTALLATION", work, hook);
    }

    /**
     * The verify built identity image suspense uses the BUILD tag, and the same bounded is deleted on a wheel-by-wheel basis.
     *
     * @param root root
     * @param work work
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    static boolean cleanBuild(final Path root, final RatisRetirementWork work) throws IOException {
        return clean(root, false, "BUILD", work, path -> {
        });
    }

    /**
     * The tag was eventually deleted, and any interruptions were kept for the next round of checks.
     *
     * @param root root
     * @param generation generation
     * @param marker marker
     * @param work work
     * @param hook hook
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static boolean clean(final Path root, final boolean generation, final String marker, final RatisRetirementWork work, final Hook hook) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        if (Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("retirement tree is not a private directory");
        }
        var paths = new ArrayList<Path>();
        try (var stream = Files.walk(root, 5)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                if (paths.size() >= Math.max(1, work.options.scanLimit() - work.scanned)) {
                    break;
                }
                Path path = iterator.next();
                work.scan();
                if (path.equals(root)) {
                    continue;
                }
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("retirement tree contains symbolic link");
                }
                String name = root.relativize(path).toString();
                boolean directory = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
                if (!(name.equals(marker) && !directory || allowed(name, generation, directory)) || !directory && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException("retirement tree contains unknown entry: " + name);
                }
                paths.add(path);
            }
        }
        paths.sort(Comparator.reverseOrder());
        for (Path path : paths) {
            if (path.equals(root.resolve(marker))) {
                continue;
            }
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                try (var entries = Files.list(path)) {
                    if (entries.findAny().isPresent()) {
                        continue;
                    }
                }
            }
            work.deleting();
            if (Files.deleteIfExists(path)) {
                work.removed++;
                RatisGenerationRecord.forceDirectory(path.getParent());
                hook.deleted(path);
            }
        }
        try (var entries = Files.list(root)) {
            if (entries.anyMatch(path -> !path.equals(root.resolve(marker)))) {
                return false;
            }
        }
        if (Files.exists(root.resolve(marker))) {
            work.deleting();
            Files.delete(root.resolve(marker));
            work.removed++;
            RatisGenerationRecord.forceDirectory(root);
            hook.deleted(root.resolve(marker));
        }
        work.deleting();
        Files.delete(root);
        work.removed++;
        RatisGenerationRecord.forceDirectory(root.getParent());
        hook.deleted(root);
        return true;
    }

    /**
     * Only when the protocol path is known will it be allowed to be deleted, and the recovery suspense directory also limits its internal structure.
     *
     * @param name name
     * @param generation generation
     * @param directory directory
     * @return the operation result
     */
    private static boolean allowed(final String name, final boolean generation, final boolean directory) {
        if (!directory && name.equals(generation ? "GENERATION" : "INSTALLATION")) {
            return true;
        }
        if (generation) {
            if (directory && (name.equals("wal") || name.matches("\\.azir-restore-" + UUID + "-[a-f0-9]{24}"))) {
                return true;
            }
            if (!directory && name.matches("\\.azir-restore-[a-f0-9]{24}\\.lock|\\.record-" + UUID)) {
                return true;
            }
            String prefix = name.startsWith("wal/") ? "wal/" : name.matches("\\.azir-restore-" + UUID + "-[a-f0-9]{24}/.*") ? name.substring(0, name.indexOf('/') + 1) : null;
            return prefix != null && wal(name.substring(prefix.length()), directory);
        }
        if (!directory && (name.equals("snapshot.bundle") || name.matches("\\.record-" + UUID))) {
            return true;
        }
        String prefix = name.equals("backup") || name.startsWith("backup/") ? "backup" : name.matches("\\.azir-unpack-" + UUID + "(?:/.*)?") ? name.split("/", 2)[0] : null;
        if (prefix == null) {
            return false;
        }
        if (name.equals(prefix)) {
            return directory;
        }
        String rest = name.substring(prefix.length() + 1);
        if (directory && (rest.equals("local") || rest.equals("data"))) {
            return true;
        }
        if (!directory && (rest.equals("COMPLETE") || rest.equals("COMPLETE.tmp") || rest.matches("data/[0-9]{4}\\.bin"))) {
            return true;
        }
        return rest.startsWith("local/") && wal(rest.substring(6), directory);
    }

    /**
     * WAL identity, checkpoint and receipt archive only recognize a fixed name and do not receive any type of file.
     *
     * @param name name
     * @param directory directory
     * @return the operation result
     */
    private static boolean wal(final String name, final boolean directory) {
        if (directory) {
            return name.matches("checkpoint-" + UUID);
        }
        return name.matches("ENGINE|REPLICA|CURRENT|RESTORING|writer\\.lock|transactions\\.wal|(CURRENT|ENGINE)-" + UUID + "\\.tmp|receipts-" + UUID + "\\.sst|checkpoint-" + UUID
                + "/(snapshot\\.bin|tail\\.wal)");
    }

    /**
     * All actual deleted positions are available to interrupt verify in the package.
     */
    @FunctionalInterface
    interface Hook {

        /**
         * Injection is not a substitute for synchronizing real deletions with directories.
         *
         * @param path path
         * @throws IOException if the operation cannot complete
         */
        void deleted(Path path) throws IOException;
    }
}
