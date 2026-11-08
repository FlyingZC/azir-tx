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

package com.flyingzc.azir.operations;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.sql.DataSource;

import static com.flyingzc.azir.operations.MysqlProtocolState.storage;

/**
 * Freezes portable current backup of the checkpoint; recovery only writes known tables and columns, and does not execute any SQL in the backup.
 */
public final class MysqlProtocolBackup {

    private static final int MAGIC = 0x415A4232;

    private static final int FORMAT = 2;

    private static final int MAX_FIELD = 64 * 1024 * 1024;

    private static final int MAX_ROW = 96 * 1024 * 1024;

    /**
     * Prohibits the case-inertation tool.
     */
    private MysqlProtocolBackup() {
    }

    /**
     * Export after freezing; only temporary files exist until the file is completed and database freeze for operator retry failed.
     *
     * @param admin admin
     * @param target target
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static Archive backup(final MysqlProtocolAdmin admin, final Path target) {
        if (Files.exists(target)) {
            throw new IllegalArgumentException("backup target already exists");
        }
        var checkpoint = admin.freeze();
        return admin.locked(() -> {
            admin.requireFrozen(checkpoint);
            if (Files.exists(target)) {
                throw new IllegalArgumentException("backup target already exists");
            }
            Path absolute = target.toAbsolutePath();
            Path temporary = null;
            try {
                Files.createDirectories(absolute.getParent());
                temporary = Files.createTempFile(absolute.getParent(), ".azir-backup-", ".partial");
                MessageDigest digest = digest();
                long rows = 0;
                try (var file = Files.newOutputStream(temporary); var hashed = new DigestOutputStream(file, digest); var out = new DataOutputStream(new BufferedOutputStream(hashed))) {
                    out.writeInt(MAGIC);
                    out.writeInt(FORMAT);
                    out.writeUTF(admin.state().prefix());
                    out.writeUTF(checkpoint.namespace());
                    out.writeLong(checkpoint.generation());
                    out.writeUTF(checkpoint.id());
                    Map<String, DataSource> roles = roles(admin);
                    out.writeInt(roles.size());
                    for (var role : roles.entrySet()) {
                        admin.verifyGate();
                        out.writeUTF(role.getKey());
                        var tables = tables(admin.state().prefix(), role.getKey());
                        out.writeInt(tables.size());
                        for (String table : tables) {
                            admin.verifyGate();
                            rows = Math.addExact(rows, writeTable(out, role.getValue(), table, role.getKey(), false));
                        }
                    }
                }
                String sha = HexFormat.of().formatHex(digest.digest());
                try (var append = new DataOutputStream(Files.newOutputStream(temporary, StandardOpenOption.APPEND))) {
                    append.writeUTF(sha);
                }
                // Only the final file name will be released after it has been brushed to avoid a process collapse leaving half-backs that look complete.
                try (var channel = java.nio.channels.FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                // Control connection during export or leader failover cannot be published as a frozen backup when it is possible to mix in subsequent GC files.
                admin.verifyGate();
                admin.requireFrozen(checkpoint);
                // A hard link to a directory is an uncoverable atomic release; neither the two different space concurrent backups can cover the same file.
                Files.createLink(absolute, temporary);
                try (var parent = java.nio.channels.FileChannel.open(absolute.getParent(), StandardOpenOption.READ)) {
                    parent.force(true);
                }
                return new Archive(absolute, sha, admin.state().prefix(), checkpoint, rows, null);
            } catch (final IOException | SQLException failure) {
                throw new IllegalStateException("consistent backup failed; deployment remains frozen", failure);
            } finally {
                if (temporary != null) {
                    try {
                        Files.deleteIfExists(temporary);
                    } catch (final IOException ignored) {
                        /*
                         * A temporary file is not issued as an recovery backup.
                         */ }
                }
            }
        });
    }

    /**
     * Only check the layout and empty space of an existing target, do not freeze the target, do not create recovery logs or protocol tables.
     *
     * @param archive archive
     * @param expectedSha expected sha
     * @param initializedTarget initialized target
     * @return the operation result
     */
    public static Archive checkStage(final Path archive, final String expectedSha, final MysqlProtocolAdmin initializedTarget) {
        initializedTarget.requireWritable();
        requireExpectedSha(expectedSha);
        var inspection = inspectLayout(archive, expectedSha);
        requireStageTarget(inspection, initializedTarget);
        requireEmpty(initializedTarget);
        return inspection.archive();
    }

    /**
     * For the first time, recovery still requires empty targets and any role or layout mismatch is denied before freezing.
     *
     * @param archive archive
     * @param expectedSha expected sha
     * @param emptyTarget empty target
     * @return the operation result
     */
    public static Archive stage(final Path archive, final String expectedSha, final MysqlProtocolAdmin emptyTarget) {
        emptyTarget.requireWritable();
        requireExpectedSha(expectedSha);
        var inspection = inspectLayout(archive, expectedSha);
        requireStageTarget(inspection, emptyTarget);
        var header = inspection.archive();
        try (var restore = MysqlProtocolRestore.start(emptyTarget, header, () -> requireEmpty(emptyTarget))) {
            return importArchive(archive, expectedSha, header, restore);
        }
    }

    /**
     * Using the same file and physical path replay failed, pre-screening errors do not add tasks to generation.
     *
     * @param archive archive
     * @param expectedSha expected sha
     * @param prefix prefix
     * @param control control
     * @param shards shards
     * @return the operation result
     */
    public static Archive resumeStage(final Path archive, final String expectedSha, final String prefix, final DataSource control, final Map<String, DataSource> shards) {
        requireExpectedSha(expectedSha);
        var inspection = inspectLayout(archive, expectedSha);
        var header = inspection.archive();
        var roles = new LinkedHashMap<String, DataSource>();
        roles.put("control", control);
        new TreeMap<>(shards).forEach((id, source) -> roles.put("shard:" + id, source));
        requireRoleIdentity(inspection, prefix, roles);
        try (var restore = MysqlProtocolRestore.resume(prefix, control, shards, header, () -> requireCompatible(inspection, prefix, roles))) {
            if (restore.complete()) {
                return new Archive(archive.toAbsolutePath(), expectedSha, prefix, header.checkpoint(), header.rows(), header.checkpoint().namespace());
            }
            return importArchive(archive, expectedSha, header, restore);
        }
    }

    /**
     * The first recovery must use another complete namespace and cannot overwrite the source time line.
     *
     * @param inspection inspection
     * @param target target
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static void requireStageTarget(final Inspection inspection, final MysqlProtocolAdmin target) {
        if (!inspection.archive().prefix().equals(target.state().prefix()) || inspection.archive().checkpoint().namespace().equals(target.state().namespace())) {
            throw new IllegalArgumentException("restore requires another pristine namespace with the same protocol");
        }
        var current = target.state().status();
        if (MysqlProtocolAdmin.transitionPending(current)) {
            throw new IllegalStateException("target has an unfinished administration intent");
        }
        if (!current.namespace().equals(target.state().namespace()) || current.generation() != target.state().generation()
                || target.status().values().stream()
                        .anyMatch(role -> !role.namespace().equals(current.namespace()) || role.generation() != current.generation() || !Set.of("ACTIVE", "FROZEN").contains(role.mode()))) {
            throw new IllegalStateException("restore requires the current active or frozen pristine target");
        }
        if (MysqlProtocolRestore.status(target.state().control(), target.state().prefix()) != null) {
            throw new IllegalStateException("restore job already exists; use resume-stage");
        }
        requireCompatible(inspection, target.state().prefix(), roles(target));
    }

    /**
     * Pre-review column by column compares the already existing target table; only fixed tabulations are used and no DDL provided in any document is executed.
     *
     * @param inspection inspection
     * @param prefix prefix
     * @param target target
     * @throws SQLException if the operation cannot complete
     */
    private static void requireCompatible(final Inspection inspection, final String prefix, final Map<String, DataSource> target) {
        requireRoleIdentity(inspection, prefix, target);
        for (var role : inspection.roles().entrySet()) {
            MysqlProtocolState.checkInterrupted();
            try (var c = target.get(role.getKey()).getConnection()) {
                for (var table : role.getValue()) {
                    MysqlProtocolSchema.requireTable(c, table.table());
                    try (var q = c.createStatement(); var r = q.executeQuery("select * from " + table.table() + " limit 0")) {
                        var metadata = r.getMetaData();
                        if (metadata.getColumnCount() != table.columns().size()) {
                            throw new SQLException("restore schema column count differs: " + table.table());
                        }
                        for (int index = 0; index < table.columns().size(); index++) {
                            var column = table.columns().get(index);
                            if (!column.name().equals(metadata.getColumnName(index + 1)) || column.category() != category(metadata.getColumnType(index + 1))) {
                                throw new SQLException("restore schema column layout differs: " + table.table());
                            }
                        }
                    }
                }
            } catch (final SQLException failure) {
                throw storage(failure);
            }
        }
    }

    /**
     * Role identity fully matches to allow query targets and cannot be compared only with the number of shard.
     *
     * @param inspection inspection
     * @param prefix prefix
     * @param target target
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireRoleIdentity(final Inspection inspection, final String prefix, final Map<String, DataSource> target) {
        if (!inspection.archive().prefix().equals(prefix) || !inspection.roles().keySet().equals(target.keySet())) {
            throw new IllegalArgumentException("restore protocol or complete shard identities differ");
        }
    }

    /**
     * The writing portal must provide a clear external summary and cannot automatically trust the document ' s own summary.
     *
     * @param expectedSha expected sha
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireExpectedSha(final String expectedSha) {
        if (expectedSha == null || !expectedSha.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("expected SHA-256 required");
        }
    }

    /**
     * Fixed schedule complete replay; reread validate for each execution and progress log cannot skip data not yet verify.
     *
     * @param archive archive
     * @param expectedSha expected sha
     * @param header header
     * @param restore restore
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    private static Archive importArchive(final Path archive, final String expectedSha, final Archive header, final MysqlProtocolRestore.Session restore) {
        MessageDigest fileDigest = digest();
        try (var hashed = new DigestInputStream(new BufferedInputStream(Files.newInputStream(archive)), fileDigest); var in = new DataInputStream(hashed)) {
            if (in.readInt() != MAGIC || in.readInt() != FORMAT) {
                throw new IOException("unsupported backup format");
            }
            String prefix = in.readUTF();
            String namespace = in.readUTF();
            long generation = in.readLong();
            String id = in.readUTF();
            if (!prefix.equals(restore.prefix) || !namespace.equals(header.checkpoint().namespace()) || generation != header.checkpoint().generation() || !id.equals(header.checkpoint().id())) {
                throw new IOException("backup identity changed after verification");
            }
            if (in.readInt() != restore.roles.size()) {
                throw new IOException("shard count differs");
            }
            Set<String> seenRoles = new HashSet<>();
            long rows = 0;
            for (int roleIndex = 0; roleIndex < restore.roles.size(); roleIndex++) {
                String role = in.readUTF();
                DataSource source = restore.roles.get(role);
                if (source == null || !seenRoles.add(role)) {
                    throw new IOException("unknown or duplicate role");
                }
                List<String> expected = tables(prefix, role);
                if (in.readInt() != expected.size()) {
                    throw new IOException("table count differs");
                }
                for (String table : expected) {
                    rows = Math.addExact(rows, readTable(in, source, table, role, restore));
                    restore.progress(role, table);
                }
            }
            // An entire paragraph of the document that is actually read during import cannot be published while the file has been replaced or changed in place.
            String actualSha = HexFormat.of().formatHex(fileDigest.digest());
            hashed.on(false);
            if (!expectedSha.equals(actualSha) || !expectedSha.equals(in.readUTF()) || in.read() != -1 || rows != header.rows()) {
                throw new IOException("backup changed during restore");
            }
            String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
            restore.local(restore.control, "control", c -> {
                try (var q = c.prepareStatement("update " + config + " set routes=? where id=1")) {
                    q.setString(1, restore.routes);
                    if (q.executeUpdate() != 1) {
                        throw new SQLException("restored configuration missing");
                    }
                }
            });
            restore.requireHeld();
            var state = MysqlProtocolState.existing(prefix, namespace, restore.control, restore.shards);
            new MysqlProtocolValidator(new MysqlProtocolAdmin(state, ignored -> 0)).verify();
            restore.finish();
            return new Archive(archive.toAbsolutePath(), expectedSha, prefix, header.checkpoint(), rows, namespace);
        } catch (final IOException | SQLException failure) {
            throw new IllegalStateException("restore incomplete; use resume-stage with the same archive and destination", failure);
        }
    }

    /**
     * Recovery becomes new namespace when the original authority cannot be reached, making it clear that the old authority which is still possible to live in isolation does not pretend to
     * be a non-destructive automatic leader failover.
     *
     * @param staged staged
     * @return the operation result
     */
    public static String activateAsNewNamespace(final MysqlProtocolAdmin staged) {
        staged.requireWritable();
        return staged.locked(() -> {
            var status = staged.state().status();
            String saved = status.destination();
            if (status.mode().equals("ACTIVE") && saved != null && saved.startsWith("new-namespace:")) {
                String[] parts = saved.split(":");
                if (parts.length == 4 && parts[2].equals(status.namespace()) && staged.status().values().stream().allMatch(s -> s.mode().equals("ACTIVE") && s.namespace().equals(parts[2])
                        && s.generation() == status.generation() && Objects.equals(s.checkpoint(), status.checkpoint()) && Objects.equals(s.destination(), saved))) {
                    return parts[2];
                }
            }
            if (!status.mode().equals("STAGED")) {
                throw new IllegalStateException("new namespace activation requires STAGED data");
            }
            String prefix = staged.state().prefix();
            String original, namespace, plan;
            if (saved != null && saved.startsWith("new-namespace:")) {
                String[] parts = saved.split(":");
                if (parts.length != 4) {
                    throw new IllegalStateException("invalid namespace activation plan");
                }
                original = parts[1];
                namespace = parts[2];
                plan = saved;
            } else {
                if (saved == null || !saved.startsWith("archive:")) {
                    throw new IllegalStateException("staged archive identity missing");
                }
                original = status.namespace();
                namespace = UUID.randomUUID().toString();
                plan = "new-namespace:" + original + ":" + namespace + ":" + saved.substring(8);
                staged.change(staged.state().control(), "control", status.generation(), "STAGED", status.checkpoint(), plan);
            }
            UUID.fromString(original);
            UUID.fromString(namespace);
            for (var role : roles(staged).entrySet()) {
                try (var c = role.getValue().getConnection()) {
                    c.setAutoCommit(false);
                    try {
                        try (var q = c.prepareStatement("select namespace,generation,mode from " + prefix + "_runtime where role=? for update")) {
                            q.setString(1, role.getKey());
                            try (var r = q.executeQuery()) {
                                if (!r.next() || (!original.equals(r.getString(1)) && !namespace.equals(r.getString(1))) || r.getLong(2) != status.generation()
                                        || !Set.of("STAGED", "ACTIVE").contains(r.getString(3))) {
                                    throw new SQLException("incomplete namespace activation plan");
                                }
                            }
                        }
                        staged.verifyGate();
                        try (var q = c.prepareStatement("update " + prefix + "_runtime set namespace=?,destination=? where role=?")) {
                            q.setString(1, namespace);
                            q.setString(2, plan);
                            q.setString(3, role.getKey());
                            q.executeUpdate();
                        }
                        String table = role.getKey().equals("control") ? (prefix.equals("pc") ? "pc_control" : prefix + "_config") : prefix + "_binding";
                        try (var q = c.prepareStatement("update " + table + " set namespace=? where id=1 and namespace in (?,?)")) {
                            q.setString(1, namespace);
                            q.setString(2, original);
                            q.setString(3, namespace);
                            q.executeUpdate();
                        }
                        staged.verifyGate();
                        c.commit();
                    } catch (final SQLException | RuntimeException | Error failure) {
                        try {
                            c.rollback();
                        } catch (final SQLException | RuntimeException | Error rollback) {
                            if (failure != rollback) {
                                failure.addSuppressed(rollback);
                            }
                            com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
                        }
                        throw failure;
                    }
                } catch (final SQLException failure) {
                    throw storage(failure);
                }
            }
            // Enables all data characters first, and finally the control role is enabled; the same plan idempotent is confirmed after the loss is returned.
            var activate = new ArrayList<>(roles(staged).entrySet());
            activate.sort(Comparator.comparing(e -> e.getKey().equals("control")));
            for (var role : activate) {
                staged.local(role.getValue(), c -> {
                    try (var q = c.prepareStatement("select namespace,generation,mode,checkpoint,destination from " + prefix + "_runtime where role=? for update")) {
                        q.setString(1, role.getKey());
                        try (var r = q.executeQuery()) {
                            if (!r.next() || !namespace.equals(r.getString(1)) || r.getLong(2) != status.generation() || !Set.of("STAGED", "ACTIVE").contains(r.getString(3))
                                    || !Objects.equals(status.checkpoint(), r.getString(4)) || !plan.equals(r.getString(5))) {
                                throw new SQLException("namespace activation was superseded");
                            }
                        }
                    }
                    staged.verifyGate();
                    try (var q = c.prepareStatement("update " + prefix + "_runtime set mode='ACTIVE',freeze_ready=false where role=?")) {
                        q.setString(1, role.getKey());
                        q.executeUpdate();
                    }
                    staged.verifyGate();
                    return null;
                });
            }
            return namespace;
        });
    }

    /**
     * Validate does not require connection database and check for complete backup to support the return of lost confirmation after the backup is completed.
     *
     * @param file file
     * @return the operation result
     */
    public static Archive inspect(final Path file) {
        return inspectLayout(file, null).archive();
    }

    /**
     * A full verify summary and a table-by-table summary are being read in one order, with only limited layout metadata to keep memory.
     *
     * @param file file
     * @param expectedSha expected sha
     * @return the operation result
     * @throws EOFException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static Inspection inspectLayout(final Path file, final String expectedSha) {
        if (expectedSha != null && !expectedSha.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("expected SHA-256 required");
        }
        try {
            String sha;
            try (var random = new RandomAccessFile(file.toFile(), "r")) {
                if (random.length() < 66) {
                    throw new IOException("truncated archive");
                }
                random.seek(random.length() - 66);
                sha = random.readUTF();
            }
            if (!sha.matches("[0-9a-f]{64}") || (expectedSha != null && !expectedSha.equals(sha))) {
                throw new IOException("archive expected SHA-256 differs");
            }
            MessageDigest fileDigest = digest();
            byte[] buffer = new byte[65536];
            try (var hashed = new DigestInputStream(new BufferedInputStream(Files.newInputStream(file)), fileDigest); var in = new DataInputStream(hashed)) {
                if (in.readInt() != MAGIC || in.readInt() != FORMAT) {
                    throw new IOException("unsupported format");
                }
                String prefix = in.readUTF();
                if (!Set.of("pc", "omid", "atlas").contains(prefix)) {
                    throw new IOException("unknown protocol");
                }
                var token = new MysqlProtocolAdmin.Checkpoint(in.readUTF(), in.readLong(), in.readUTF());
                int roles = in.readInt();
                if (roles < 2 || roles > 1024) {
                    throw new IOException("invalid role count");
                }
                long rows = 0;
                var layout = new LinkedHashMap<String, List<TableLayout>>();
                for (int roleIndex = 0; roleIndex < roles; roleIndex++) {
                    MysqlProtocolState.checkInterrupted();
                    String role = in.readUTF();
                    if (layout.containsKey(role) || !(role.equals("control") || role.matches("shard:[A-Za-z0-9_-]{1,64}"))) {
                        throw new IOException("invalid role");
                    }
                    var tables = tables(prefix, role);
                    if (in.readInt() != tables.size()) {
                        throw new IOException("table count mismatch");
                    }
                    var tableLayouts = new ArrayList<TableLayout>();
                    for (String table : tables) {
                        if (!in.readUTF().equals(table)) {
                            throw new IOException("table mismatch");
                        }
                        int count = in.readInt();
                        if (count < 1 || count > 32) {
                            throw new IOException("column count");
                        }
                        var columns = new ArrayList<ColumnLayout>();
                        var seen = new HashSet<String>();
                        for (int column = 0; column < count; column++) {
                            String name = in.readUTF();
                            byte type = in.readByte();
                            if (!name.matches("[a-z_]{1,64}") || !seen.add(name) || type < 1 || type > 3) {
                                throw new IOException("invalid column layout");
                            }
                            columns.add(new ColumnLayout(name, type));
                        }
                        long actual = 0;
                        MessageDigest tableDigest = digest();
                        for (int length; (length = in.readInt()) != -1;) {
                            if (length < 1 || length > MAX_ROW) {
                                throw new IOException("row size");
                            }
                            int remaining = length;
                            while (remaining > 0) {
                                MysqlProtocolState.checkInterrupted();
                                int amount = in.read(buffer, 0, Math.min(buffer.length, remaining));
                                if (amount < 0) {
                                    throw new EOFException();
                                }
                                tableDigest.update(buffer, 0, amount);
                                remaining -= amount;
                            }
                            actual = Math.addExact(actual, 1);
                        }
                        if (!in.readUTF().equals(HexFormat.of().formatHex(tableDigest.digest())) || in.readLong() != actual) {
                            throw new IOException("table digest or row count differs");
                        }
                        rows = Math.addExact(rows, actual);
                        tableLayouts.add(new TableLayout(table, columns));
                    }
                    layout.put(role, tableLayouts);
                }
                String actualSha = HexFormat.of().formatHex(fileDigest.digest());
                hashed.on(false);
                if (!layout.containsKey("control") || !sha.equals(actualSha) || !sha.equals(in.readUTF()) || in.read() != -1) {
                    throw new IOException("archive SHA-256 or trailer differs");
                }
                return new Inspection(new Archive(file.toAbsolutePath(), sha, prefix, token, rows, null), layout);
            }
        } catch (final IOException failure) {
            throw new IllegalArgumentException("invalid backup archive", failure);
        }
    }

    /**
     * File summary verify before any target database is written; cut or corrupted files cannot trigger recovery.
     *
     * @param archive archive
     * @param expectedSha expected sha
     * @throws EOFException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public static void verifyFile(final Path archive, final String expectedSha) {
        requireExpectedSha(expectedSha);
        try {
            long body = Files.size(archive) - 66;
            if (body < 24) {
                throw new IOException("truncated archive");
            }
            MessageDigest digest = digest();
            try (var in = new DataInputStream(new BufferedInputStream(Files.newInputStream(archive)))) {
                byte[] buffer = new byte[65536];
                long left = body;
                while (left > 0) {
                    int count = in.read(buffer, 0, (int) Math.min(buffer.length, left));
                    if (count < 0) {
                        throw new EOFException();
                    }
                    digest.update(buffer, 0, count);
                    left -= count;
                }
                if (!expectedSha.equals(HexFormat.of().formatHex(digest.digest())) || !expectedSha.equals(in.readUTF()) || in.read() != -1) {
                    throw new IOException("backup SHA-256 mismatch");
                }
            }
        } catch (final IOException failure) {
            throw new IllegalArgumentException("invalid backup archive", failure);
        }
    }

    /**
     * Known protocol table layout; recovery does not accept CREATE TABLE or any name.
     *
     * @param prefix prefix
     * @param role role
     * @return the operation result
     */
    static List<String> tables(final String prefix, final String role) {
        if (role.equals("control")) {
            return prefix.equals("pc") ? List.of("pc_control", "pc_readers", "mvcc_tso", "pc_runtime")
                    : List.of(prefix + "_config", prefix + "_sequence", prefix + "_txn", prefix + "_conflicts", prefix + "_gc", prefix + "_runtime");
        }
        return prefix.equals("pc") ? List.of("pc_binding", "pc_guard", "pc_data", "pc_lock", "pc_write", "pc_rollback", "pc_runtime")
                : List.of(prefix + "_binding", prefix + "_keys", prefix + "_versions", prefix + "_runtime");
    }

    /**
     * Sets a role order, and when the control library shares physical database with an shard, runtime exports only each one.
     *
     * @param admin admin
     * @return the operation result
     */
    static Map<String, DataSource> roles(final MysqlProtocolAdmin admin) {
        var roles = new LinkedHashMap<String, DataSource>();
        roles.put("control", admin.state().control());
        new TreeMap<>(admin.state().shards()).forEach((id, source) -> roles.put("shard:" + id, source));
        return roles;
    }

    /**
     * For the first time, recovery is prohibited to overwrite non-empty data; only the same registered recovery task allows replay.
     *
     * @param admin admin
     * @throws IllegalArgumentException if the operation cannot complete
     */
    private static void requireEmpty(final MysqlProtocolAdmin admin) {
        String prefix = admin.state().prefix();
        for (var role : roles(admin).entrySet()) {
            for (String table : tables(prefix, role.getKey())) {
                if (table.endsWith("_runtime") || table.endsWith("_config") || table.equals("pc_control") || table.endsWith("_binding") || table.endsWith("_sequence") || table.equals("mvcc_tso")
                        || table.endsWith("_gc")) {
                    continue;
                }
                if (MysqlProtocolAdmin.scalar(role.getValue(), "select exists(select 1 from " + table + " limit 1)") != 0) {
                    throw new IllegalArgumentException("restore destination is not empty: " + table);
                }
            }
        }
    }

    /**
     * Fluid export to determine the order of a table, single-line payload protected by a fixed ceiling.
     *
     * @param out out
     * @param source database connection source
     * @param table table
     * @param role role
     * @param restoring restoring
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static long writeTable(final DataOutputStream out, final DataSource source, final String table, final String role, final boolean restoring) throws SQLException, IOException {
        try (var c = source.getConnection(); var q = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            MysqlProtocolSchema.requireTable(c, table);
            q.setFetchSize(Integer.MIN_VALUE);
            try (var r = q.executeQuery(select(c, table, role))) {
                var meta = r.getMetaData();
                int columns = meta.getColumnCount();
                out.writeUTF(table);
                out.writeInt(columns);
                byte[] categories = new byte[columns];
                String[] names = new String[columns];
                for (int i = 0; i < columns; i++) {
                    names[i] = meta.getColumnName(i + 1);
                    categories[i] = category(meta.getColumnType(i + 1));
                    out.writeUTF(names[i]);
                    out.writeByte(categories[i]);
                }
                MessageDigest hash = digest();
                long rows = 0;
                while (r.next()) {
                    MysqlProtocolState.checkInterrupted();
                    byte[] encoded = encode(r, names, categories, restoring && table.endsWith("_runtime"));
                    if (encoded.length > MAX_ROW) {
                        throw new IOException("backup row exceeds bounded format");
                    }
                    out.writeInt(encoded.length);
                    out.write(encoded);
                    hash.update(encoded);
                    rows++;
                }
                out.writeInt(-1);
                out.writeUTF(HexFormat.of().formatHex(hash.digest()));
                out.writeLong(rows);
                return rows;
            }
        }
    }

    /**
     * Database has a layout to check listings and categories, then bind parameters recovery and do not collage SQL in files.
     *
     * @param in in
     * @param source database connection source
     * @param expectedTable expected table
     * @param role role
     * @param restore restore
     * @return the operation result
     * @throws EOFException if the operation cannot complete
     * @throws IOException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static long readTable(final DataInputStream in, final DataSource source, final String expectedTable, final String role,
                                  final MysqlProtocolRestore.Session restore) throws IOException, SQLException {
        if (!expectedTable.equals(in.readUTF())) {
            throw new IOException("unexpected backup table");
        }
        int columns = in.readInt();
        if (columns < 1 || columns > 32) {
            throw new IOException("invalid column count");
        }
        String expectedDigest;
        long expectedCount;
        long count = 0;
        try (var c = source.getConnection()) {
            MysqlProtocolSchema.requireTable(c, expectedTable);
            String[] names = new String[columns];
            byte[] categories = new byte[columns];
            try (var q = c.createStatement(); var r = q.executeQuery("select * from " + expectedTable + " limit 0")) {
                var meta = r.getMetaData();
                if (columns != meta.getColumnCount()) {
                    throw new IOException("schema column count differs");
                }
                for (int i = 0; i < columns; i++) {
                    names[i] = in.readUTF();
                    categories[i] = in.readByte();
                    if (!names[i].equals(meta.getColumnName(i + 1)) || categories[i] != category(meta.getColumnType(i + 1)) || !names[i].matches("[a-z_]+")) {
                        throw new IOException("backup schema differs");
                    }
                }
            }
            boolean runtime = expectedTable.endsWith("_runtime");
            boolean atomic = runtime || expectedTable.endsWith("_config") || expectedTable.equals("pc_control") || expectedTable.endsWith("_binding") || expectedTable.endsWith("_sequence")
                    || expectedTable.equals("mvcc_tso") || expectedTable.endsWith("_gc");
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            c.setAutoCommit(false);
            try {
                restore.fence(c, role);
                if (atomic) {
                    try (var q = c.prepareStatement("delete from " + expectedTable + (runtime ? " where role=?" : ""))) {
                        if (runtime) {
                            q.setString(1, role);
                        }
                        q.executeUpdate();
                    }
                } else {
                    // Emptys the large table in the same batch commit, avoiding retry manufacturing unbounded undo and long-term monopolization of recovery fence.
                    int deleted;
                    do {
                        try (var q = c.createStatement()) {
                            deleted = q.executeUpdate("delete from " + expectedTable + " limit 256");
                        }
                        c.commit();
                        restore.fence(c, role);
                    } while (deleted == 256);
                }
                String sql = "insert into " + expectedTable + "(" + String.join(",", names) + ") values(" + String.join(",", Collections.nCopies(columns, "?")) + ")";
                MessageDigest digest = digest();
                int pendingRows = 0;
                long pendingBytes = 0;
                boolean needsFence = false;
                try (var insert = c.prepareStatement(sql)) {
                    for (int length; (length = in.readInt()) != -1;) {
                        MysqlProtocolState.checkInterrupted();
                        if (needsFence) {
                            restore.fence(c, role);
                            needsFence = false;
                        }
                        if (length < 1 || length > MAX_ROW) {
                            throw new IOException("invalid row length");
                        }
                        byte[] row = in.readNBytes(length);
                        if (row.length != length) {
                            throw new EOFException();
                        }
                        digest.update(row);
                        try (var values = new DataInputStream(new ByteArrayInputStream(row))) {
                            for (int i = 0; i < columns; i++) {
                                if (!values.readBoolean()) {
                                    insert.setNull(i + 1, Types.NULL);
                                } else if (categories[i] == 1) {
                                    insert.setLong(i + 1, values.readLong());
                                } else {
                                    int size = values.readInt();
                                    if (size < 0 || size > MAX_FIELD) {
                                        throw new IOException("invalid cell length");
                                    }
                                    byte[] value = values.readNBytes(size);
                                    if (value.length != size) {
                                        throw new EOFException();
                                    }
                                    if (categories[i] == 2) {
                                        insert.setString(i + 1, runtime && names[i].equals("mode") ? "RESTORING" : new String(value, java.nio.charset.StandardCharsets.UTF_8));
                                    } else {
                                        insert.setBytes(i + 1, value);
                                    }
                                }
                            }
                            if (values.read() != -1) {
                                throw new IOException("extra row bytes");
                            }
                        }
                        insert.executeUpdate();
                        count++;
                        pendingRows++;
                        pendingBytes += row.length;
                        if (!atomic && (pendingRows >= 256 || pendingBytes >= 8L * 1024 * 1024)) {
                            c.commit();
                            pendingRows = 0;
                            pendingBytes = 0;
                            needsFence = true;
                        }
                    }
                }
                expectedDigest = in.readUTF();
                expectedCount = in.readLong();
                if (!expectedDigest.equals(HexFormat.of().formatHex(digest.digest())) || count != expectedCount) {
                    throw new IOException("table digest differs");
                }
                if (runtime && count != 1) {
                    throw new IOException("runtime role must contain exactly one fence row");
                }
                c.commit();
            } catch (final IOException | SQLException | RuntimeException | Error failure) {
                try {
                    c.rollback();
                } catch (final SQLException | RuntimeException | Error rollback) {
                    if (failure != rollback) {
                        failure.addSuppressed(rollback);
                    }
                    com.flyingzc.azir.control.MysqlNamedLock.discard(c, failure);
                }
                throw failure;
            }
        }
        // Releasing connection after re-reading is visible, avoiding the use of an additional written pool slot for return reading.
        restore.requireHeld();
        var sink = new TableDigestStream();
        try (var out = new DataOutputStream(sink)) {
            writeTable(out, source, expectedTable, role, true);
        }
        if (!expectedDigest.equals(sink.tableSha) || expectedCount != sink.rows) {
            throw new IOException("restored database readback differs");
        }
        return count;
    }

    /**
     * Exportes in the main key order to ensure that files and database are rereadable for comparison.
     *
     * @param c c
     * @param table table
     * @param role role
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static String select(final Connection c, final String table, final String role) throws SQLException {
        var keys = new TreeMap<Short, String>();
        try (var r = c.getMetaData().getPrimaryKeys(c.getCatalog(), null, table)) {
            while (r.next()) {
                keys.put(r.getShort("KEY_SEQ"), r.getString("COLUMN_NAME"));
            }
        }
        if (keys.isEmpty()) {
            throw new SQLException("backup table has no primary key");
        }
        return "select * from " + table + (table.endsWith("_runtime") ? " where role='" + role + "'" : "") + " order by " + String.join(",", keys.values());
    }

    /**
     * Sets the code for a fixed protocol field, the number is uniform to a long integer and the binary value is not converted by character.
     *
     * @param r r
     * @param names names
     * @param categories categories
     * @param restoring restoring
     * @return the operation result
     * @throws IOException if the operation cannot complete
     * @throws SQLException if the operation cannot complete
     */
    private static byte[] encode(final ResultSet r, final String[] names, final byte[] categories, final boolean restoring) throws SQLException, IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            for (int i = 0; i < categories.length; i++) {
                if (categories[i] == 1) {
                    long number = r.getLong(i + 1);
                    boolean present = !r.wasNull();
                    out.writeBoolean(present);
                    if (present) {
                        out.writeLong(number);
                    }
                } else {
                    byte[] data;
                    if (categories[i] == 2) {
                        String value = r.getString(i + 1);
                        data = value == null ? null : (restoring && names[i].equals("mode") ? "FROZEN" : value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    } else {
                        // GetObject payload as a whole; binary field read-only take and limit bytes once for the NULL check.
                        try (var value = r.getBinaryStream(i + 1)) {
                            data = value == null ? null : value.readNBytes(MAX_FIELD + 1);
                        }
                    }
                    out.writeBoolean(data != null);
                    if (data == null) {
                        continue;
                    }
                    if (data.length > MAX_FIELD) {
                        throw new IOException("cell exceeds bounded backup format");
                    }
                    out.writeInt(data.length);
                    out.write(data);
                }
            }
        }
        return bytes.toByteArray();
    }

    /**
     * The current protocol contains only integers, strings and binary fields. Other types reject implicit conversion.
     *
     * @param type type
     * @return the operation result
     * @throws SQLException if the operation cannot complete
     */
    private static byte category(final int type) throws SQLException {
        return switch (type) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.BOOLEAN -> 1;
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> 2;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> 3;
            default -> throw new SQLException("unsupported backup column type " + type);
        };
    }

    /**
     * Unified selection SHA-256, the missing algorithm is an error in running time configuration.
     *
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * A backup to file summary validate or a temporary recovery result.
     *
     * @param file file
     * @param sha256 sha256
     * @param prefix prefix
     * @param checkpoint checkpoint
     * @param rows rows
     * @param restoredNamespace restored namespace
     */
    public record Archive(Path file, String sha256, String prefix, MysqlProtocolAdmin.Checkpoint checkpoint,
            long rows, String restoredNamespace) {
    }

    /**
     * A fixed name and a standardized type of backup column does not save operational data.
     *
     * @param name name
     * @param category category
     */
    private record ColumnLayout(String name, byte category) {
    }

    /**
     * Limited layout of the table to prevent external changes in pre-screening results.
     *
     * @param table table
     * @param columns columns
     */
    private record TableLayout(String table, List<ColumnLayout> columns) {

        /**
         * The pre-screening layout is immutable as soon as it is created.
         *
         * @param table table
         * @param columns columns
         */
        private TableLayout {
            columns = List.copyOf(columns);
        }
    }

    /**
     * Offline file check results and limited metadata saved by role.
     *
     * @param archive archive
     * @param roles roles
     */
    private record Inspection(Archive archive, Map<String, List<TableLayout>> roles) {

        /**
         * The role and table lists are fixed by the check results, and subsequent import is re-engineered with the verify actual file.
         *
         * @param archive archive
         * @param roles roles
         */
        private Inspection {
            var copy = new LinkedHashMap<String, List<TableLayout>>();
            roles.forEach((role, tables) -> copy.put(role, List.copyOf(tables)));
            roles = Collections.unmodifiableMap(copy);
        }
    }

    /**
     * Only parses the table to export a tail summary and reads back verify without loading the entire table into memory.
     */
    private static final class TableDigestStream extends OutputStream {

        private final java.io.PipedInputStream input = new java.io.PipedInputStream(65536);

        private final java.io.PipedOutputStream output;

        private final Thread reader;

        private volatile IOException failure;

        private String tableSha;

        private long rows;

        /**
         * Use a limited buffer to parse the export format, and only summary and line numbers are reserved for individual tables.
         *
         * @throws IOException if the operation cannot complete
         */
        TableDigestStream() throws IOException {
            output = new java.io.PipedOutputStream(input);
            reader = Thread.ofVirtual().start(() -> {
                try (var in = new DataInputStream(input)) {
                    in.readUTF();
                    int columns = in.readInt();
                    for (int i = 0; i < columns; i++) {
                        in.readUTF();
                        in.readByte();
                    }
                    for (int size; (size = in.readInt()) != -1;) {
                        in.skipNBytes(size);
                    }
                    tableSha = in.readUTF();
                    rows = in.readLong();
                } catch (final IOException problem) {
                    failure = problem;
                }
            });
        }

        /**
         * Writes an bounded conduit for the solver.
         *
         * @param value value to write
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final int value) throws IOException {
            output.write(value);
        }

        /**
         * Batch Write to Avoid Bytes Distribution.
         *
         * @param data data
         * @param offset offset
         * @param length length
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void write(final byte[] data, final int offset, final int length) throws IOException {
            output.write(data, offset, length);
        }

        /**
         * Waits for the format to resolve and maintains the anomaly.
         *
         * @throws IOException if the operation cannot complete
         */
        @Override
        public void close() throws IOException {
            try {
                output.close();
            } finally {
                try {
                    reader.join(10_000);
                    if (reader.isAlive()) {
                        input.close();
                        reader.interrupt();
                        throw new IOException("table digest parser did not stop");
                    }
                } catch (final InterruptedException problem) {
                    Thread.currentThread().interrupt();
                    input.close();
                    reader.interrupt();
                    throw new IOException("table digest parser interrupted", problem);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
