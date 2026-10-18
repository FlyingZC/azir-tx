package mvcc.operations;

import javax.sql.DataSource;
import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.sql.*;
import java.util.*;
import static mvcc.operations.MysqlProtocolState.storage;

/** 冻结检查点的可移植流式备份；恢复只写已知表和列，不执行备份中的任意 SQL。 */
public final class MysqlProtocolBackup {
    private static final int MAGIC = 0x415A4232;
    private static final int FORMAT = 2;
    private static final int MAX_FIELD = 64 * 1024 * 1024;
    private static final int MAX_ROW = 96 * 1024 * 1024;
    /** 一个经过文件摘要校验的备份或已暂存恢复结果。 */
    public record Archive(Path file, String sha256, String prefix, MysqlProtocolAdmin.Checkpoint checkpoint,
                          long rows, String restoredNamespace) { }
    /** 备份列的固定名称和归一化类型，不保存业务数据。 */
    private record ColumnLayout(String name, byte category) { }
    /** 单表的有限布局，防止外部修改预检结果。 */
    private record TableLayout(String table, List<ColumnLayout> columns) {
        /** 预检布局一经创建就不可改变。 */
        private TableLayout { columns = List.copyOf(columns); }
    }
    /** 离线文件检查结果与按角色保存的有限元数据。 */
    private record Inspection(Archive archive, Map<String, List<TableLayout>> roles) {
        /** 角色与表列表都按检查结果固定，后续导入仍重新验证实际文件。 */
        private Inspection {
            var copy = new LinkedHashMap<String, List<TableLayout>>(); roles.forEach((role, tables) -> copy.put(role, List.copyOf(tables)));
            roles = Collections.unmodifiableMap(copy);
        }
    }
    /** 禁止实例化无状态工具。 */
    private MysqlProtocolBackup() { }

    /** 冻结后导出；文件完成前只存在临时文件，失败仍保持数据库冻结供操作员重试。 */
    public static Archive backup(MysqlProtocolAdmin admin, Path target) {
        if (Files.exists(target)) { throw new IllegalArgumentException("backup target already exists"); }
        var checkpoint = admin.freeze();
        return admin.locked(() -> {
            admin.requireFrozen(checkpoint);
            if (Files.exists(target)) { throw new IllegalArgumentException("backup target already exists"); }
            Path absolute = target.toAbsolutePath(); Path temporary = null;
            try {
                Files.createDirectories(absolute.getParent()); temporary = Files.createTempFile(absolute.getParent(), ".azir-backup-", ".partial");
                MessageDigest digest = digest(); long rows = 0;
                try (var file = Files.newOutputStream(temporary); var hashed = new DigestOutputStream(file, digest); var out = new DataOutputStream(new BufferedOutputStream(hashed))) {
                    out.writeInt(MAGIC); out.writeInt(FORMAT); out.writeUTF(admin.state().prefix()); out.writeUTF(checkpoint.namespace()); out.writeLong(checkpoint.generation()); out.writeUTF(checkpoint.id());
                    Map<String, DataSource> roles = roles(admin); out.writeInt(roles.size());
                    for (var role : roles.entrySet()) {
                        admin.verifyGate(); out.writeUTF(role.getKey());
                        var tables = tables(admin.state().prefix(), role.getKey()); out.writeInt(tables.size());
                        for (String table : tables) { admin.verifyGate(); rows = Math.addExact(rows, writeTable(out, role.getValue(), table, role.getKey(), false)); }
                    }
                }
                String sha = HexFormat.of().formatHex(digest.digest());
                try (var append = new DataOutputStream(Files.newOutputStream(temporary, StandardOpenOption.APPEND))) { append.writeUTF(sha); }
                // 刷盘后才公布最终文件名，避免进程崩溃留下外观完整的半备份。
                try (var channel = java.nio.channels.FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
                // 导出期间控制连接被杀或发生换主时，不能把可能混入后续 GC 的文件公布为冻结备份。
                admin.verifyGate(); admin.requireFrozen(checkpoint);
                // 同目录硬链接是不可覆盖的原子发布；两个不同空间并发备份也不能覆盖同一个文件。
                Files.createLink(absolute, temporary);
                try (var parent = java.nio.channels.FileChannel.open(absolute.getParent(), StandardOpenOption.READ)) { parent.force(true); }
                return new Archive(absolute, sha, admin.state().prefix(), checkpoint, rows, null);
            } catch (IOException | SQLException failure) { throw new IllegalStateException("consistent backup failed; deployment remains frozen", failure); }
            finally { if (temporary != null) { try { Files.deleteIfExists(temporary); } catch (IOException ignored) { /* 临时文件不作为可恢复备份发布。 */ } } }
        });
    }
    /** 只检查既有目标的布局和空空间，不冻结目标、不创建恢复日志或协议表。 */
    public static Archive checkStage(Path archive, String expectedSha, MysqlProtocolAdmin initializedTarget) {
        initializedTarget.requireWritable(); requireExpectedSha(expectedSha); var inspection = inspectLayout(archive, expectedSha);
        requireStageTarget(inspection, initializedTarget); requireEmpty(initializedTarget); return inspection.archive();
    }
    /** 首次恢复仍要求空目标，任何角色和布局不匹配都在冻结之前拒绝。 */
    public static Archive stage(Path archive, String expectedSha, MysqlProtocolAdmin emptyTarget) {
        emptyTarget.requireWritable(); requireExpectedSha(expectedSha); var inspection = inspectLayout(archive, expectedSha);
        requireStageTarget(inspection, emptyTarget); var header = inspection.archive();
        try (var restore = MysqlProtocolRestore.start(emptyTarget, header, () -> requireEmpty(emptyTarget))) {
            return importArchive(archive, expectedSha, header, restore);
        }
    }
    /** 使用相同文件和物理路由重放未完成任务，预检错误不会递增任务执行代次。 */
    public static Archive resumeStage(Path archive, String expectedSha, String prefix, DataSource control, Map<String, DataSource> shards) {
        requireExpectedSha(expectedSha); var inspection = inspectLayout(archive, expectedSha); var header = inspection.archive();
        var roles = new LinkedHashMap<String, DataSource>(); roles.put("control", control); new TreeMap<>(shards).forEach((id, source) -> roles.put("shard:" + id, source));
        requireRoleIdentity(inspection, prefix, roles);
        try (var restore = MysqlProtocolRestore.resume(prefix, control, shards, header, () -> requireCompatible(inspection, prefix, roles))) {
            if (restore.complete()) { return new Archive(archive.toAbsolutePath(), expectedSha, prefix, header.checkpoint(), header.rows(), header.checkpoint().namespace()); }
            return importArchive(archive, expectedSha, header, restore);
        }
    }
    /** 首次恢复必须使用另一个完整命名空间，不能覆盖源时间线。 */
    private static void requireStageTarget(Inspection inspection, MysqlProtocolAdmin target) {
        if (!inspection.archive().prefix().equals(target.state().prefix()) || inspection.archive().checkpoint().namespace().equals(target.state().namespace())) { throw new IllegalArgumentException("restore requires another pristine namespace with the same protocol"); }
        var current = target.state().status();
        if (MysqlProtocolAdmin.transitionPending(current)) { throw new IllegalStateException("target has an unfinished administration intent"); }
        if (!current.namespace().equals(target.state().namespace()) || current.generation() != target.state().generation()
                || target.status().values().stream().anyMatch(role -> !role.namespace().equals(current.namespace()) || role.generation() != current.generation() || !Set.of("ACTIVE", "FROZEN").contains(role.mode()))) { throw new IllegalStateException("restore requires the current active or frozen pristine target"); }
        if (MysqlProtocolRestore.status(target.state().control(), target.state().prefix()) != null) { throw new IllegalStateException("restore job already exists; use resume-stage"); }
        requireCompatible(inspection, target.state().prefix(), roles(target));
    }
    /** 预检逐列比较既有目标表；只使用固定表名，不执行任何文件提供的 DDL。 */
    private static void requireCompatible(Inspection inspection, String prefix, Map<String, DataSource> target) {
        requireRoleIdentity(inspection, prefix, target);
        for (var role : inspection.roles().entrySet()) {
            MysqlProtocolState.checkInterrupted();
            try (var c = target.get(role.getKey()).getConnection()) {
                for (var table : role.getValue()) {
                    MysqlProtocolSchema.requireTable(c, table.table());
                    try (var q = c.createStatement(); var r = q.executeQuery("select * from " + table.table() + " limit 0")) {
                        var metadata = r.getMetaData();
                        if (metadata.getColumnCount() != table.columns().size()) { throw new SQLException("restore schema column count differs: " + table.table()); }
                        for (int index = 0; index < table.columns().size(); index++) {
                            var column = table.columns().get(index);
                            if (!column.name().equals(metadata.getColumnName(index + 1)) || column.category() != category(metadata.getColumnType(index + 1))) { throw new SQLException("restore schema column layout differs: " + table.table()); }
                        }
                    }
                }
            } catch (SQLException failure) { throw storage(failure); }
        }
    }
    /** 角色身份完全匹配后才允许查询目标，不能只比较分片数量。 */
    private static void requireRoleIdentity(Inspection inspection, String prefix, Map<String, DataSource> target) {
        if (!inspection.archive().prefix().equals(prefix) || !inspection.roles().keySet().equals(target.keySet())) { throw new IllegalArgumentException("restore protocol or complete shard identities differ"); }
    }
    /** 写入入口必须提供明确的外部摘要，不能自动相信文件自带的摘要。 */
    private static void requireExpectedSha(String expectedSha) {
        if (expectedSha == null || !expectedSha.matches("[0-9a-f]{64}")) { throw new IllegalArgumentException("expected SHA-256 required"); }
    }
    /** 固定表序完整重放；每次执行都重新回读校验，进度日志不能跳过尚未验证的数据。 */
    private static Archive importArchive(Path archive, String expectedSha, Archive header, MysqlProtocolRestore.Session restore) {
        MessageDigest fileDigest = digest();
        try (var hashed = new DigestInputStream(new BufferedInputStream(Files.newInputStream(archive)), fileDigest); var in = new DataInputStream(hashed)) {
            if (in.readInt() != MAGIC || in.readInt() != FORMAT) { throw new IOException("unsupported backup format"); }
            String prefix = in.readUTF(); String namespace = in.readUTF(); long generation = in.readLong(); String id = in.readUTF();
            if (!prefix.equals(restore.prefix) || !namespace.equals(header.checkpoint().namespace()) || generation != header.checkpoint().generation() || !id.equals(header.checkpoint().id())) { throw new IOException("backup identity changed after verification"); }
            if (in.readInt() != restore.roles.size()) { throw new IOException("shard count differs"); }
            Set<String> seenRoles = new HashSet<>(); long rows = 0;
            for (int roleIndex = 0; roleIndex < restore.roles.size(); roleIndex++) {
                String role = in.readUTF(); DataSource source = restore.roles.get(role);
                if (source == null || !seenRoles.add(role)) { throw new IOException("unknown or duplicate role"); }
                List<String> expected = tables(prefix, role);
                if (in.readInt() != expected.size()) { throw new IOException("table count differs"); }
                for (String table : expected) {
                    rows = Math.addExact(rows, readTable(in, source, table, role, restore)); restore.progress(role, table);
                }
            }
            // 导入期间再验证实际读取的整段文件摘要，文件被替换或原地修改也不能发布 STAGED。
            String actualSha = HexFormat.of().formatHex(fileDigest.digest()); hashed.on(false);
            if (!expectedSha.equals(actualSha) || !expectedSha.equals(in.readUTF()) || in.read() != -1 || rows != header.rows()) { throw new IOException("backup changed during restore"); }
            String config = prefix.equals("pc") ? "pc_control" : prefix + "_config";
            restore.local(restore.control, "control", c -> {
                try (var q = c.prepareStatement("update " + config + " set routes=? where id=1")) { q.setString(1, restore.routes); if (q.executeUpdate() != 1) { throw new SQLException("restored configuration missing"); } }
            });
            restore.requireHeld();
            var state = MysqlProtocolState.existing(prefix, namespace, restore.control, restore.shards);
            new MysqlProtocolValidator(new MysqlProtocolAdmin(state, ignored -> 0)).verify();
            restore.finish();
            return new Archive(archive.toAbsolutePath(), expectedSha, prefix, header.checkpoint(), rows, namespace);
        } catch (IOException | SQLException failure) { throw new IllegalStateException("restore incomplete; use resume-stage with the same archive and destination", failure); }
    }
    /** 原权威不可达时恢复成新命名空间，明确隔离仍可能存活的旧权威，不冒充无损自动换主。 */
    public static String activateAsNewNamespace(MysqlProtocolAdmin staged) {
        staged.requireWritable();
        return staged.locked(() -> {
            var status = staged.state().status(); String saved = status.destination();
            if (status.mode().equals("ACTIVE") && saved != null && saved.startsWith("new-namespace:")) {
                String[] parts = saved.split(":");
                if (parts.length == 4 && parts[2].equals(status.namespace()) && staged.status().values().stream().allMatch(s -> s.mode().equals("ACTIVE") && s.namespace().equals(parts[2]) && s.generation() == status.generation() && Objects.equals(s.checkpoint(), status.checkpoint()) && Objects.equals(s.destination(), saved))) { return parts[2]; }
            }
            if (!status.mode().equals("STAGED")) { throw new IllegalStateException("new namespace activation requires STAGED data"); }
            String prefix = staged.state().prefix(); String original, namespace, plan;
            if (saved != null && saved.startsWith("new-namespace:")) {
                String[] parts = saved.split(":"); if (parts.length != 4) { throw new IllegalStateException("invalid namespace activation plan"); }
                original = parts[1]; namespace = parts[2]; plan = saved;
            } else {
                if (saved == null || !saved.startsWith("archive:")) { throw new IllegalStateException("staged archive identity missing"); }
                original = status.namespace(); namespace = UUID.randomUUID().toString(); plan = "new-namespace:" + original + ":" + namespace + ":" + saved.substring(8);
                staged.change(staged.state().control(), "control", status.generation(), "STAGED", status.checkpoint(), plan);
            }
            UUID.fromString(original); UUID.fromString(namespace);
            for (var role : roles(staged).entrySet()) {
                try (var c = role.getValue().getConnection()) {
                    c.setAutoCommit(false);
                    try {
                        try (var q = c.prepareStatement("select namespace,generation,mode from " + prefix + "_runtime where role=? for update")) {
                            q.setString(1, role.getKey()); try (var r = q.executeQuery()) {
                                if (!r.next() || (!original.equals(r.getString(1)) && !namespace.equals(r.getString(1))) || r.getLong(2) != status.generation() || !Set.of("STAGED", "ACTIVE").contains(r.getString(3))) { throw new SQLException("incomplete namespace activation plan"); }
                            }
                        }
                        staged.verifyGate();
                        try (var q = c.prepareStatement("update " + prefix + "_runtime set namespace=?,destination=? where role=?")) { q.setString(1, namespace); q.setString(2, plan); q.setString(3, role.getKey()); q.executeUpdate(); }
                        String table = role.getKey().equals("control") ? (prefix.equals("pc") ? "pc_control" : prefix + "_config") : prefix + "_binding";
                        try (var q = c.prepareStatement("update " + table + " set namespace=? where id=1 and namespace in (?,?)")) { q.setString(1, namespace); q.setString(2, original); q.setString(3, namespace); q.executeUpdate(); }
                        staged.verifyGate(); c.commit();
                    } catch (SQLException | RuntimeException | Error failure) { try { c.rollback(); } catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } mvcc.control.MysqlNamedLock.discard(c, failure); } throw failure; }
                } catch (SQLException failure) { throw storage(failure); }
            }
            // 先启用全部数据角色，最后启用控制角色；回复丢失后以相同计划幂等确认。
            var activate = new ArrayList<>(roles(staged).entrySet()); activate.sort(Comparator.comparing(e -> e.getKey().equals("control")));
            for (var role : activate) {
                staged.local(role.getValue(), c -> {
                    try (var q = c.prepareStatement("select namespace,generation,mode,checkpoint,destination from " + prefix + "_runtime where role=? for update")) {
                        q.setString(1, role.getKey()); try (var r = q.executeQuery()) {
                            if (!r.next() || !namespace.equals(r.getString(1)) || r.getLong(2) != status.generation() || !Set.of("STAGED", "ACTIVE").contains(r.getString(3)) || !Objects.equals(status.checkpoint(), r.getString(4)) || !plan.equals(r.getString(5))) { throw new SQLException("namespace activation was superseded"); }
                        }
                    }
                    staged.verifyGate();
                    try (var q = c.prepareStatement("update " + prefix + "_runtime set mode='ACTIVE',freeze_ready=false where role=?")) { q.setString(1, role.getKey()); q.executeUpdate(); }
                    staged.verifyGate(); return null;
                });
            }
            return namespace;
        });
    }
    /** 无需连接数据库即可校验和检查完整备份，支持备份完成后回复丢失的确认。 */
    public static Archive inspect(Path file) { return inspectLayout(file, null).archive(); }
    /** 在一次顺序读取中验证完整摘要和逐表摘要，只有有限布局元数据保留在内存。 */
    private static Inspection inspectLayout(Path file, String expectedSha) {
        if (expectedSha != null && !expectedSha.matches("[0-9a-f]{64}")) { throw new IllegalArgumentException("expected SHA-256 required"); }
        try {
            String sha;
            try (var random = new RandomAccessFile(file.toFile(), "r")) {
                if (random.length() < 66) { throw new IOException("truncated archive"); } random.seek(random.length() - 66); sha = random.readUTF();
            }
            if (!sha.matches("[0-9a-f]{64}") || (expectedSha != null && !expectedSha.equals(sha))) { throw new IOException("archive expected SHA-256 differs"); }
            MessageDigest fileDigest = digest(); byte[] buffer = new byte[65536];
            try (var hashed = new DigestInputStream(new BufferedInputStream(Files.newInputStream(file)), fileDigest); var in = new DataInputStream(hashed)) {
                if (in.readInt() != MAGIC || in.readInt() != FORMAT) { throw new IOException("unsupported format"); }
                String prefix = in.readUTF(); if (!Set.of("pc", "omid", "atlas").contains(prefix)) { throw new IOException("unknown protocol"); }
                var token = new MysqlProtocolAdmin.Checkpoint(in.readUTF(), in.readLong(), in.readUTF());
                int roles = in.readInt(); if (roles < 2 || roles > 1024) { throw new IOException("invalid role count"); }
                long rows = 0; var layout = new LinkedHashMap<String, List<TableLayout>>();
                for (int roleIndex = 0; roleIndex < roles; roleIndex++) {
                    MysqlProtocolState.checkInterrupted();
                    String role = in.readUTF(); if (layout.containsKey(role) || !(role.equals("control") || role.matches("shard:[A-Za-z0-9_-]{1,64}"))) { throw new IOException("invalid role"); }
                    var tables = tables(prefix, role); if (in.readInt() != tables.size()) { throw new IOException("table count mismatch"); }
                    var tableLayouts = new ArrayList<TableLayout>();
                    for (String table : tables) {
                        if (!in.readUTF().equals(table)) { throw new IOException("table mismatch"); }
                        int count = in.readInt(); if (count < 1 || count > 32) { throw new IOException("column count"); }
                        var columns = new ArrayList<ColumnLayout>(); var seen = new HashSet<String>();
                        for (int column = 0; column < count; column++) {
                            String name = in.readUTF(); byte type = in.readByte();
                            if (!name.matches("[a-z_]{1,64}") || !seen.add(name) || type < 1 || type > 3) { throw new IOException("invalid column layout"); }
                            columns.add(new ColumnLayout(name, type));
                        }
                        long actual = 0; MessageDigest tableDigest = digest();
                        for (int length; (length = in.readInt()) != -1;) {
                            if (length < 1 || length > MAX_ROW) { throw new IOException("row size"); }
                            int remaining = length;
                            while (remaining > 0) {
                                MysqlProtocolState.checkInterrupted(); int amount = in.read(buffer, 0, Math.min(buffer.length, remaining));
                                if (amount < 0) { throw new EOFException(); } tableDigest.update(buffer, 0, amount); remaining -= amount;
                            }
                            actual = Math.addExact(actual, 1);
                        }
                        if (!in.readUTF().equals(HexFormat.of().formatHex(tableDigest.digest())) || in.readLong() != actual) { throw new IOException("table digest or row count differs"); }
                        rows = Math.addExact(rows, actual); tableLayouts.add(new TableLayout(table, columns));
                    }
                    layout.put(role, tableLayouts);
                }
                String actualSha = HexFormat.of().formatHex(fileDigest.digest()); hashed.on(false);
                if (!layout.containsKey("control") || !sha.equals(actualSha) || !sha.equals(in.readUTF()) || in.read() != -1) { throw new IOException("archive SHA-256 or trailer differs"); }
                return new Inspection(new Archive(file.toAbsolutePath(), sha, prefix, token, rows, null), layout);
            }
        } catch (IOException failure) { throw new IllegalArgumentException("invalid backup archive", failure); }
    }
    /** 文件摘要在任何目标数据库写入之前验证；截断或损坏的文件不能触发恢复。 */
    public static void verifyFile(Path archive, String expectedSha) {
        requireExpectedSha(expectedSha);
        try {
            long body = Files.size(archive) - 66;
            if (body < 24) { throw new IOException("truncated archive"); }
            MessageDigest digest = digest();
            try (var in = new DataInputStream(new BufferedInputStream(Files.newInputStream(archive)))) {
                byte[] buffer = new byte[65536]; long left = body;
                while (left > 0) { int count = in.read(buffer, 0, (int) Math.min(buffer.length, left)); if (count < 0) { throw new EOFException(); } digest.update(buffer, 0, count); left -= count; }
                if (!expectedSha.equals(HexFormat.of().formatHex(digest.digest())) || !expectedSha.equals(in.readUTF()) || in.read() != -1) { throw new IOException("backup SHA-256 mismatch"); }
            }
        } catch (IOException failure) { throw new IllegalArgumentException("invalid backup archive", failure); }
    }
    /** 已知协议表布局；恢复不接受文件提供的 CREATE TABLE 或任意表名。 */
    static List<String> tables(String prefix, String role) {
        if (role.equals("control")) {
            return prefix.equals("pc") ? List.of("pc_control", "pc_readers", "mvcc_tso", "pc_runtime")
                    : List.of(prefix + "_config", prefix + "_sequence", prefix + "_txn", prefix + "_conflicts", prefix + "_gc", prefix + "_runtime");
        }
        return prefix.equals("pc") ? List.of("pc_binding", "pc_guard", "pc_data", "pc_lock", "pc_write", "pc_rollback", "pc_runtime")
                : List.of(prefix + "_binding", prefix + "_keys", prefix + "_versions", prefix + "_runtime");
    }
    /** 固定角色顺序，控制库与某分片共用物理库时 runtime 只导出各自的行。 */
    static Map<String, DataSource> roles(MysqlProtocolAdmin admin) {
        var roles = new LinkedHashMap<String, DataSource>(); roles.put("control", admin.state().control());
        new TreeMap<>(admin.state().shards()).forEach((id, source) -> roles.put("shard:" + id, source)); return roles;
    }
    /** 首次恢复禁止覆盖非空数据；只有已登记的同一恢复任务才允许重放。 */
    private static void requireEmpty(MysqlProtocolAdmin admin) {
        String prefix = admin.state().prefix();
        for (var role : roles(admin).entrySet()) {
            for (String table : tables(prefix, role.getKey())) {
                if (table.endsWith("_runtime") || table.endsWith("_config") || table.equals("pc_control") || table.endsWith("_binding") || table.endsWith("_sequence") || table.equals("mvcc_tso") || table.endsWith("_gc")) { continue; }
                if (MysqlProtocolAdmin.scalar(role.getValue(), "select exists(select 1 from " + table + " limit 1)") != 0) { throw new IllegalArgumentException("restore destination is not empty: " + table); }
            }
        }
    }
    /** 流式导出确定顺序的表，单行载荷受固定上限保护。 */
    private static long writeTable(DataOutputStream out, DataSource source, String table, String role, boolean restoring) throws SQLException, IOException {
        try (var c = source.getConnection(); var q = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            MysqlProtocolSchema.requireTable(c, table);
            q.setFetchSize(Integer.MIN_VALUE);
            try (var r = q.executeQuery(select(c, table, role))) {
                var meta = r.getMetaData(); int columns = meta.getColumnCount(); out.writeUTF(table); out.writeInt(columns);
                byte[] categories = new byte[columns]; String[] names = new String[columns];
                for (int i = 0; i < columns; i++) { names[i] = meta.getColumnName(i + 1); categories[i] = category(meta.getColumnType(i + 1)); out.writeUTF(names[i]); out.writeByte(categories[i]); }
                MessageDigest hash = digest(); long rows = 0;
                while (r.next()) {
                    MysqlProtocolState.checkInterrupted(); byte[] encoded = encode(r, names, categories, restoring && table.endsWith("_runtime"));
                    if (encoded.length > MAX_ROW) { throw new IOException("backup row exceeds bounded format"); }
                    out.writeInt(encoded.length); out.write(encoded); hash.update(encoded); rows++;
                }
                out.writeInt(-1); out.writeUTF(HexFormat.of().formatHex(hash.digest())); out.writeLong(rows); return rows;
            }
        }
    }
    /** 按目标数据库既有布局检查列名与类别，再绑定参数恢复，不拼接文件中的 SQL。 */
    private static long readTable(DataInputStream in, DataSource source, String expectedTable, String role, MysqlProtocolRestore.Session restore) throws IOException, SQLException {
        if (!expectedTable.equals(in.readUTF())) { throw new IOException("unexpected backup table"); }
        int columns = in.readInt(); if (columns < 1 || columns > 32) { throw new IOException("invalid column count"); }
        String expectedDigest; long expectedCount; long count = 0;
        try (var c = source.getConnection()) {
            MysqlProtocolSchema.requireTable(c, expectedTable);
            String[] names = new String[columns]; byte[] categories = new byte[columns];
            try (var q = c.createStatement(); var r = q.executeQuery("select * from " + expectedTable + " limit 0")) {
                var meta = r.getMetaData(); if (columns != meta.getColumnCount()) { throw new IOException("schema column count differs"); }
                for (int i = 0; i < columns; i++) {
                    names[i] = in.readUTF(); categories[i] = in.readByte();
                    if (!names[i].equals(meta.getColumnName(i + 1)) || categories[i] != category(meta.getColumnType(i + 1)) || !names[i].matches("[a-z_]+")) { throw new IOException("backup schema differs"); }
                }
            }
            boolean runtime = expectedTable.endsWith("_runtime");
            boolean atomic = runtime || expectedTable.endsWith("_config") || expectedTable.equals("pc_control") || expectedTable.endsWith("_binding") || expectedTable.endsWith("_sequence") || expectedTable.equals("mvcc_tso") || expectedTable.endsWith("_gc");
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); c.setAutoCommit(false);
            try {
                restore.fence(c, role);
                if (atomic) {
                    try (var q = c.prepareStatement("delete from " + expectedTable + (runtime ? " where role=?" : ""))) { if (runtime) { q.setString(1, role); } q.executeUpdate(); }
                } else {
                    // 清空大表同样分批提交，避免重试制造无界 undo 和长时间独占恢复栅栏。
                    int deleted;
                    do {
                        try (var q = c.createStatement()) { deleted = q.executeUpdate("delete from " + expectedTable + " limit 256"); }
                        c.commit(); restore.fence(c, role);
                    } while (deleted == 256);
                }
                String sql = "insert into " + expectedTable + "(" + String.join(",", names) + ") values(" + String.join(",", Collections.nCopies(columns, "?")) + ")";
                MessageDigest digest = digest(); int pendingRows = 0; long pendingBytes = 0; boolean needsFence = false;
                try (var insert = c.prepareStatement(sql)) {
                    for (int length; (length = in.readInt()) != -1;) {
                        MysqlProtocolState.checkInterrupted();
                        if (needsFence) { restore.fence(c, role); needsFence = false; }
                        if (length < 1 || length > MAX_ROW) { throw new IOException("invalid row length"); }
                        byte[] row = in.readNBytes(length); if (row.length != length) { throw new EOFException(); } digest.update(row);
                        try (var values = new DataInputStream(new ByteArrayInputStream(row))) {
                            for (int i = 0; i < columns; i++) {
                                if (!values.readBoolean()) { insert.setNull(i + 1, Types.NULL); }
                                else if (categories[i] == 1) { insert.setLong(i + 1, values.readLong()); }
                                else {
                                    int size = values.readInt(); if (size < 0 || size > MAX_FIELD) { throw new IOException("invalid cell length"); }
                                    byte[] value = values.readNBytes(size); if (value.length != size) { throw new EOFException(); }
                                    if (categories[i] == 2) { insert.setString(i + 1, runtime && names[i].equals("mode") ? "RESTORING" : new String(value, java.nio.charset.StandardCharsets.UTF_8)); }
                                    else { insert.setBytes(i + 1, value); }
                                }
                            }
                            if (values.read() != -1) { throw new IOException("extra row bytes"); }
                        }
                        insert.executeUpdate(); count++; pendingRows++; pendingBytes += row.length;
                        if (!atomic && (pendingRows >= 256 || pendingBytes >= 8L * 1024 * 1024)) {
                            c.commit(); pendingRows = 0; pendingBytes = 0; needsFence = true;
                        }
                    }
                }
                expectedDigest = in.readUTF(); expectedCount = in.readLong();
                if (!expectedDigest.equals(HexFormat.of().formatHex(digest.digest())) || count != expectedCount) { throw new IOException("table digest differs"); }
                if (runtime && count != 1) { throw new IOException("runtime role must contain exactly one fence row"); }
                c.commit();
            } catch (IOException | SQLException | RuntimeException | Error failure) {
                try { c.rollback(); } catch (SQLException | RuntimeException | Error rollback) { if (failure != rollback) { failure.addSuppressed(rollback); } mvcc.control.MysqlNamedLock.discard(c, failure); }
                throw failure;
            }
        }
        // 显式归还写连接后再借用回读连接，避免回读额外占用一个写池名额。
        restore.requireHeld(); var sink = new TableDigestStream();
        try (var out = new DataOutputStream(sink)) { writeTable(out, source, expectedTable, role, true); }
        if (!expectedDigest.equals(sink.tableSha) || expectedCount != sink.rows) { throw new IOException("restored database readback differs"); }
        return count;
    }
    /** 只解析表导出尾部摘要，读回验证不把整张表装入内存。 */
    private static final class TableDigestStream extends OutputStream {
        private final java.io.PipedInputStream input = new java.io.PipedInputStream(65536);
        private final java.io.PipedOutputStream output;
        private final Thread reader;
        private volatile IOException failure;
        private String tableSha;
        private long rows;
        /** 用一个有限缓冲解析导出格式，单表只保留摘要和行数。 */
        TableDigestStream() throws IOException {
            output = new java.io.PipedOutputStream(input);
            reader = Thread.ofVirtual().start(() -> {
                try (var in = new DataInputStream(input)) {
                    in.readUTF(); int columns = in.readInt(); for (int i = 0; i < columns; i++) { in.readUTF(); in.readByte(); }
                    for (int size; (size = in.readInt()) != -1;) { in.skipNBytes(size); }
                    tableSha = in.readUTF(); rows = in.readLong();
                } catch (IOException problem) { failure = problem; }
            });
        }
        /** 写入解析器的有界管道。 */
        @Override public void write(int value) throws IOException { output.write(value); }
        /** 批量写入避免逐字节分配。 */
        @Override public void write(byte[] data, int offset, int length) throws IOException { output.write(data, offset, length); }
        /** 等待格式解析结束并保留异常。 */
        @Override public void close() throws IOException {
            try { output.close(); }
            finally {
                try {
                    reader.join(10_000);
                    if (reader.isAlive()) { input.close(); reader.interrupt(); throw new IOException("table digest parser did not stop"); }
                } catch (InterruptedException problem) {
                    Thread.currentThread().interrupt(); input.close(); reader.interrupt(); throw new IOException("table digest parser interrupted", problem);
                }
            }
            if (failure != null) { throw failure; }
        }
    }
    /** 以主键顺序导出，保证文件和数据库回读摘要可重复比较。 */
    private static String select(Connection c, String table, String role) throws SQLException {
        var keys = new TreeMap<Short, String>();
        try (var r = c.getMetaData().getPrimaryKeys(c.getCatalog(), null, table)) { while (r.next()) { keys.put(r.getShort("KEY_SEQ"), r.getString("COLUMN_NAME")); } }
        if (keys.isEmpty()) { throw new SQLException("backup table has no primary key"); }
        return "select * from " + table + (table.endsWith("_runtime") ? " where role='" + role + "'" : "") + " order by " + String.join(",", keys.values());
    }
    /** 对固定协议字段做规范编码，数字统一为长整型，二进制值不经过字符转换。 */
    private static byte[] encode(ResultSet r, String[] names, byte[] categories, boolean restoring) throws SQLException, IOException {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            for (int i = 0; i < categories.length; i++) {
                if (categories[i] == 1) {
                    long number = r.getLong(i + 1); boolean present = !r.wasNull(); out.writeBoolean(present);
                    if (present) { out.writeLong(number); }
                } else {
                    byte[] data;
                    if (categories[i] == 2) {
                        String value = r.getString(i + 1);
                        data = value == null ? null : (restoring && names[i].equals("mode") ? "FROZEN" : value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    } else {
                        // 不为检查 NULL 先 getObject 整份载荷；二进制字段只读取一次并限制字节数。
                        try (var value = r.getBinaryStream(i + 1)) { data = value == null ? null : value.readNBytes(MAX_FIELD + 1); }
                    }
                    out.writeBoolean(data != null); if (data == null) { continue; }
                    if (data.length > MAX_FIELD) { throw new IOException("cell exceeds bounded backup format"); } out.writeInt(data.length); out.write(data);
                }
            }
        }
        return bytes.toByteArray();
    }
    /** 当前协议只含整数、字符串和二进制字段，其它类型拒绝隐式转换。 */
    private static byte category(int type) throws SQLException {
        return switch (type) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT, Types.BOOLEAN -> 1;
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR -> 2;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> 3;
            default -> throw new SQLException("unsupported backup column type " + type);
        };
    }
    /** 统一选择 SHA-256，算法缺失属于运行时配置错误。 */
    private static MessageDigest digest() { try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); } }
}
