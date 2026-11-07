package com.flyingzc.azir.wal.ha;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** 仅删除已验证旧代次里的协议文件，未知文件和链接拒绝，目录逐轮有界清理。 */
final class RatisRetirementFiles {
    private static final String UUID = "[a-f0-9-]{36}";
    /** 工具不允许实例化。 */
    private RatisRetirementFiles() { }
    /** 控制目录锁内回收已中断发布的私有暂存文件，不要求半写文件具有有效 CRC。 */
    static void scratch(Path root, boolean snapshots, RatisRetirementWork work) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) { return; }
        if (Files.isSymbolicLink(root)) { throw new IOException("scratch root is symbolic"); }
        try (var entries = Files.list(root)) { var iterator = entries.iterator(); while (iterator.hasNext()) {
            work.scan(); Path path = iterator.next(); String name = path.getFileName().toString();
            boolean owned = snapshots ? name.matches("\\.bundle-" + UUID + "|snapshot\\.[0-9]+_[0-9]+\\.md5\\.tmp") : name.matches("\\.(record|retirement)-" + UUID);
            if (!owned) { continue; } if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("scratch entry is not regular"); }
            work.deleting(); Files.delete(path); work.removed++; RatisGenerationRecord.forceDirectory(root);
        } }
    }
    /** 一次只检查有限树前缀，重复调用能在大归档目录中继续前进。 */
    static boolean clean(Path root, boolean generation, RatisRetirementWork work, Hook hook) throws IOException {
        return clean(root, generation, generation ? "GENERATION" : "INSTALLATION", work, hook);
    }
    /** 已验证构建身份的镜像暂存采用 BUILD 标记，同样逐轮有界删除。 */
    static boolean cleanBuild(Path root, RatisRetirementWork work) throws IOException { return clean(root, false, "BUILD", work, path -> { }); }
    /** 标记最后删除，任何中断都保留可供下一轮核对的归属。 */
    private static boolean clean(Path root, boolean generation, String marker, RatisRetirementWork work, Hook hook) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) { return true; }
        if (Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("retirement tree is not a private directory"); }
        var paths = new ArrayList<Path>();
        try (var stream = Files.walk(root, 5)) {
            var iterator = stream.iterator();
            while (iterator.hasNext()) {
                if (paths.size() >= Math.max(1, work.options.scanLimit() - work.scanned)) { break; }
                Path path = iterator.next(); work.scan(); if (path.equals(root)) { continue; }
                if (Files.isSymbolicLink(path)) { throw new IOException("retirement tree contains symbolic link"); }
                String name = root.relativize(path).toString(); boolean directory = Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
                if (!(name.equals(marker) && !directory || allowed(name, generation, directory)) || !directory && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) { throw new IOException("retirement tree contains unknown entry: " + name); }
                paths.add(path);
            }
        }
        paths.sort(Comparator.reverseOrder());
        for (Path path : paths) {
            if (path.equals(root.resolve(marker))) { continue; }
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) { try (var entries = Files.list(path)) { if (entries.findAny().isPresent()) { continue; } } }
            work.deleting(); if (Files.deleteIfExists(path)) { work.removed++; RatisGenerationRecord.forceDirectory(path.getParent()); hook.deleted(path); }
        }
        try (var entries = Files.list(root)) { if (entries.anyMatch(path -> !path.equals(root.resolve(marker)))) { return false; } }
        if (Files.exists(root.resolve(marker))) { work.deleting(); Files.delete(root.resolve(marker)); work.removed++; RatisGenerationRecord.forceDirectory(root); hook.deleted(root.resolve(marker)); }
        work.deleting(); Files.delete(root); work.removed++; RatisGenerationRecord.forceDirectory(root.getParent()); hook.deleted(root); return true;
    }
    /** 所有实际删除后的位置都可供包内中断验证。 */
    @FunctionalInterface interface Hook {
        /** 注入不能替代真实删除与目录同步。 */
        void deleted(Path path) throws IOException;
    }
    /** 已知协议路径才允许删除，恢复暂存目录同样限制其内部结构。 */
    private static boolean allowed(String name, boolean generation, boolean directory) {
        if (!directory && name.equals(generation ? "GENERATION" : "INSTALLATION")) { return true; }
        if (generation) {
            if (directory && (name.equals("wal") || name.matches("\\.azir-restore-" + UUID + "-[a-f0-9]{24}"))) { return true; }
            if (!directory && name.matches("\\.azir-restore-[a-f0-9]{24}\\.lock|\\.record-" + UUID)) { return true; }
            String prefix = name.startsWith("wal/") ? "wal/" : name.matches("\\.azir-restore-" + UUID + "-[a-f0-9]{24}/.*") ? name.substring(0, name.indexOf('/') + 1) : null;
            return prefix != null && wal(name.substring(prefix.length()), directory);
        }
        if (!directory && (name.equals("snapshot.bundle") || name.matches("\\.record-" + UUID))) { return true; }
        String prefix = name.equals("backup") || name.startsWith("backup/") ? "backup" : name.matches("\\.azir-unpack-" + UUID + "(?:/.*)?") ? name.split("/", 2)[0] : null;
        if (prefix == null) { return false; } if (name.equals(prefix)) { return directory; }
        String rest = name.substring(prefix.length() + 1);
        if (directory && (rest.equals("local") || rest.equals("data"))) { return true; }
        if (!directory && (rest.equals("COMPLETE") || rest.equals("COMPLETE.tmp") || rest.matches("data/[0-9]{4}\\.bin"))) { return true; }
        return rest.startsWith("local/") && wal(rest.substring(6), directory);
    }
    /** WAL 身份、检查点和凭据归档只认固定名字，不接收任意文件模式。 */
    private static boolean wal(String name, boolean directory) {
        if (directory) { return name.matches("checkpoint-" + UUID); }
        return name.matches("ENGINE|REPLICA|CURRENT|RESTORING|writer\\.lock|transactions\\.wal|(CURRENT|ENGINE)-" + UUID + "\\.tmp|receipts-" + UUID + "\\.sst|checkpoint-" + UUID + "/(snapshot\\.bin|tail\\.wal)");
    }
}
