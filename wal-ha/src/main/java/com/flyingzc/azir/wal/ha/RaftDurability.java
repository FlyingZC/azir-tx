package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import java.io.IOException;
import java.nio.file.Path;

/** 官方日志和投票文件已经 force 后，同步文件创建及原子替换涉及的目录项。 */
final class RaftDurability {
    /** 工具不允许实例化。 */
    private RaftDurability() { }

    /** 首次装配同步完整本地目录链，后续普通追加只需同步 current 内的名称变化。 */
    static void initialize(RaftStorage storage) throws IOException {
        Path current = storage.getStorageDir().getCurrentDir().toPath();
        for (int i = 0; i < 5 && current != null; i++, current = current.getParent()) { RatisGenerationRecord.forceDirectory(current); }
    }

    /** 同步投票元数据、配置和日志段的目录，不能用关闭文件代替目录项持久化。 */
    static void current(RaftServer server, RaftGroupId group) throws IOException {
        RatisGenerationRecord.forceDirectory(server.getDivision(group).getRaftStorage().getStorageDir().getCurrentDir().toPath());
    }
}
