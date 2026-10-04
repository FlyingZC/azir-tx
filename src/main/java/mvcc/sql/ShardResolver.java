package mvcc.sql;

import java.util.List;
import java.util.Objects;

/**
 * SQL 表和主键到物理分片的解析接口。
 */
@FunctionalInterface
public interface ShardResolver {
    /**
     * 按表名和主键解析物理分片编号。
     */
    String resolveShard(String table, String key);

    /**
     * 创建固定返回单分片的解析器。
     */
    static ShardResolver singleShard(String shardId) {
        return (table, key) -> shardId;
    }

    /**
     * 创建按主键哈希选择分片的解析器。
     */
    static ShardResolver hashByKey(List<String> shardIds) {
        List<String> copiedShardIds = List.copyOf(shardIds);
        if (copiedShardIds.isEmpty()) {
            throw new IllegalArgumentException("shardIds must not be empty");
        }
        return (table, key) -> copiedShardIds.get(Math.floorMod(Objects.hashCode(key), copiedShardIds.size()));
    }
}
