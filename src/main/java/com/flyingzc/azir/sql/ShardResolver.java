package com.flyingzc.azir.sql;

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
     * 返回扫描指定逻辑表时需要访问的全部物理分片。
     */
    default List<String> resolveShards(String table) {
        return List.of(resolveShard(table, ""));
    }

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
        return new ShardResolver() {
            /**
             * 按主键哈希选择单个写入或点查分片。
             */
            @Override
            public String resolveShard(String table, String key) {
                return copiedShardIds.get(Math.floorMod(Objects.hashCode(key), copiedShardIds.size()));
            }

            /**
             * 全表扫描必须覆盖哈希规则管理的所有分片。
             */
            @Override
            public List<String> resolveShards(String table) {
                return copiedShardIds;
            }
        };
    }
}
