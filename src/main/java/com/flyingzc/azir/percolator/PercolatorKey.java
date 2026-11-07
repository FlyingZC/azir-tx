package com.flyingzc.azir.percolator;

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.util.Objects;

/** Percolator 键的完整地址，分片编号参与 primary 的唯一定位。 */
public record PercolatorKey(String shard, String key) implements Comparable<PercolatorKey> {
    /** 校验分片名称和 MySQL 二进制键长度，避免截断或编码碰撞。 */
    public PercolatorKey {
        Objects.requireNonNull(shard, "shard");
        if (!shard.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("shard must match [A-Za-z0-9._-]{1,64}");
        }
        Objects.requireNonNull(key, "key");
        if (key.length() > 512 || Utf8KeyOrder.encode(key).length > 512) {
            throw new IllegalArgumentException("key exceeds 512 UTF-8 bytes");
        }
    }

    /** 按固定分片和 UTF-8 键顺序加锁，减少同分片批量事务的死锁。 */
    @Override
    public int compareTo(PercolatorKey other) {
        int order = shard.compareTo(other.shard);
        return order == 0 ? Utf8KeyOrder.compare(key, other.key) : order;
    }
}
