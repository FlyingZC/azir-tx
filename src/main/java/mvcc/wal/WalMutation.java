package mvcc.wal;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** 不可变的分片键变更，空值专门表示删除墓碑。 */
public record WalMutation(String shard, String key, byte[] value) {
    /** 校验键编码上限并复制调用方数组，避免日志内容被外部修改。 */
    public WalMutation {
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(key, "key");
        if (shard.isEmpty() || shard.getBytes(StandardCharsets.UTF_8).length > 128
                || key.getBytes(StandardCharsets.UTF_8).length > 512
                || !StandardCharsets.UTF_8.newEncoder().canEncode(shard)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(key)) {
            throw new IllegalArgumentException("invalid UTF-8 shard or key");
        }
        value = value == null ? null : value.clone();
    }

    /** 返回值副本，保持已冻结写集不可变。 */
    @Override public byte[] value() { return value == null ? null : value.clone(); }

    /** 返回是否为删除墓碑。 */
    public boolean deleted() { return value == null; }

    /** 返回不可变载荷长度，容量校验无需复制整份数据。 */
    public int valueLength() { return value == null ? 0 : value.length; }

    /** 返回资源记账大小，包含每个版本的保守固定开销。 */
    public long retainedBytes() {
        return 128L + shard.getBytes(StandardCharsets.UTF_8).length
                + key.getBytes(StandardCharsets.UTF_8).length + (value == null ? 0 : value.length);
    }
}
