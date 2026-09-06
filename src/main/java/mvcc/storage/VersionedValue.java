package mvcc.storage;

/**
 * 带版本时间戳的存储值对象。
 */
public record VersionedValue(String key, byte[] value, boolean deleted, long versionTs, boolean ownIntent) {
}
