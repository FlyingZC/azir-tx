package mvcc.storage;

public record VersionedValue(String key, byte[] value, boolean deleted, long versionTs, boolean ownIntent) {
}
