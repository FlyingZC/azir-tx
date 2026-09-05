package mvcc.storage;

public record KeyRange(String startKeyInclusive, String endKeyExclusive) {
}
