package mvcc.storage;

import mvcc.common.MutationType;

public record Mutation(String key, MutationType type, byte[] value) {
    public static Mutation put(String key, byte[] value) {
        return new Mutation(key, MutationType.PUT, value);
    }

    public static Mutation delete(String key) {
        return new Mutation(key, MutationType.DELETE, null);
    }
}
