package mvcc.storage;

import mvcc.common.MutationType;

/**
 * 写入变更对象，描述一次 put 或 delete 操作。
 */
public record Mutation(String key, MutationType type, byte[] value) {
    /**
     * 在事务中写入键值数据。
     */
    public static Mutation put(String key, byte[] value) {
        return new Mutation(key, MutationType.PUT, value);
    }

    /**
     * 在事务中删除键值数据。
     */
    public static Mutation delete(String key) {
        return new Mutation(key, MutationType.DELETE, null);
    }
}
