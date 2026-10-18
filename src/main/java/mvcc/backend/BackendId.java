package mvcc.backend;

import java.util.Objects;

/** 后端持久身份；同一 startTs 不能在不同模型或数据空间之间混用。 */
public record BackendId(String implementation, String namespace, long startTs) {
    /** 校验可供跨进程查询的完整事务身份。 */
    public BackendId {
        Objects.requireNonNull(implementation, "implementation");
        Objects.requireNonNull(namespace, "namespace");
        if (startTs <= 0) { throw new IllegalArgumentException("startTs must be positive"); }
    }
}
