package mvcc.backend;

/** 公共提交凭据，只读结果不承诺持久化写决议，时间戳不跨后端解释。 */
public record BackendCommit(BackendId id, long commitTs, boolean readOnly) {
    /** 写提交必须携带晚于开始边界的真实提交时间戳。 */
    public BackendCommit {
        java.util.Objects.requireNonNull(id, "id");
        if (readOnly ? commitTs != 0 : commitTs <= id.startTs()) {
            throw new IllegalArgumentException("invalid backend commit timestamp");
        }
    }
}
