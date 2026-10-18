package mvcc.backend;

/** 可插拔事务内核会话，SQL 不解释原生协议的中间状态。 */
public interface BackendTransaction extends AutoCloseable {
    /** 业务可观测的会话状态，UNKNOWN 不允许假回滚。 */
    enum State { ACTIVE, COMMITTED, ABORTED, UNKNOWN }
    /** 返回完整原事务身份。 */
    BackendId id();
    /** 返回当前会话状态。 */
    State state();
    /** 创建单条 SQL 使用的固定语句视图。 */
    BackendReadView statement();
    /** 在当前业务事务写入非空值。 */
    void put(String shard, String key, byte[] value);
    /** 在当前业务事务写入删除墓碑。 */
    void delete(String shard, String key);
    /** 经原生核心确认持久提交，重复成功返回同一凭据。 */
    BackendCommit commit();
    /** 仅对尚未提交的原事务作明确回滚。 */
    void rollback();
    /** 关闭活动事务，保留 UNKNOWN 的原始身份与决议。 */
    @Override
    void close();
}
