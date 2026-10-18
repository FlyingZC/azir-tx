package mvcc.percolator;

import mvcc.storage.Mutation;
import mvcc.storage.KeyRange;
import mvcc.storage.VersionedValue;
import java.util.List;
import java.util.Optional;

/** 独立 Percolator 存储协议边界；终态权威是 primary 的 write 或 rollback 记录。 */
public interface PercolatorStore {
    /** 仍未决议、已提交和已回滚三种持久状态。 */
    enum State { PENDING, COMMITTED, ROLLED_BACK, RETIRED }

    /** 查询、提交、主动回滚和只清理过期锁四种 primary 原子操作。 */
    enum Action { INSPECT, COMMIT, ROLLBACK, EXPIRE }

    /** primary 的持久决议；只有提交状态携带非零提交时间戳。 */
    record Decision(State state, long commitTs) {
        /** 拒绝没有合法时间戳的提交决议。 */
        public Decision {
            java.util.Objects.requireNonNull(state, "state");
            if ((state == State.COMMITTED) != (commitTs > 0)) {
                throw new IllegalArgumentException("invalid decision timestamp");
            }
        }
    }

    /** 单个键的持久锁，恢复时必须按原 startTs 回查完整 primary 地址。 */
    record Lock(PercolatorKey key, long startTs, PercolatorKey primary) { }

    /** 同一个本地一致性读视图中的锁和值，防止分次读取跨过一次提交。 */
    record Snapshot(Optional<Lock> lock, Optional<VersionedValue> value) { }

    /** 返回该存储负责的逻辑分片。 */
    String shardId();

    /** 初始化与原有模型隔离的 pc_* 协议表。 */
    void initialize();

    /** 原子检测冲突并写入同分片整批 data 和 lock，失败不能留下半批预写。 */
    void prewrite(long startTs, PercolatorKey primary, long ttlMillis, List<Mutation> mutations);

    /** 原子读取锁与 commitTs 不超过读边界的最近版本。 */
    Snapshot snapshot(String key, long readTs);

    /** 按 UTF-8 顺序枚举永久 guard 候选键，最多返回 limit 个，不预先过滤锁或墓碑。 */
    List<String> keys(KeyRange range, String afterKeyExclusive, int limit);

    /** 在 primary 键锁下串行化提交、过期清理、主动回滚和迟到预写栅栏。 */
    Decision primary(PercolatorKey key, long startTs, Action action, long commitTs);

    /** 按已经确认的 primary 终态幂等完成同分片键；调用方必须先取得权威决议。 */
    void finish(long startTs, PercolatorKey primary, Decision decision, List<String> keys);

    /** 有界发现残留锁，允许重建运行时后继续恢复。 */
    List<Lock> pending(int limit);
    /** 有界回收安全点前的冗余载荷；默认扩展存储不启用物理清理。 */
    default int collect(long safePoint, int limit) { return 0; }
    /** 返回残留锁总数，未实现诊断的存储明确拒绝。 */
    default long pendingCount() { throw new UnsupportedOperationException("pending count unavailable"); }
}
