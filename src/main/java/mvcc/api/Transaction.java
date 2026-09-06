package mvcc.api;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;
import mvcc.storage.Mutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * 事务上下文对象，保存事务运行期状态和缓冲写集合。
 */
public final class Transaction {
    private final long txId;
    private final long startTs;
    private final IsolationLevel isolationLevel;
    private final Set<String> participants = new LinkedHashSet<>();
    private final Map<String, NavigableMap<String, Mutation>> bufferedMutationsByShard = new LinkedHashMap<>();
    private boolean metadataPersisted;
    private TxStatus status = TxStatus.ACTIVE;

    /**
     * 构造 Transaction 实例。
     */
    public Transaction(long txId, long startTs, IsolationLevel isolationLevel) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
    }

    /**
     * 返回事务编号。
     */
    public long txId() {
        return txId;
    }

    /**
     * 返回事务开始时间戳。
     */
    public long startTs() {
        return startTs;
    }

    /**
     * 返回事务隔离级别。
     */
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    /**
     * 返回事务参与分片集合。
     */
    public Set<String> participants() {
        return Collections.unmodifiableSet(participants);
    }

    /**
     * 标记事务参与分片。
     */
    public void addParticipant(String shardId) {
        participants.add(shardId);
    }

    /**
     * 写入事务本地缓冲。
     */
    public void bufferMutation(String shardId, Mutation mutation) {
        participants.add(shardId);
        bufferedMutationsByShard
                .computeIfAbsent(shardId, ignored -> new TreeMap<>())
                .put(mutation.key(), mutation);
    }

    /**
     * 查询本地缓冲中的指定键变更。
     */
    public Optional<Mutation> bufferedMutation(String shardId, String key) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mutations.get(key));
    }

    /**
     * 返回指定分片的本地缓冲写集合。
     */
    public List<Mutation> bufferedMutations(String shardId) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return List.copyOf(mutations.values());
    }

    /**
     * 返回指定键范围内的本地缓冲写集合。
     */
    public List<Mutation> bufferedMutationsInRange(String shardId, String startKeyInclusive, String endKeyExclusive) {
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(mutations.subMap(startKeyInclusive, true, endKeyExclusive, false).values());
    }

    /**
     * 返回事务状态。
     */
    public TxStatus status() {
        return status;
    }

    /**
     * 返回事务状态。
     */
    public void status(TxStatus status) {
        this.status = status;
    }

    /**
     * 读写事务元数据是否已经持久化的标记。
     */
    public boolean metadataPersisted() {
        return metadataPersisted;
    }

    /**
     * 读写事务元数据是否已经持久化的标记。
     */
    public void metadataPersisted(boolean metadataPersisted) {
        this.metadataPersisted = metadataPersisted;
    }
}
