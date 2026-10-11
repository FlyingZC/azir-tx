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
    private final long deadlineMillis;
    private final Object owner;
    private boolean rollbackOnly;
    private boolean decisionStarted;
    private final Map<String, Map<String, Integer>> writeSizes = new LinkedHashMap<>();
    private int writeKeyCount;
    private long writeBytes;

    /**
     * 构造 Transaction 实例。
     */
    public Transaction(long txId, long startTs, IsolationLevel isolationLevel) {
        this(txId, startTs, isolationLevel, Long.MAX_VALUE, null);
    }

    /** 构造拥有固定租约和管理器私有归属标识的事务。 */
    public Transaction(long txId, long startTs, IsolationLevel isolationLevel, long deadlineMillis, Object owner) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
        this.deadlineMillis = deadlineMillis;
        this.owner = owner;
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
                .computeIfAbsent(shardId, ignored -> new TreeMap<>(mvcc.storage.Utf8KeyOrder::compare))
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

    /** 按游标取得有限缓冲键，避免逐页复制或遍历整份事务写集合。 */
    public List<Mutation> bufferedMutationsPage(String shardId, String startKeyInclusive, String endKeyExclusive,
                                                String afterKeyExclusive, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("buffer page limit must be positive");
        }
        NavigableMap<String, Mutation> mutations = bufferedMutationsByShard.get(shardId);
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        String lower = startKeyInclusive;
        boolean inclusive = true;
        if (afterKeyExclusive != null && (lower == null || mvcc.storage.Utf8KeyOrder.compare(afterKeyExclusive, lower) >= 0)) {
            lower = afterKeyExclusive;
            inclusive = false;
        }
        if (lower != null && endKeyExclusive != null && mvcc.storage.Utf8KeyOrder.compare(lower, endKeyExclusive) >= 0) {
            return List.of();
        }
        NavigableMap<String, Mutation> view = lower == null ? mutations : mutations.tailMap(lower, inclusive);
        if (endKeyExclusive != null) {
            view = view.headMap(endKeyExclusive, false);
        }
        return view.values().stream().limit(limit).toList();
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

    /** 返回事务固定租约的到期时间。 */
    public long deadlineMillis() {
        return deadlineMillis;
    }

    /** 核对私有归属标识，不暴露标识值供调用方伪造。 */
    public boolean ownedBy(Object owner) {
        return this.owner != null && this.owner == owner;
    }

    /** 预写结果异常后只允许回滚，防止提交不完整参与者集合。 */
    public void markRollbackOnly() {
        rollbackOnly = true;
    }

    /** 返回事务是否仅允许回滚。 */
    public boolean rollbackOnly() {
        return rollbackOnly;
    }

    /** 标记提交已进入可能产生持久化副作用的决议阶段。 */
    public void decisionStarted(boolean decisionStarted) {
        this.decisionStarted = decisionStarted;
    }

    /** 判断是否必须解析提交决议才能处理异常。 */
    public boolean decisionStarted() {
        return decisionStarted;
    }

    /** 按覆盖后的写集合计费，在变更缓冲前拒绝超限事务。 */
    public void reserveWrite(String shardId, Mutation mutation, int maxKeys, long maxBytes) {
        Map<String, Integer> sizes = writeSizes.computeIfAbsent(shardId, ignored -> new LinkedHashMap<>());
        Integer oldSize = sizes.get(mutation.key());
        long newSize = mutation.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                + (long) (mutation.value() == null ? 0 : mutation.value().length);
        int nextKeys = writeKeyCount + (oldSize == null ? 1 : 0);
        long nextBytes = writeBytes - (oldSize == null ? 0 : oldSize) + newSize;
        if (nextKeys > maxKeys || nextBytes > maxBytes || newSize > Integer.MAX_VALUE) {
            throw new mvcc.common.MvccException(mvcc.common.ErrorCode.RESOURCE_EXHAUSTED,
                    "transaction write budget exceeded", false, txId);
        }
        sizes.put(mutation.key(), (int) newSize);
        writeKeyCount = nextKeys;
        writeBytes = nextBytes;
    }
}
