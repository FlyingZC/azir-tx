package mvcc.txn;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 事务分类结果，保存提交协议类型和参与分片快照。
 */
public final class TransactionClassification {
    private final TransactionProtocolType protocolType;
    private final Set<String> participants;

    /**
     * 构造事务分类结果。
     */
    public TransactionClassification(TransactionProtocolType protocolType, Set<String> participants) {
        this.protocolType = Objects.requireNonNull(protocolType, "protocolType");
        this.participants = new LinkedHashSet<>(Objects.requireNonNull(participants, "participants"));
    }

    /**
     * 返回事务提交协议类型。
     */
    public TransactionProtocolType protocolType() {
        return protocolType;
    }

    /**
     * 返回参与分片集合。
     */
    public Set<String> participants() {
        return Collections.unmodifiableSet(participants);
    }
}
