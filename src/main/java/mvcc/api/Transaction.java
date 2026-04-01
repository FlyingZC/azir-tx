package mvcc.api;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class Transaction {
    private final long txId;
    private final long startTs;
    private final IsolationLevel isolationLevel;
    private final Set<String> participants = new LinkedHashSet<>();
    private TxStatus status = TxStatus.ACTIVE;

    public Transaction(long txId, long startTs, IsolationLevel isolationLevel) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
    }

    public long txId() {
        return txId;
    }

    public long startTs() {
        return startTs;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public Set<String> participants() {
        return Collections.unmodifiableSet(participants);
    }

    public void addParticipant(String shardId) {
        participants.add(shardId);
    }

    public TxStatus status() {
        return status;
    }

    public void status(TxStatus status) {
        this.status = status;
    }
}
