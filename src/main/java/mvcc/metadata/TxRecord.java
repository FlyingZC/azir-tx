package mvcc.metadata;

import mvcc.common.IsolationLevel;
import mvcc.common.TxStatus;

import java.util.LinkedHashSet;
import java.util.Set;

public final class TxRecord {
    private final long txId;
    private final long startTs;
    private final IsolationLevel isolationLevel;
    private final Set<String> participants;
    private final TxStatus status;
    private final Long commitTs;

    public TxRecord(long txId,
                    long startTs,
                    IsolationLevel isolationLevel,
                    Set<String> participants,
                    TxStatus status,
                    Long commitTs) {
        this.txId = txId;
        this.startTs = startTs;
        this.isolationLevel = isolationLevel;
        this.participants = new LinkedHashSet<>(participants);
        this.status = status;
        this.commitTs = commitTs;
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
        return Set.copyOf(participants);
    }

    public TxStatus status() {
        return status;
    }

    public Long commitTs() {
        return commitTs;
    }

    public TxRecord withParticipants(Set<String> newParticipants) {
        return new TxRecord(txId, startTs, isolationLevel, newParticipants, status, commitTs);
    }

    public TxRecord withStatus(TxStatus newStatus) {
        return new TxRecord(txId, startTs, isolationLevel, participants, newStatus, commitTs);
    }

    public TxRecord withCommitted(long newCommitTs) {
        return new TxRecord(txId, startTs, isolationLevel, participants, TxStatus.COMMITTED, newCommitTs);
    }
}
