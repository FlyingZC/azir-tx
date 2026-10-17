package mvcc.publication;

import mvcc.metadata.TransactionMetadataValidation;

import java.util.Objects;
import java.util.Set;

/** 一笔已有持久分段槽和完整参与者、等待中心批量决议的事务。 */
public record PublicationSegmentCommit(PublicationSegmentLease segment, long commitTs,
                                       long txId, long deadlineMillis, Set<String> participants) {
    /** 固定请求身份并拒绝越段时间戳和空参与者集合。 */
    public PublicationSegmentCommit {
        Objects.requireNonNull(segment, "segment");
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (commitTs < segment.firstTs() || commitTs > segment.lastTs()
                || deadlineMillis < 1 || participants.isEmpty()) {
            throw new IllegalArgumentException("invalid segmented batch decision request");
        }
        participants = Set.copyOf(participants);
    }
}
