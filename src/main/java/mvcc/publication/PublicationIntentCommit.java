package mvcc.publication;

import mvcc.metadata.TransactionMetadataValidation;

import java.util.Set;

/** 一笔已完成参与者预写、等待控制库原子决议的事务。 */
public record PublicationIntentCommit(long txId, long expiresAtMillis, Set<String> participants) {
    /** 固定参与者集合并拒绝无参与者的提交请求。 */
    public PublicationIntentCommit {
        TransactionMetadataValidation.validateParticipants(txId, participants);
        if (participants.isEmpty() || expiresAtMillis <= 0) {
            throw new IllegalArgumentException("intent commit requires participants and a positive deadline");
        }
        participants = Set.copyOf(participants);
    }
}
