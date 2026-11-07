package com.flyingzc.azir.publication;

import com.flyingzc.azir.metadata.TransactionMetadataValidation;

/** 一笔单分片本地直接提交等待原子预留的事务身份。 */
public record PublicationLocalCommitRequest(long txId, long expiresAtMillis, String participant) {
    /** 校验唯一参与者与不可变的正租约截止时间。 */
    public PublicationLocalCommitRequest {
        TransactionMetadataValidation.validateParticipant(txId, participant);
        if (expiresAtMillis <= 0) {
            throw new IllegalArgumentException("local commit deadline must be positive");
        }
    }
}
