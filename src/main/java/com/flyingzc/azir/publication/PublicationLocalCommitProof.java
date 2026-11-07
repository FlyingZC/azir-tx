package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.TxStatus;
import com.flyingzc.azir.metadata.TransactionMetadataValidation;
import com.flyingzc.azir.storage.ParticipantRecord;

import java.util.Objects;

/** 一笔已在单分片物理提交、等待中心账本补决议的持久证明。 */
public record PublicationLocalCommitProof(ParticipantRecord local, long commitTs, String participant) {
    /** 只接受与本地 COMMITTED 终态一致的时间戳及单一参与者。 */
    public PublicationLocalCommitProof {
        Objects.requireNonNull(local, "local");
        TransactionMetadataValidation.validateParticipant(local.txId(), participant);
        if (local.status() != TxStatus.COMMITTED || local.commitTs() == null
                || local.commitTs() != commitTs || commitTs <= local.startTs()) {
            throw new IllegalArgumentException("local completion requires matching committed participant proof");
        }
    }
}
