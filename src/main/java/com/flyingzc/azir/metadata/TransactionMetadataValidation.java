package com.flyingzc.azir.metadata;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

import java.util.Set;

/** 统一内存和持久化元数据的时间戳及参与者路由校验。 */
public final class TransactionMetadataValidation {
    /** 仅提供元数据不变量校验，不保存可变状态。 */
    private TransactionMetadataValidation() {
    }

    /** 提交决议和单分片占位时间戳必须严格晚于开始时间，禁止回绕或相等。 */
    public static void validateCommitTs(long txId, long startTs, Long commitTs) {
        if (commitTs == null || commitTs <= startTs) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "commit timestamp must be greater than start timestamp", false, txId);
        }
    }

    /** 空集合表示无参与者；集合本身和其中的路由标识必须有效。 */
    public static void validateParticipants(long txId, Set<String> participants) {
        if (participants == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant set must not be null", false, txId);
        }
        participants.forEach(participant -> validateParticipant(txId, participant));
    }

    /** 保留旧 CSV 格式，禁止空白标识或逗号造成恢复路由歧义。 */
    public static void validateParticipant(long txId, String participant) {
        if (participant == null || participant.isBlank() || participant.indexOf(',') >= 0) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE, "participant must be nonblank and must not contain a comma", false, txId);
        }
    }
}
