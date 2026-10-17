package mvcc.publication;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger;
import mvcc.publication.mysql.MysqlLocalFirstPublicationLedger.SegmentReservation;
import mvcc.storage.mysql.MysqlShardSegmentFence;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** 有界轮转恢复失联本地段，先物理 fencing 再封存槽并回收已发布候选。 */
public final class LocalFirstPublicationRecoveryManager {
    private final MysqlLocalFirstPublicationLedger ledger;
    private final Map<String, MysqlShardSegmentFence> shards;
    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();
    private long segmentAfterStart;
    private long gcAfterSegmentStart;

    /** 一轮独立统计物理封段、中止尾槽、候选回收与故障数。 */
    public record RecoveryResult(int sealedSegments, int abortedSlots, int purgedCandidates,
                                 int failures, long publishedTs) {
        /** 恢复统计不能出现负计数或回退水位。 */
        public RecoveryResult {
            if (sealedSegments < 0 || abortedSlots < 0 || purgedCandidates < 0
                    || failures < 0 || publishedTs < 0) {
                throw new IllegalArgumentException("invalid local-first recovery result");
            }
        }
    }

    /** 恢复路由必须与每个分片持久 fence 的目标身份一致。 */
    public LocalFirstPublicationRecoveryManager(MysqlLocalFirstPublicationLedger ledger,
                                                Map<String, MysqlShardSegmentFence> shards) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.shards = Map.copyOf(shards);
        this.shards.forEach((shardId, physical) -> {
            if (!shardId.equals(physical.shardId())) {
                throw new IllegalArgumentException("local-first recovery route differs from shard fence");
            }
        });
    }

    /** 先完成段物理 fence 与中心终态，再发布水位并清理已封段分片候选。 */
    public synchronized RecoveryResult recoverOnce(int segmentLimit, int candidateLimit) {
        if (segmentLimit < 1 || segmentLimit > 4096 || candidateLimit < 1 || candidateLimit > 4096) {
            throw new IllegalArgumentException("invalid local-first recovery batch limit");
        }
        int sealed = 0;
        int aborted = 0;
        int purged = 0;
        int failures = 0;
        List<SegmentReservation> segments = ledger.findRecoverableSegments(segmentAfterStart, segmentLimit);
        segmentAfterStart = segments.size() < segmentLimit ? 0 : segments.getLast().lease().firstTs();
        for (SegmentReservation segment : segments) {
            try {
                var request = ledger.beginFencing(segment, false);
                if (MysqlLocalFirstPublicationLedger.INTENT_SEGMENT_ROUTE.equals(segment.shardId())) {
                    aborted += ledger.sealIntentSegment(segment);
                } else {
                    MysqlShardSegmentFence physical = requireShard(segment.shardId());
                    physical.fence(request.physicalLease());
                    aborted += ledger.sealFencedSegment(segment, physical);
                }
                sealed++;
            } catch (MvccException ex) {
                if (ex.errorCode() != ErrorCode.TRANSACTION_ABORTED) {
                    lastFailure.set(ex);
                    failures++;
                }
            } catch (RuntimeException ex) {
                lastFailure.set(ex);
                failures++;
            }
        }
        long published;
        try {
            published = ledger.advancePublishedFast(4096);
        } catch (RuntimeException ex) {
            lastFailure.set(ex);
            failures++;
            published = ledger.currentPublished();
        }
        List<SegmentReservation> gcSegments = ledger.findPurgeableSegments(gcAfterSegmentStart, candidateLimit);
        gcAfterSegmentStart = gcSegments.size() < candidateLimit
                ? 0 : gcSegments.getLast().lease().firstTs();
        for (SegmentReservation segment : gcSegments) {
            try {
                var proof = ledger.segmentGcProof(segment.lease().firstTs());
                if (proof.isEmpty()) {
                    continue;
                }
                MysqlShardSegmentFence physical = requireShard(proof.get().shardId());
                physical.purgeFencedSegmentCandidates(proof.get());
                purged += ledger.markSegmentCandidatesPurged(proof.get(), physical);
            } catch (RuntimeException ex) {
                lastFailure.set(ex);
                failures++;
            }
        }
        return new RecoveryResult(sealed, aborted, purged, failures, published);
    }

    /** 缺少目标分片时保留 FENCING 和候选 GC 记录供路由恢复后重试。 */
    private MysqlShardSegmentFence requireShard(String shardId) {
        MysqlShardSegmentFence physical = shards.get(shardId);
        if (physical == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "local-first recovery shard unavailable: " + shardId, false, null);
        }
        return physical;
    }

    /** 返回最近一次失败以便运维识别无法发布的最老段。 */
    public RuntimeException lastFailure() {
        return lastFailure.get();
    }
}
