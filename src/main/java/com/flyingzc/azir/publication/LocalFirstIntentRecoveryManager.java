package com.flyingzc.azir.publication;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.TxRecord;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.CommittedIntentProof;
import com.flyingzc.azir.publication.mysql.MysqlLocalFirstPublicationLedger.IntentFinalizationRequest;
import com.flyingzc.azir.storage.StorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter;
import com.flyingzc.azir.storage.mysql.MysqlStorageAdapter.IntentTerminalProof;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongPredicate;

/** 按中心持久决议有界恢复跨分片意向，并在全部分片落盘后释放中心 GC 锚点。 */
public final class LocalFirstIntentRecoveryManager {
    private final MysqlLocalFirstPublicationLedger ledger;
    private final Map<String, StorageAdapter> shards;
    private final int maxInFlight;
    private final AtomicReference<RuntimeException> lastFailure = new AtomicReference<>();
    private long committedAfterTxId = Long.MIN_VALUE;
    private long abortAfterTxId = Long.MIN_VALUE;

    /** 一轮分别统计物化、回滚和失败的跨分片事务。 */
    public record RecoveryResult(int finalized, int aborted, int failures) {
        /** 恢复统计不得出现负数。 */
        public RecoveryResult {
            if (finalized < 0 || aborted < 0 || failures < 0) {
                throw new IllegalArgumentException("invalid cross-shard intent recovery result");
            }
        }
    }

    /** 每条路由必须与分片适配器的持久身份一致。 */
    public LocalFirstIntentRecoveryManager(MysqlLocalFirstPublicationLedger ledger,
                                           Map<String, StorageAdapter> shards) {
        this(ledger, shards, 16);
    }

    /** 并行窗口由调用方显式约束，不能无限借用分片或控制库连接。 */
    public LocalFirstIntentRecoveryManager(MysqlLocalFirstPublicationLedger ledger,
                                           Map<String, StorageAdapter> shards, int maxInFlight) {
        if (maxInFlight < 1 || maxInFlight > 64) {
            throw new IllegalArgumentException("invalid cross-shard recovery concurrency");
        }
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.shards = Map.copyOf(shards);
        this.maxInFlight = maxInFlight;
        this.shards.forEach((shardId, adapter) -> {
            if (!shardId.equals(adapter.shardId()) || !(adapter instanceof MysqlStorageAdapter)) {
                throw new IllegalArgumentException("cross-shard recovery route differs from participant");
            }
        });
    }

    /** 按有符号事务号轮转，单笔失败保留中心锚点且不阻塞同批其他事务。 */
    public synchronized RecoveryResult recoverOnce(int limit) {
        if (limit < 1 || limit > 4096) {
            throw new IllegalArgumentException("invalid cross-shard recovery batch limit");
        }
        List<Long> committed = ledger.findUnfinalizedIntentDecisions(committedAfterTxId, limit);
        committedAfterTxId = committed.size() < limit ? Long.MIN_VALUE : committed.getLast();
        RecoveryCounts committedResult = recoverCommittedBatch(committed);
        List<Long> abortWork = ledger.findIntentAbortWorkIds(abortAfterTxId, limit);
        abortAfterTxId = abortWork.size() < limit ? Long.MIN_VALUE : abortWork.getLast();
        RecoveryCounts abortResult = recoverParallel(abortWork, this::recoverAborted);
        return new RecoveryResult(committedResult.succeeded(), abortResult.succeeded(),
                committedResult.failed() + abortResult.failed());
    }

    /** 单笔中心 COMMITTED 决议可独立补齐各参与者并形成最终化批次输入。 */
    private IntentFinalizationRequest materializeCommitted(long txId, ExecutorService fanout) {
        CommittedIntentProof centerProof = ledger.committedIntentProof(txId);
        TxRecord decision = centerProof.record();
        boolean vector = ledger.isVectorIntentTransaction(txId);
        List<IntentTerminalProof> proofs = new ArrayList<>(decision.participants().size());
        List<CompletableFuture<IntentTerminalProof>> participants = new ArrayList<>(decision.participants().size());
        for (String shardId : decision.participants().stream().sorted().toList()) {
            participants.add(CompletableFuture.supplyAsync(() ->
                    vector ? requireShard(shardId).commitVectorIntentWithProof(centerProof)
                            : requireShard(shardId).commitIntentWithProof(centerProof), fanout));
        }
        for (CompletableFuture<IntentTerminalProof> participant : participants) {
            IntentTerminalProof proof = participant.join();
            if (proof.record().startTs() != decision.startTs()) {
                throw illegal(txId, "committed participant differs from center decision");
            }
            proofs.add(proof);
        }
        return new IntentFinalizationRequest(txId, proofs);
    }

    /** 分片物化可并行，中心 GC 锚点必须等完整证明后才整批最终化。 */
    private RecoveryCounts recoverCommittedBatch(List<Long> txIds) {
        if (txIds.isEmpty()) {
            return new RecoveryCounts(0, 0);
        }
        AtomicInteger failed = new AtomicInteger();
        Semaphore budget = new Semaphore(maxInFlight);
        ConcurrentLinkedQueue<IntentFinalizationRequest> ready = new ConcurrentLinkedQueue<>();
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> tasks = new ArrayList<>(txIds.size());
            for (long txId : txIds) {
                tasks.add(CompletableFuture.runAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        ready.add(materializeCommitted(txId, workers));
                    } catch (RuntimeException ex) {
                        lastFailure.set(ex);
                        failed.incrementAndGet();
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            tasks.forEach(CompletableFuture::join);
        }
        if (ready.isEmpty()) {
            return new RecoveryCounts(0, failed.get());
        }
        List<IntentFinalizationRequest> batch = List.copyOf(ready);
        try {
            return new RecoveryCounts(ledger.markIntentFinalizedBatch(batch), failed.get());
        } catch (RuntimeException batchFailure) {
            // 一笔异常不能让同批其他已物化事务长期占用中心 GC 锚点。
            lastFailure.set(batchFailure);
            int finalized = 0;
            for (IntentFinalizationRequest request : batch) {
                try {
                    if (ledger.markIntentFinalized(request.txId(), request.proofs())) {
                        finalized++;
                    }
                } catch (RuntimeException ex) {
                    lastFailure.set(ex);
                    failed.incrementAndGet();
                }
            }
            return new RecoveryCounts(finalized, failed.get());
        }
    }

    /** 单笔中心 ABORTED 决议在所有分片回滚后才能解除 GC 锚点。 */
    private boolean recoverAborted(long txId) {
        // 中心先作 ABORTED 决议，使迟到的提交凭据不能再获批。
        TxRecord decision = ledger.abortIntentTransaction(txId);
        List<IntentTerminalProof> proofs = new ArrayList<>(decision.participants().size());
        for (String shardId : decision.participants().stream().sorted().toList()) {
            IntentTerminalProof proof = requireShard(shardId).abortIntentWithProof(txId);
            if (proof.record().startTs() != decision.startTs()) {
                throw illegal(txId, "aborted participant differs from center transaction");
            }
            proofs.add(proof);
        }
        return ledger.markIntentAbortFinalized(txId, proofs);
    }

    /** 有界窗口并行不同事务，单笔失败仅保留该事务的持久恢复记录。 */
    private RecoveryCounts recoverParallel(List<Long> txIds, LongPredicate action) {
        if (txIds.isEmpty()) {
            return new RecoveryCounts(0, 0);
        }
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        Semaphore budget = new Semaphore(maxInFlight);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Void>> tasks = new ArrayList<>(txIds.size());
            for (long txId : txIds) {
                tasks.add(CompletableFuture.runAsync(() -> {
                    budget.acquireUninterruptibly();
                    try {
                        if (action.test(txId)) {
                            succeeded.incrementAndGet();
                        }
                    } catch (RuntimeException ex) {
                        lastFailure.set(ex);
                        failed.incrementAndGet();
                    } finally {
                        budget.release();
                    }
                }, workers));
            }
            tasks.forEach(CompletableFuture::join);
        }
        return new RecoveryCounts(succeeded.get(), failed.get());
    }

    /** 并行完成数与失败数都只在本轮局部汇总。 */
    private record RecoveryCounts(int succeeded, int failed) {
    }

    /** 缺失路由必须保留中心记录以等待恢复。 */
    private MysqlStorageAdapter requireShard(String shardId) {
        StorageAdapter adapter = shards.get(shardId);
        if (!(adapter instanceof MysqlStorageAdapter mysql)) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "cross-shard recovery route unavailable: " + shardId, false, null);
        }
        return mysql;
    }

    /** 构造需要停止最终化的持久身份错误。 */
    private MvccException illegal(long txId, String message) {
        return new MvccException(ErrorCode.ILLEGAL_STATE, message, false, txId);
    }

    /** 返回最近一次失败，供后台调度器展示和告警。 */
    public RuntimeException lastFailure() {
        return lastFailure.get();
    }
}
