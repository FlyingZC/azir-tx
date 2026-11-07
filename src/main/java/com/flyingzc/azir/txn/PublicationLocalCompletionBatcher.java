package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.publication.PublicationLocalCommitProof;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;
import com.flyingzc.azir.storage.ParticipantRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** 聚合已获得分片 COMMITTED 证明的中心补决议和连续水位发布。 */
final class PublicationLocalCompletionBatcher {
    private final MysqlPublicationLedger ledger;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final List<Thread> workers;
    private final LongAdder groups = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private boolean closed;

    /** 一份真实本地终态证明及独立发布结果。 */
    private record Request(PublicationLocalCommitProof proof, CompletableFuture<Boolean> result) {
    }

    /** 汇报实际补决议批次大小与失败回退。 */
    record Stats(long groups, long requests, long fallbacks) {
    }

    /** 创建有界工作队列及有限后台虚拟线程。 */
    PublicationLocalCompletionBatcher(MysqlPublicationLedger ledger, int queueCapacity, int maxBatchSize,
                                      int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid local completion batch limits");
        }
        this.ledger = ledger;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-local-complete-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /** 等待自身中心决议提交，并返回本批是否已经越过自身发布水位。 */
    boolean complete(ParticipantRecord local, long commitTs, String participant) {
        var future = new CompletableFuture<Boolean>();
        var request = new Request(new PublicationLocalCommitProof(local, commitTs, participant), future);
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local completion batcher is closed", false, local.txId());
            }
            if (!queue.offer(request)) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "local completion batch queue is full", true, local.txId());
            }
        }
        try {
            return future.join();
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /** 处理完已接受的证明后退出，未决证明仍可由独立恢复器重放。 */
    void close(long timeoutMillis) {
        synchronized (queue) {
            closed = true;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            for (Thread worker : workers) {
                worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /** 返回累计批次聚合程度。 */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /** 首笔证明到达后短暂收集同批并持久化结果。 */
    private void run() {
        while (true) {
            try {
                Request first = queue.poll(10, TimeUnit.MILLISECONDS);
                if (first == null) {
                    synchronized (queue) {
                        if (closed && queue.isEmpty()) {
                            return;
                        }
                    }
                    continue;
                }
                List<Request> batch = new ArrayList<>(maxBatchSize);
                batch.add(first);
                queue.drainTo(batch, maxBatchSize - 1);
                if (batch.size() == 1) {
                    Request second = queue.poll(1, TimeUnit.MILLISECONDS);
                    if (second != null) {
                        batch.add(second);
                        queue.drainTo(batch, maxBatchSize - batch.size());
                    }
                }
                process(batch);
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    /** 批次失败或提交响应未知时逐笔重放，以持久账本终态隔离问题事务。 */
    private void process(List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<Boolean> results = ledger.completeLocalCommitBatch(batch.stream().map(Request::proof).toList());
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(results.get(index));
            }
        } catch (RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /** 以原本地 COMMITTED 证明逐笔解析中心决议。 */
    private void resolveIndividually(Request request) {
        try {
            PublicationLocalCommitProof proof = request.proof();
            request.result().complete(ledger.completeLocalCommitAfterFencing(
                    proof.local(), proof.commitTs(), proof.participant()));
        } catch (RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }
}
