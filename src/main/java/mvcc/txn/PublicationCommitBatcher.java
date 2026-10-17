package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.PublicationCommitResult;
import mvcc.publication.PublicationIntentCommit;
import mvcc.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** 将同一协调器已预写的事务合并到有界控制库提交批次。 */
final class PublicationCommitBatcher {
    private final MysqlPublicationLedger ledger;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final List<Thread> workers;
    private final LongAdder groups = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder singletons = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private boolean closed;

    /** 保存一次提交请求及其独立的完成结果。 */
    private record Request(PublicationIntentCommit commit, CompletableFuture<PublicationCommitResult> result) {
    }

    /** 汇报实际聚合程度与批次失败回退次数。 */
    record Stats(long groups, long requests, long singletons, long fallbacks) {
    }

    /** 建立有界队列和指定数量的虚拟线程，避免排队请求占用控制库连接。 */
    PublicationCommitBatcher(MysqlPublicationLedger ledger, int queueCapacity, int maxBatchSize,
                             int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid publication batch limits");
        }
        this.ledger = ledger;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-publication-batch-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /** 等待自身持久化决议，批次失败时仍按事务身份返回独立结果。 */
    PublicationCommitResult commit(long txId, long expiresAtMillis, Set<String> participants) {
        var result = new CompletableFuture<PublicationCommitResult>();
        var request = new Request(new PublicationIntentCommit(txId, expiresAtMillis, participants), result);
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication batcher is closed", false, txId);
            }
            if (!queue.offer(request)) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "publication batch queue is full", true, txId);
            }
        }
        try {
            return result.join();
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /** 处理完已接受的请求后关闭工作线程，未决事务仍由持久恢复接管。 */
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

    /** 返回只读累计统计，供隔离压测观察批次形态。 */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), singletons.sum(), fallbacks.sum());
    }

    /** 首笔请求到达后短暂汇集同批事务，控制每批上限与前台等待。 */
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
                // 线程仅负责协调批次；清除中断标志后继续交付已接受的请求。
                Thread.interrupted();
            }
        }
    }

    /** 批量失败或响应未知时逐笔重放，利用单笔接口的幂等决议校验隔离失败。 */
    private void process(List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            singletons.increment();
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<PublicationCommitResult> decisions = ledger.commitPreparedIntentBatch(
                    batch.stream().map(Request::commit).toList());
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(decisions.get(index));
            }
        } catch (RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /** 单笔重放既能解析已提交批次，也能隔离批次中的无效事务。 */
    private void resolveIndividually(Request request) {
        try {
            PublicationIntentCommit commit = request.commit();
            request.result().complete(ledger.commitPreparedIntents(
                    commit.txId(), commit.expiresAtMillis(), commit.participants()));
        } catch (RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }
}
