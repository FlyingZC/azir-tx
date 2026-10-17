package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.IsolationLevel;
import mvcc.common.MvccException;
import mvcc.metadata.BeginReservation;
import mvcc.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** 把同一协调器的快照开始请求聚合到有界共享水位租约事务。 */
final class PublicationBeginBatcher {
    private final MysqlPublicationLedger ledger;
    private final long timeoutMillis;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final List<Thread> workers;
    private final LongAdder groups = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private boolean closed;

    /** 保存原隔离级别及其独立的持久化开始结果。 */
    private record Request(IsolationLevel isolationLevel, CompletableFuture<BeginReservation> result) {
    }

    /** 返回租约批次的累计聚合及逐笔回退次数。 */
    record Stats(long groups, long requests, long fallbacks) {
    }

    /** 创建有界队列与固定虚拟工作线程。 */
    PublicationBeginBatcher(MysqlPublicationLedger ledger, long timeoutMillis, int queueCapacity,
                            int maxBatchSize, int workerCount) {
        if (timeoutMillis < 1 || timeoutMillis > 86_400_000L || queueCapacity < 1
                || maxBatchSize < 2 || maxBatchSize > 64 || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid publication begin batch limits");
        }
        this.ledger = ledger;
        this.timeoutMillis = timeoutMillis;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-ledger-begin-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /** 等待自身租约提交并返回不会与其他调用共用 txId 的快照。 */
    BeginReservation begin(IsolationLevel isolationLevel) {
        var result = new CompletableFuture<BeginReservation>();
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "publication begin batcher is closed", false, null);
            }
            if (!queue.offer(new Request(isolationLevel, result))) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "publication begin batch queue is full", true, null);
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

    /** 排空已接受请求后关闭工作线程；响应未知的无主租约由 GC 按截止时间删除。 */
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

    /** 返回有界队列的累计批次形态。 */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /** 聚合队列中已有请求，首笔孤立请求最多短等一毫秒。 */
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

    /** 批次失败或响应未知时逐笔重试 begin，旧批次无主租约只会保守保护 GC。 */
    private void process(List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<BeginReservation> reservations = ledger.beginBatch(batch.size(), timeoutMillis);
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(reservations.get(index));
            }
        } catch (RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /** 独立重试一笔快照开始，不复用结果未知批次的任何事务号。 */
    private void resolveIndividually(Request request) {
        try {
            request.result().complete(ledger.begin(request.isolationLevel(), timeoutMillis, false));
        } catch (RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }
}
