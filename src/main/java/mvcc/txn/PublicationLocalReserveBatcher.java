package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.publication.PublicationLocalCommitRequest;
import mvcc.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** 把独立单分片事务的持久 PENDING 预留聚合为有界控制事务。 */
final class PublicationLocalReserveBatcher {
    private final MysqlPublicationLedger ledger;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final List<Thread> workers;
    private final LongAdder groups = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder fallbacks = new LongAdder();
    private boolean closed;

    /** 一笔本地直接提交预留及其独立响应。 */
    private record Request(PublicationLocalCommitRequest reservation, CompletableFuture<Long> result) {
    }

    /** 返回有界预留批次的实际聚合和回退统计。 */
    record Stats(long groups, long requests, long fallbacks) {
    }

    /** 创建有界队列和有限虚拟线程，避免排队请求占用数据库连接。 */
    PublicationLocalReserveBatcher(MysqlPublicationLedger ledger, int queueCapacity, int maxBatchSize,
                                   int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid local publication batch limits");
        }
        this.ledger = ledger;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-local-reserve-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /** 等待自身时间槽持久化，返回独立提交时间戳。 */
    long reserve(long txId, long expiresAtMillis, String participant) {
        var future = new CompletableFuture<Long>();
        var request = new Request(new PublicationLocalCommitRequest(txId, expiresAtMillis, participant), future);
        synchronized (queue) {
            if (closed) {
                throw new MvccException(ErrorCode.ILLEGAL_STATE,
                        "local reservation batcher is closed", false, txId);
            }
            if (!queue.offer(request)) {
                throw new MvccException(ErrorCode.RESOURCE_EXHAUSTED,
                        "local reservation batch queue is full", true, txId);
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

    /** 完成已接受请求后退出，未决 PENDING 仍由持久恢复器接管。 */
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

    /** 返回累计批次数、请求数和逐笔回退次数。 */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /** 首笔到达后短暂收集同批请求并逐批提交。 */
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

    /** 批次失败时逐笔重放可解析已提交响应，也能隔离无效事务。 */
    private void process(List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            resolveIndividually(batch.getFirst());
            return;
        }
        try {
            List<Long> reserved = ledger.reserveLocalCommitBatch(
                    batch.stream().map(Request::reservation).toList());
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(reserved.get(index));
            }
        } catch (RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /** 按原事务身份恢复或单独预留一个时间槽。 */
    private void resolveIndividually(Request request) {
        try {
            PublicationLocalCommitRequest reservation = request.reservation();
            request.result().complete(ledger.reserveLocalCommit(
                    reservation.txId(), reservation.expiresAtMillis(), reservation.participant()));
        } catch (RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }
}
