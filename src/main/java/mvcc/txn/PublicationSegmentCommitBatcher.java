package mvcc.txn;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** 将同一 owner 分段内已预写事务合成一笔无全局状态行锁的控制决议。 */
final class PublicationSegmentCommitBatcher {
    private final PublicationSegmentOwnerRegistry owner;
    private final ArrayBlockingQueue<Request> queue;
    private final int maxBatchSize;
    private final List<Thread> workers;
    private final LongAdder groups = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder singletons = new LongAdder();
    private final LongAdder unknownGroups = new LongAdder();
    private final LongAdder synchronousFallbacks = new LongAdder();
    private boolean closed;

    /** 保存一笔决议请求及其独立的前台确认。 */
    private record Request(PublicationSegmentOwnerRegistry.CommitRequest commit,
                           CompletableFuture<Long> result) {
    }

    /** 返回实际批次规模及失去结果证明的批次数。 */
    record Stats(long groups, long requests, long singletons,
                 long unknownGroups, long synchronousFallbacks) {
    }

    /** 创建有界队列与固定数量的虚拟批处理线程。 */
    PublicationSegmentCommitBatcher(PublicationSegmentOwnerRegistry owner, int queueCapacity,
                                    int maxBatchSize, int workerCount) {
        if (queueCapacity < 1 || maxBatchSize < 2 || maxBatchSize > 64
                || workerCount < 1 || workerCount > 8) {
            throw new IllegalArgumentException("invalid segmented commit batch limits");
        }
        this.owner = owner;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxBatchSize = maxBatchSize;
        List<Thread> started = new ArrayList<>(workerCount);
        for (int index = 0; index < workerCount; index++) {
            started.add(Thread.ofVirtual().name("mvcc-segment-batch-" + index).start(this::run));
        }
        this.workers = List.copyOf(started);
    }

    /** 等待自己的持久中心决议，任何批次未知结果都保留原事务身份供恢复。 */
    long commit(PublicationSegmentOwnerRegistry.Assignment assignment, long txId,
                long deadlineMillis, Set<String> participants) {
        var result = new CompletableFuture<Long>();
        var request = new Request(new PublicationSegmentOwnerRegistry.CommitRequest(
                assignment, txId, deadlineMillis, participants), result);
        boolean direct;
        synchronized (queue) {
            direct = closed || !queue.offer(request);
        }
        if (direct) {
            synchronousFallbacks.increment();
            return owner.decide(assignment, txId, deadlineMillis, participants);
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

    /** 已接受请求处理结束后停止批量工作线程。 */
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

    /** 返回只读批次统计供隔离压测观察聚合程度。 */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), singletons.sum(),
                unknownGroups.sum(), synchronousFallbacks.sum());
    }

    /** 从有界队列短暂收集请求，再按分段身份分别提交。 */
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
                Map<Long, List<Request>> bySegment = new LinkedHashMap<>();
                for (Request request : batch) {
                    bySegment.computeIfAbsent(request.commit().assignment().segmentStart(),
                            ignored -> new ArrayList<>()).add(request);
                }
                bySegment.values().forEach(this::process);
            } catch (InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    /** 批次 SQL 失败后不猜测提交结果，交给后台按持久槽和元数据恢复。 */
    private void process(List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            singletons.increment();
            Request request = batch.getFirst();
            try {
                var commit = request.commit();
                request.result().complete(owner.decide(commit.assignment(), commit.txId(),
                        commit.deadlineMillis(), commit.participants()));
            } catch (RuntimeException ex) {
                request.result().completeExceptionally(ex);
            }
            return;
        }
        try {
            owner.decideBatch(batch.stream().map(Request::commit).toList());
            for (Request request : batch) {
                request.result().complete(request.commit().assignment().commitTs());
            }
        } catch (RuntimeException ex) {
            unknownGroups.increment();
            for (Request request : batch) {
                request.result().completeExceptionally(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "segmented batch outcome requires durable transaction lookup", false,
                        request.commit().txId(), ex));
            }
        }
    }
}
