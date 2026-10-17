package mvcc.storage.mysql;

import mvcc.common.ErrorCode;
import mvcc.common.MvccException;
import mvcc.storage.Mutation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** 每个分片独立合并跨事务预写，前台仍逐笔等待自己的持久保留证明。 */
final class MysqlVectorCrossPrewriteBatcher implements AutoCloseable {
    private final MysqlStorageAdapter adapter;
    private final ArrayBlockingQueue<Pending> queue;
    private final int maxBatchSize;
    private final long maxWaitNanos;
    private final ExecutorService workers;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final LongAdder batches = new LongAdder();
    private final LongAdder grouped = new LongAdder();
    private final LongAdder fallbackMembers = new LongAdder();
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

    /** 事务自己的 Future 只接收该事务同分片的持久预写凭据。 */
    private record Pending(MysqlStorageAdapter.VectorPrewriteRequest request,
                           CompletableFuture<MysqlStorageAdapter.PrewriteProof> result) {
    }

    /** 固定分片并发、单批上限、收集窗口和内存积压预算。 */
    MysqlVectorCrossPrewriteBatcher(MysqlStorageAdapter adapter, int lanes,
                                    int maxBatchSize, long maxWaitMicros, int capacity) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        if (lanes < 1 || lanes > 64 || maxBatchSize < 1 || maxBatchSize > 64
                || maxWaitMicros < 0 || maxWaitMicros > 10_000
                || capacity < maxBatchSize || capacity > 65_536) {
            throw new IllegalArgumentException("invalid vector cross prewrite batcher limits");
        }
        queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatchSize = maxBatchSize;
        maxWaitNanos = TimeUnit.MICROSECONDS.toNanos(maxWaitMicros);
        workers = Executors.newFixedThreadPool(lanes, task ->
                Thread.ofPlatform().daemon().name("azir-vector-cross-prewrite-" + adapter.shardId()).unstarted(task));
        for (int index = 0; index < lanes; index++) {
            workers.submit(this::runLoop);
        }
    }

    /** 排队前复制载荷，超出队列预算时不给前台虚假预写成功凭据。 */
    CompletableFuture<MysqlStorageAdapter.PrewriteProof> submit(
            MysqlStorageAdapter.VectorPrewriteRequest request, long timeoutMillis) {
        Objects.requireNonNull(request, "request");
        if (timeoutMillis < 1 || closed.get()) {
            throw new IllegalArgumentException("closed vector prewrite batcher or invalid timeout");
        }
        Mutation mutation = request.mutation();
        byte[] value = mutation.value();
        Mutation copied = new Mutation(mutation.key(), mutation.type(),
                value == null ? null : value.clone());
        var fixed = new MysqlStorageAdapter.VectorPrewriteRequest(request.txId(), request.startTs(),
                request.expireAtMillis(), copied, request.stripe(), request.startSequence());
        var result = new CompletableFuture<MysqlStorageAdapter.PrewriteProof>();
        Pending pending = new Pending(fixed, result);
        try {
            if (!queue.offer(pending, timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "vector cross prewrite queue is full", true, request.txId());
            }
            if (closed.get() && queue.remove(pending)) {
                result.completeExceptionally(unavailable(request.txId()));
            }
            return result;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "vector cross prewrite enqueue interrupted", true, request.txId(), ex);
        }
    }

    /** 不让同键或同事务出现在同一物理批次，避免相互覆盖身份。 */
    private void runLoop() {
        Pending deferred = null;
        try {
            while (!closed.get() || deferred != null || !queue.isEmpty()) {
                Pending first = deferred != null ? deferred : queue.poll(100, TimeUnit.MILLISECONDS);
                deferred = null;
                if (first == null) {
                    continue;
                }
                List<Pending> pending = new ArrayList<>(maxBatchSize);
                pending.add(first);
                Set<String> keys = new HashSet<>();
                Set<Long> txIds = new HashSet<>();
                keys.add(first.request().mutation().key());
                txIds.add(first.request().txId());
                long deadline = System.nanoTime() + maxWaitNanos;
                while (pending.size() < maxBatchSize) {
                    long remaining = deadline - System.nanoTime();
                    Pending next = remaining > 0 ? queue.poll(remaining, TimeUnit.NANOSECONDS) : queue.poll();
                    if (next == null) {
                        break;
                    }
                    if (!keys.add(next.request().mutation().key()) || !txIds.add(next.request().txId())) {
                        deferred = next;
                        break;
                    }
                    pending.add(next);
                }
                flush(pending);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            closed.set(true);
            lastFailure.set(ex);
        } catch (Throwable ex) {
            closed.set(true);
            lastFailure.set(ex);
        } finally {
            if (deferred != null) {
                deferred.result().completeExceptionally(unavailable(deferred.request().txId()));
            }
            Pending abandoned;
            while ((abandoned = queue.poll()) != null) {
                abandoned.result().completeExceptionally(unavailable(abandoned.request().txId()));
            }
        }
    }

    /** 整批故障逐笔重放单键例程，既能核对未知提交也隔离确定冲突。 */
    private void flush(List<Pending> pending) {
        batches.increment();
        grouped.add(pending.size());
        try {
            List<MysqlStorageAdapter.PrewriteProof> proofs = adapter.prewriteVectorBatchWithProof(
                    pending.stream().map(Pending::request).toList());
            for (int index = 0; index < pending.size(); index++) {
                pending.get(index).result().complete(proofs.get(index));
            }
        } catch (RuntimeException batchFailure) {
            lastFailure.set(batchFailure);
            fallbackMembers.add(pending.size());
            for (Pending item : pending) {
                try {
                    var request = item.request();
                    item.result().complete(adapter.prewriteWithVectorProof(request.txId(),
                            request.startTs(), request.mutation(), request.stripe(),
                            request.startSequence()));
                } catch (RuntimeException memberFailure) {
                    item.result().completeExceptionally(memberFailure);
                }
            }
        }
    }

    /** 返回已经执行过的物理批次数。 */
    long batchCount() {
        return batches.sum();
    }

    /** 返回参与批量尝试的逻辑事务数。 */
    long groupedCount() {
        return grouped.sum();
    }

    /** 物理批次失败后逐笔重放的事务数。 */
    long fallbackCount() {
        return fallbackMembers.sum();
    }

    /** 暴露最近原始异常，已成功回退的批次也保留诊断证据。 */
    Throwable lastFailure() {
        return lastFailure.get();
    }

    /** 停机期间未知结果必须让调用方复核真实参与者，不得推断已回滚。 */
    private static MvccException unavailable(long txId) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "vector cross prewrite batcher stopped before proving this participant", true, txId);
    }

    /** 消费已入队请求后停止，不再接纳新跨片预写。 */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(15, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException ex) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
