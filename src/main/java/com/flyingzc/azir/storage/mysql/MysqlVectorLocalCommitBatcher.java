package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.Mutation;

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

/** 把独立客户端的本地单键事务有界合并为分片物理提交。 */
final class MysqlVectorLocalCommitBatcher implements AutoCloseable {
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

    /** 每个待提交逻辑事务拥有独立完成凭据，不能用批次成功掩盖个体结果。 */
    private record Pending(MysqlStorageAdapter.LocalVectorCommitRequest request,
                           CompletableFuture<Long> result) {
    }

    /** 固定分片、并行物理提交数、批量大小、等待窗口和队列上界。 */
    MysqlVectorLocalCommitBatcher(MysqlStorageAdapter adapter, int lanes, int maxBatchSize,
                                  long maxWaitMicros, int capacity) {
        this.adapter = Objects.requireNonNull(adapter, "adapter");
        if (lanes < 1 || lanes > 64 || maxBatchSize < 1 || maxBatchSize > 64
                || maxWaitMicros < 0 || maxWaitMicros > 10_000
                || capacity < maxBatchSize || capacity > 65_536) {
            throw new IllegalArgumentException("invalid vector local batcher limits");
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.maxBatchSize = maxBatchSize;
        this.maxWaitNanos = TimeUnit.MICROSECONDS.toNanos(maxWaitMicros);
        this.workers = Executors.newFixedThreadPool(lanes, task ->
                Thread.ofPlatform().daemon().name("azir-vector-local-batch-" + adapter.shardId()).unstarted(task));
        for (int index = 0; index < lanes; index++) {
            workers.submit(this::runLoop);
        }
    }

    /** 队列满时有界等待；返回的 Future 只在真实持久提交或失败核对后完成。 */
    CompletableFuture<Long> submit(MysqlStorageAdapter.LocalVectorCommitRequest request,
                                   long timeoutMillis) {
        Objects.requireNonNull(request, "request");
        if (timeoutMillis < 1 || closed.get()) {
            throw new IllegalArgumentException("closed batcher or invalid enqueue timeout");
        }
        CompletableFuture<Long> result = new CompletableFuture<>();
        Mutation mutation = request.mutation();
        byte[] value = mutation.value();
        Mutation copied = new Mutation(mutation.key(), mutation.type(),
                value == null ? null : value.clone());
        Pending pending = new Pending(new MysqlStorageAdapter.LocalVectorCommitRequest(
                request.txId(), copied, request.stripe(), request.startSequence()), result);
        try {
            if (!queue.offer(pending, timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new MvccException(ErrorCode.LOCK_WAIT_TIMEOUT,
                        "local vector batch queue is full", true, request.txId());
            }
            if (closed.get() && queue.remove(pending)) {
                result.completeExceptionally(unavailable(request.txId()));
            }
            return result;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new MvccException(ErrorCode.STORAGE_UNAVAILABLE,
                    "local vector batch enqueue interrupted", true, request.txId(), ex);
        }
    }

    /** 同键请求不进入同一物理批次；冲突由稳定键锁和版本检查最终裁决。 */
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
            lastFailure.set(ex);
            closed.set(true);
        } catch (Throwable ex) {
            lastFailure.set(ex);
            closed.set(true);
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

    /** 整批失败先查持久身份，再逐笔回退，不能盲目把结果未知当成回滚。 */
    private void flush(List<Pending> pending) {
        batches.increment();
        grouped.add(pending.size());
        try {
            List<Long> sequences = adapter.directCommitVectorLocalBatch(
                    pending.stream().map(Pending::request).toList());
            for (int index = 0; index < pending.size(); index++) {
                pending.get(index).result().complete(sequences.get(index));
            }
        } catch (RuntimeException batchFailure) {
            lastFailure.set(batchFailure);
            fallbackMembers.add(pending.size());
            for (Pending item : pending) {
                try {
                    var durable = adapter.verifyLocalVectorCommit(item.request());
                    long sequence = durable.isPresent() ? durable.getAsLong()
                            : adapter.directCommitVectorLocal(item.request().txId(),
                            item.request().mutation(), item.request().stripe(),
                            item.request().startSequence());
                    item.result().complete(sequence);
                } catch (RuntimeException memberFailure) {
                    item.result().completeExceptionally(memberFailure);
                }
            }
        }
    }

    /** 返回已经真正调用过物理批次的笔数。 */
    long batchCount() {
        return batches.sum();
    }

    /** 返回本轮真正参与物理组批的逻辑事务总数。 */
    long groupedCount() {
        return grouped.sum();
    }

    /** 单批失败后转入持久核对和逐笔提交的逻辑事务数。 */
    long fallbackCount() {
        return fallbackMembers.sum();
    }

    /** 异常证据仅用于监控；已回退成功的批次仍可有原始错误。 */
    Throwable lastFailure() {
        return lastFailure.get();
    }

    /** 中止等待中的任务使用可重试未知结果，调用方须按事务号复核。 */
    private static MvccException unavailable(long txId) {
        return new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                "local vector batcher stopped before confirming this transaction", true, txId);
    }

    /** 处理已入队请求后停止后台线程，关闭后拒绝新请求。 */
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
