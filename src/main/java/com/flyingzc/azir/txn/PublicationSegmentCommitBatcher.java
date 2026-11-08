/*
 * Copyright 2026 Azir TX contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flyingzc.azir.txn;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;

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

/**
 * Combining prewrite transaction in the same own sub-section with a control decision without a lock on global status.
 */
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

    /**
     * Creates the bounded queue with a fixed number of virtual batch threads.
     *
     * @param owner owner
     * @param queueCapacity queue capacity
     * @param maxBatchSize max batch size
     * @param workerCount worker count
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationSegmentCommitBatcher(final PublicationSegmentOwnerRegistry owner, final int queueCapacity,
                                    final int maxBatchSize, final int workerCount) {
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

    /**
     * Waiting for its own durable center decision, any unknown result of batch retains the original transaction identity for recovery.
     *
     * @param assignment assignment
     * @param txId transaction identifier
     * @param deadlineMillis deadline millis
     * @param participants participants
     * @return the operation result
     */
    long commit(final PublicationSegmentOwnerRegistry.Assignment assignment, final long txId,
                final long deadlineMillis, final Set<String> participants) {
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
        } catch (final CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /**
     * Stop batch lines of work after the requested processing has been accepted.
     *
     * @param timeoutMillis timeout millis
     */
    void close(final long timeoutMillis) {
        synchronized (queue) {
            closed = true;
        }
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            for (Thread worker : workers) {
                worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns read-only batch statistics for quarantine pressure measurements to observe aggregates.
     *
     * @return the operation result
     */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), singletons.sum(),
                unknownGroups.sum(), synchronousFallbacks.sum());
    }

    /**
     * Collect requests from the bounded queue briefly, followed by the identity line by the commit.
     */
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
            } catch (final InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    /**
     * Batch SQL failed without guessing the result of commit and handed it to background by pressing durable slots and metadata recovery.
     *
     * @param batch batch
     */
    private void process(final List<Request> batch) {
        groups.increment();
        requests.add(batch.size());
        if (batch.size() == 1) {
            singletons.increment();
            Request request = batch.getFirst();
            try {
                var commit = request.commit();
                request.result().complete(owner.decide(commit.assignment(), commit.txId(),
                        commit.deadlineMillis(), commit.participants()));
            } catch (final RuntimeException ex) {
                request.result().completeExceptionally(ex);
            }
            return;
        }
        try {
            owner.decideBatch(batch.stream().map(Request::commit).toList());
            for (Request request : batch) {
                request.result().complete(request.commit().assignment().commitTs());
            }
        } catch (final RuntimeException ex) {
            unknownGroups.increment();
            for (Request request : batch) {
                request.result().completeExceptionally(new MvccException(ErrorCode.TRANSACTION_STATUS_UNKNOWN,
                        "segmented batch outcome requires durable transaction lookup", false,
                        request.commit().txId(), ex));
            }
        }
    }

    /**
     * Saves an decision request and its independent foreground confirmation.
     *
     * @param commit commit
     * @param result result
     */
    private record Request(PublicationSegmentOwnerRegistry.CommitRequest commit,
            CompletableFuture<Long> result) {
    }

    /**
     * Returns the actual batch size and the number of batch s lost in results.
     *
     * @param groups groups
     * @param requests requests
     * @param singletons singletons
     * @param unknownGroups unknown groups
     * @param synchronousFallbacks synchronous fallbacks
     */
    record Stats(long groups, long requests, long singletons,
            long unknownGroups, long synchronousFallbacks) {
    }
}
