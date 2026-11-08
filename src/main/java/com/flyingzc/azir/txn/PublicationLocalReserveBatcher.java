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
import com.flyingzc.azir.publication.PublicationLocalCommitRequest;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Keep durable PENDING of a standalone shard transaction presumed as bounded control transaction.
 */
final class PublicationLocalReserveBatcher {

    private final MysqlPublicationLedger ledger;

    private final ArrayBlockingQueue<Request> queue;

    private final int maxBatchSize;

    private final List<Thread> workers;

    private final LongAdder groups = new LongAdder();

    private final LongAdder requests = new LongAdder();

    private final LongAdder fallbacks = new LongAdder();

    private boolean closed;

    /**
     * Creates bounded queue and limited virtual lines to avoid queuing requests for database connection.
     *
     * @param ledger ledger
     * @param queueCapacity queue capacity
     * @param maxBatchSize max batch size
     * @param workerCount worker count
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationLocalReserveBatcher(final MysqlPublicationLedger ledger, final int queueCapacity, final int maxBatchSize,
                                   final int workerCount) {
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

    /**
     * Waiting for your own slot durable storage, returns independent commit timestamp.
     *
     * @param txId transaction identifier
     * @param expiresAtMillis expires at millis
     * @param participant participant
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    long reserve(final long txId, final long expiresAtMillis, final String participant) {
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
        } catch (final CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /**
     * Pending PENDING is still taken over by durable recovery when the request has been accepted.
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
     * Returns the cumulative number of batch, the number of requests and the number of exits.
     *
     * @return the operation result
     */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /**
     * Collect the same request briefly after the first arrival and type commit.
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
                process(batch);
            } catch (final InterruptedException ex) {
                Thread.interrupted();
            }
        }
    }

    /**
     * X-0 failed by writing replay to parse commit responses and isolate transaction invalid.
     *
     * @param batch batch
     */
    private void process(final List<Request> batch) {
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
        } catch (final RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /**
     * Press transaction identity recovery or reserve a single slot.
     *
     * @param request request
     */
    private void resolveIndividually(final Request request) {
        try {
            PublicationLocalCommitRequest reservation = request.reservation();
            request.result().complete(ledger.reserveLocalCommit(
                    reservation.txId(), reservation.expiresAtMillis(), reservation.participant()));
        } catch (final RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }

    /**
     * An local direct commit reservation and its independent response.
     *
     * @param reservation reservation
     * @param result result
     */
    private record Request(PublicationLocalCommitRequest reservation, CompletableFuture<Long> result) {
    }

    /**
     * Returns the actual aggregation and regression statistics of bounded presets for batch.
     *
     * @param groups groups
     * @param requests requests
     * @param fallbacks fallbacks
     */
    record Stats(long groups, long requests, long fallbacks) {
    }
}
