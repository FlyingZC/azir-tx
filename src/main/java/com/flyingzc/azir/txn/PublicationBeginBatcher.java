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
import com.flyingzc.azir.common.IsolationLevel;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.metadata.BeginReservation;
import com.flyingzc.azir.publication.mysql.MysqlPublicationLedger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Starts the snapshot request for the same coordinator to merge to bounded share watermark lease transaction.
 */
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

    /**
     * Creates an bounded queue and a fixed virtual workspace.
     *
     * @param ledger ledger
     * @param timeoutMillis timeout millis
     * @param queueCapacity queue capacity
     * @param maxBatchSize max batch size
     * @param workerCount worker count
     * @throws IllegalArgumentException if the operation cannot complete
     */
    PublicationBeginBatcher(final MysqlPublicationLedger ledger, final long timeoutMillis, final int queueCapacity,
                            final int maxBatchSize, final int workerCount) {
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

    /**
     * Wait for your own lease commit and return the snapshot that will not be shared with other calls txId.
     *
     * @param isolationLevel global transaction isolation level
     * @return the operation result
     * @throws MvccException if the operation cannot complete
     */
    BeginReservation begin(final IsolationLevel isolationLevel) {
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
        } catch (final CompletionException ex) {
            if (ex.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw ex;
        }
    }

    /**
     * Emptyed the line of work after accepted request; responding to unknown orphan lease was deleted by GC by the deadline.
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
     * Returns the cumulative batch form of the bounded queue.
     *
     * @return the operation result
     */
    Stats stats() {
        return new Stats(groups.sum(), requests.sum(), fallbacks.sum());
    }

    /**
     * Requests are made in the Queue of Convergence, with the first isolated request having a minimum of one millisecond.
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
     * Batch failed or responded to unknown retry begin, old batch orphan lease protected only GC.
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
            List<BeginReservation> reservations = ledger.beginBatch(batch.size(), timeoutMillis);
            for (int index = 0; index < batch.size(); index++) {
                batch.get(index).result().complete(reservations.get(index));
            }
        } catch (final RuntimeException ex) {
            fallbacks.increment();
            for (Request request : batch) {
                resolveIndividually(request);
            }
        }
    }

    /**
     * Starts an independent retry snapshot and no transaction ID is used for unknown outcome batch.
     *
     * @param request request
     */
    private void resolveIndividually(final Request request) {
        try {
            request.result().complete(ledger.begin(request.isolationLevel(), timeoutMillis, false));
        } catch (final RuntimeException ex) {
            request.result().completeExceptionally(ex);
        }
    }

    /**
     * Saves the original level of isolation and its independent durable storage start result.
     *
     * @param isolationLevel global transaction isolation level
     * @param result result
     */
    private record Request(IsolationLevel isolationLevel, CompletableFuture<BeginReservation> result) {
    }

    /**
     * Returns the cumulative aggregate of lease batch and the number of backslides.
     *
     * @param groups groups
     * @param requests requests
     * @param fallbacks fallbacks
     */
    record Stats(long groups, long requests, long fallbacks) {
    }
}
