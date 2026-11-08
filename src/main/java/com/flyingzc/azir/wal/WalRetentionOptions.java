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

package com.flyingzc.azir.wal;

/**
 * The new mode checkpoint and commit receipt collection policy does not change the configuration or access to the original Azir.
 *
 * @param receiptRetentionMillis receipt retention millis
 * @param checkpointIntervalMillis checkpoint interval millis
 * @param checkpointWalBytes checkpoint wal bytes
 * @param maxReceiptArchiveBytes max receipt archive bytes
 * @param maxPendingReceipts max pending receipts
 */
public record WalRetentionOptions(long receiptRetentionMillis, long checkpointIntervalMillis,
        long checkpointWalBytes, long maxReceiptArchiveBytes,
        int maxPendingReceipts) {

    /**
     * Zero retention period indicates permanent reservation, with a clear back pressure when the capacity reaches budget instead of discarding receipt.
     *
     * @param receiptRetentionMillis receipt retention millis
     * @param checkpointIntervalMillis checkpoint interval millis
     * @param checkpointWalBytes checkpoint wal bytes
     * @param maxReceiptArchiveBytes max receipt archive bytes
     * @param maxPendingReceipts max pending receipts
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public WalRetentionOptions {
        if (receiptRetentionMillis < 0 || checkpointIntervalMillis < 1 || checkpointWalBytes < 1024
                || maxReceiptArchiveBytes < 4096 || maxPendingReceipts < 1 || maxPendingReceipts > 1_000_000) {
            throw new IllegalArgumentException("invalid WAL retention policy");
        }
    }

    /**
     * Keeps the existing result query contract, and collection is allowed to expire only if the caller has clearly set a deadline.
     *
     * @return the operation result
     */
    public static WalRetentionOptions preserveAll() {
        return new WalRetentionOptions(0, 60_000, 4L * 1024 * 1024, 512L * 1024 * 1024, 100_000);
    }

    /**
     * Returns the policy of clearly specifying a receipt retention window, with the remaining resource thresholds being consistent.
     *
     * @param millis millis
     * @return the operation result
     */
    public WalRetentionOptions withReceiptRetentionMillis(final long millis) {
        return new WalRetentionOptions(millis, checkpointIntervalMillis, checkpointWalBytes,
                maxReceiptArchiveBytes, maxPendingReceipts);
    }
}
