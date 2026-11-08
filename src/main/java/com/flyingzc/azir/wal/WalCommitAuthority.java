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

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Duplicate a credible consensus port for commit; achieving it must be guaranteed that the majority durable character is separated from the term of office.
 */
public interface WalCommitAuthority {

    /**
     * Nonblocking returns the current serviceable leader ' s term of office and zero means that he is not an effective leader at this time.
     *
     * @return the operation result
     */
    long epoch();

    /**
     * Completion of majority read barrier and waiting for the local application to return to the remaining leadership.
     *
     * @param timeoutMillis timeout millis
     * @return the operation result
     * @throws Exception if the operation cannot complete
     */
    long readBarrier(long timeoutMillis) throws Exception;

    /**
     * Commit complete prepared batch; successful only with majority commit and local installation, cannot be automatically reintroduced over a term.
     *
     * @param epoch epoch
     * @param commits commits
     * @return the operation result
     */
    CompletionStage<Void> submit(long epoch, List<WalCommit> commits);
}
