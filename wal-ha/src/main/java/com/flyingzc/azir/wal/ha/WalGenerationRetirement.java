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

package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import java.io.IOException;
import java.util.UUID;

/**
 * The old generation retirement capability, which is granted on a temporary basis within the node lock, cannot be constructed or permanently maintained by ordinary operations.
 */
public final class WalGenerationRetirement {

    private final WalReplicaIdentity identity;

    private final UUID generation;

    private final Validator validator;

    /**
     * Only the durable status manager of this package is issued after checking current and installed generation.
     *
     * @param identity identity
     * @param generation generation
     * @param validator validator
     */
    WalGenerationRetirement(final WalReplicaIdentity identity, final UUID generation, final Validator validator) {
        this.identity = identity;
        this.generation = generation;
        this.validator = validator;
    }

    /**
     * Fixed node ownership for backend reconciliation.
     *
     * @return the operation result
     */
    public WalReplicaIdentity identity() {
        return identity;
    }

    /**
     * Fixed retirement object not to accept any physical database name.
     *
     * @return the operation result
     */
    public UUID generation() {
        return generation;
    }

    /**
     * Review permissions prior to each disruptive step remain within nodal lock, effective life cycle and work budget.
     *
     * @throws IOException if the operation cannot complete
     */
    public void verify() throws IOException {
        validator.verify();
    }

    /**
     * The licence and the single node are to be used to maintain the call binding, which will expire after withdrawal.
     */
    @FunctionalInterface
    interface Validator {

        /**
         * Failure to meet the conditions of protection must fail and cannot be downgraded to a direct deletion.
         *
         * @throws IOException if the operation cannot complete
         */
        void verify() throws IOException;
    }
}
