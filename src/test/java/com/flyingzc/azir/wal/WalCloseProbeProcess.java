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

import java.nio.file.Path;

/**
 * Close return with fixed two virtual liner vehicles to avoid the big nuclear developer covering the pinning.
 */
public final class WalCloseProbeProcess {

    /**
     * Independent JVM probe does not allow for exemplification.
     */
    private WalCloseProbeProcess() {
    }

    /**
     * Re-use materialization true closure asserted that the number of processors was significantly limited by the parent process.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        var test = new WalCancellationAcceptanceTest();
        test.directory = Path.of(args[0]);
        test.closeDoesNotInterruptAnInFlightMaterializer();
        System.out.println("low-processor close completed");
    }
}
