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

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Ordinary regression must cover the small processor budget and cannot rely on a large number of cargo threads from the developer to be tested off.
 */
class WalLowProcessorCloseTest {

    @TempDir
    Path directory;

    /**
     * Fixed two processors in the subJVM, closed to unmountable carrier and could not be interrupted materialization.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void virtualCloseProgressesWithTwoCarriers() throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx128m", "-XX:ActiveProcessorCount=2", "-cp", classpath, WalCloseProbeProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            assertTrue(child.waitFor(45, TimeUnit.SECONDS), "owned low-processor child did not finish");
            Assertions.assertEquals(0, child.exitValue(), Files.readString(directory.resolve("child.log")));
            assertTrue(Files.readString(directory.resolve("child.log")).contains("low-processor close completed"));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                assertTrue(child.waitFor(30, TimeUnit.SECONDS));
            }
        }
    }
}
