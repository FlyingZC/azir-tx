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

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Assertions;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The new mirror must stop the old application from missing cold conflict fence and continue to operate after that, while retaining its ability to upgrade reading.
 */
class WalCheckpointFormatTest {

    /**
     * Encoding version to Two, version One is only read as an old mirror and must be recoded with a new version.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void newImagesRejectLegacyReaderButLegacyImagesRemainReadable() throws Exception {
        var state = image();
        byte[] current = state.encode();
        Assertions.assertEquals(2, ByteBuffer.wrap(current).getInt(4));
        byte[] legacy = current.clone();
        ByteBuffer.wrap(legacy).putInt(4, 1);
        var restored = WalCheckpointState.decode(legacy);
        Assertions.assertEquals(state.engineId(), restored.engineId());
        Assertions.assertEquals(state.versions(), restored.versions());
        Assertions.assertEquals(2, ByteBuffer.wrap(restored.encode()).getInt(4));
        // The previous version of the decoder required format I, and no new mirror could be misconceived as an empty engine for a conflict-free history.
        Assertions.assertNotEquals(1, ByteBuffer.wrap(current).getInt(4));
    }

    /**
     * Unknown format cannot be guessed compatible and can not continue after the recovery field is lost on a future mirror.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void unknownImageVersionsFailClosed() throws Exception {
        for (int format : new int[]{0, 3, Integer.MAX_VALUE}) {
            byte[] bytes = image().encode();
            ByteBuffer.wrap(bytes).putInt(4, format);
            assertThrows(IOException.class, () -> WalCheckpointState.decode(bytes));
        }
    }

    /**
     * The smallest mirror of the constructed materialization header, and the old verify field is still fully disassembled instead of being left silent.
     *
     * @return the operation result
     */
    private static WalCheckpointState image() {
        var receipt = new WalReceipt(UUID.randomUUID(), 1, "0".repeat(64));
        return new WalCheckpointState(UUID.randomUUID(), Set.of("a"), Set.of("a"), 1, 0, receipt,
                List.of(new WalCheckpointState.Version(1, new WalMutation("a", "k", null))), List.of(), new byte[128], 0);
    }
}
