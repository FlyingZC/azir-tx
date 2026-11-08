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

import com.flyingzc.azir.wal.WalMvccException;
import com.flyingzc.azir.wal.WalOptions;
import com.flyingzc.azir.wal.WalTransaction;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import com.flyingzc.azir.common.IsolationLevel;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Independent JVM failure probe, stdin is only used to test schedule, not as a product RPC or authentication protocol.
 */
public final class RatisWalProcessProbe {

    /**
     * Prevent instantiation of this utility class.
     */
    private RatisWalProcessProbe() {
    }

    /**
     * Each JVM runs an actual Raft/MVCC node and the result is recorded by the parent process before being injected into a killing.
     *
     * @param args args
     * @throws Exception if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     * @throws IllegalStateException if the operation cannot complete
     */
    public static void main(final String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        String id = args[1];
        UUID groupId = UUID.fromString(args[2]);
        var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) {
            peers.add(RaftPeer.newBuilder().setId("p" + i).setAddress("127.0.0.1:" + args[3 + i]).build());
        }
        var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
        try (
                var node = RatisWalNode.openLoopback(directory, RaftPeerId.valueOf(id), group, Set.of("a", "b"), Map.of(), WalOptions.defaults().withTimeout(5000));
                var input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            result("STARTED " + ProcessHandle.current().pid());
            WalTransaction buffered = null;
            for (String command; (command = input.readLine()) != null;) {
                String[] fields = command.split(" ", 2);
                try {
                    switch (fields[0]) {
                        case "STATUS" -> result("STATUS " + node.leaderEpoch() + " " + node.appliedSequence());
                        case "PING" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                            long start = tx.startSequence();
                            node.commit(tx);
                            result("PING " + start);
                        }
                        case "WRITE" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                            result("INTENT " + tx.id());
                            byte[] value = Base64.getUrlDecoder().decode(fields[1]);
                            node.put(tx, "a", "k", value);
                            node.put(tx, "b", "k", value);
                            var receipt = node.commit(tx);
                            result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence());
                        }
                        case "BUFFER" -> {
                            if (buffered != null) {
                                throw new IllegalStateException("buffered transaction already exists");
                            }
                            buffered = node.begin(IsolationLevel.REPEATABLE_READ);
                            byte[] value = Base64.getUrlDecoder().decode(fields[1]);
                            node.put(buffered, "a", "k", value);
                            node.put(buffered, "b", "k", value);
                            result("BUFFERED " + buffered.id());
                        }
                        case "FINISH" -> {
                            if (buffered == null) {
                                throw new IllegalStateException("no buffered transaction");
                            }
                            var tx = buffered;
                            buffered = null;
                            var receipt = node.commit(tx);
                            result("COMMITTED " + receipt.transactionId() + " " + receipt.commitSequence());
                        }
                        case "READ" -> {
                            var tx = node.begin(IsolationLevel.REPEATABLE_READ);
                            var snapshot = node.beginReadStatement(tx);
                            String a = encode(node.get(tx, "a", "k", snapshot));
                            String b = encode(node.get(tx, "b", "k", snapshot));
                            node.rollback(tx);
                            result("VALUES " + a + " " + b);
                        }
                        case "LOOKUP" -> result("LOOKUP " + node.lookupCommit(UUID.fromString(fields[1])).status());
                        case "STOP" -> {
                            result("STOPPING");
                            return;
                        }
                        default -> throw new IllegalArgumentException("unknown probe command");
                    }
                } catch (final Exception failure) {
                    result("ERROR " + failure.getClass().getSimpleName() + (failure instanceof WalMvccException wal ? " " + wal.errorCode() : ""));
                }
            }
        }
    }

    /**
     * Make a clear distinction between empty values and actual binary payload.
     *
     * @param value value to write
     * @return the operation result
     */
    private static String encode(final java.util.Optional<byte[]> value) {
        return value.map(bytes -> Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)).orElse("-");
    }

    /**
     * Each business result is immediately flush, and the parent process is counted as entering the confirmation prefix after it has been successfully received.
     *
     * @param value value to write
     */
    private static void result(final String value) {
        System.out.println("AZIR " + value);
        System.out.flush();
    }
}
