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

import org.apache.ratis.protocol.RaftPeer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.junit.jupiter.api.Test;
import java.io.IOException;

/**
 * Durable identity, role, configuration comparison and damage to member instruments reject specifics.
 */
class RatisMembershipTicketTest {

    /**
     * After saving recovery reserves all member fields, the initial non-voting at the new node is still three votes.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void durableTicketPreservesFreshIdentityAndTwoStageRosters() throws Exception {
        var plan = plan();
        byte[] encoded = plan.encode();
        var restored = RatisMemberReplacement.decode(encoded);
        assertArrayEquals(encoded, restored.encode());
        Assertions.assertEquals(4, restored.joiningGroup().getPeers().size());
        Assertions.assertEquals(RaftPeerRole.LISTENER, restored.replacement().getStartupRole());
        Assertions.assertEquals(0, restored.phase(restored.original()));
        Assertions.assertEquals(1, restored.phase(restored.staged(8)));
        Assertions.assertEquals(2, restored.phase(restored.completed(9)));
        Assertions.assertEquals(3, restored.completed(9).voters().size());
        assertTrue(restored.completed(9).learners().isEmpty());
        assertFalse(restored.completed(9).voters().stream().anyMatch(peer -> peer.getId().equals(restored.removed())));
    }

    /**
     * The main and extended configuration numbers can be continued, and other nodes or endpoint changes cannot be covered by the instrument.
     */
    @Test
    void leaderNoopIsResumableButDifferentRosterOrAddressIsNot() {
        var plan = plan();
        Assertions.assertEquals(0, plan.phase(new RatisMembershipView(8, plan.original().voters(), List.of())));
        var other = RaftPeer.newBuilder().setId("other").setAddress("127.0.0.1:19005").setStartupRole(RaftPeerRole.LISTENER).build();
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(9, plan.original().voters(), List.of(other))));
        var moved = RaftPeer.newBuilder(plan.replacement()).setAddress("127.0.0.1:19006").build();
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(9, plan.original().voters(), List.of(moved))));
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(6, plan.original().voters(), List.of())));
    }

    /**
     * Old IDs, duplicate addresses, wrong characters and damaged instruments were rejected prior to any consensus request.
     *
     * @throws Exception if the operation cannot complete
     */
    @Test
    void invalidRolesReusedIdentityAndCorruptionAreRejected() throws Exception {
        var plan = plan();
        var reused = RaftPeer.newBuilder(plan.original().voters().getFirst()).setStartupRole(RaftPeerRole.LISTENER).build();
        assertThrows(IllegalArgumentException.class, () -> new RatisMemberReplacement(plan.operationId(), plan.groupId(), plan.original(), plan.removed(), reused));
        var duplicate = RaftPeer.newBuilder(plan.replacement()).setAddress(plan.original().voters().getFirst().getAddress()).build();
        assertThrows(IllegalArgumentException.class, () -> new RatisMemberReplacement(plan.operationId(), plan.groupId(), plan.original(), plan.removed(), duplicate));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisMembershipView(1, plan.original().voters(), List.of(RaftPeer.newBuilder(plan.replacement()).setStartupRole(RaftPeerRole.FOLLOWER).build())));
        byte[] bad = plan.encode();
        bad[20] ^= 1;
        assertThrows(IOException.class, () -> RatisMemberReplacement.decode(bad));
        assertThrows(IOException.class, () -> RatisMemberReplacement.decode(Arrays.copyOf(bad, 32)));
    }

    /**
     * Constructs a fixed Zooming and operates UUID to determine the new Peer ID that is not reusable.
     *
     * @return the operation result
     */
    private static RatisMemberReplacement plan() {
        var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < 3; i++) {
            peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build());
        }
        UUID operation = UUID.randomUUID();
        var fresh = RaftPeer.newBuilder().setId("r-" + operation).setAddress("127.0.0.1:19004").setStartupRole(RaftPeerRole.LISTENER).build();
        return new RatisMemberReplacement(operation, UUID.randomUUID(), new RatisMembershipView(7, peers, List.of()), peers.getLast().getId(), fresh);
    }
}
