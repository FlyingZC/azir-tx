package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** 成员票据的持久身份、角色、配置比较和损坏拒绝专项。 */
class RatisMembershipTicketTest {
    /** 保存后恢复保留全部成员字段，新节点初始非投票，最终仍为三票。 */
    @Test void durableTicketPreservesFreshIdentityAndTwoStageRosters() throws Exception {
        var plan = plan(); byte[] encoded = plan.encode(); var restored = RatisMemberReplacement.decode(encoded); assertArrayEquals(encoded, restored.encode());
        assertEquals(4, restored.joiningGroup().getPeers().size()); assertEquals(RaftPeerRole.LISTENER, restored.replacement().getStartupRole());
        assertEquals(0, restored.phase(restored.original())); assertEquals(1, restored.phase(restored.staged(8))); assertEquals(2, restored.phase(restored.completed(9)));
        assertEquals(3, restored.completed(9).voters().size()); assertTrue(restored.completed(9).learners().isEmpty()); assertFalse(restored.completed(9).voters().stream().anyMatch(peer -> peer.getId().equals(restored.removed())));
    }

    /** 切主同拓扑的配置序号前进可续做，其他节点或端点变化不能被票据覆盖。 */
    @Test void leaderNoopIsResumableButDifferentRosterOrAddressIsNot() {
        var plan = plan(); assertEquals(0, plan.phase(new RatisMembershipView(8, plan.original().voters(), List.of())));
        var other = RaftPeer.newBuilder().setId("other").setAddress("127.0.0.1:19005").setStartupRole(RaftPeerRole.LISTENER).build();
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(9, plan.original().voters(), List.of(other))));
        var moved = RaftPeer.newBuilder(plan.replacement()).setAddress("127.0.0.1:19006").build();
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(9, plan.original().voters(), List.of(moved))));
        assertThrows(IllegalStateException.class, () -> plan.phase(new RatisMembershipView(6, plan.original().voters(), List.of())));
    }

    /** 旧 ID、重复地址、错误角色和损坏票据都在任何共识请求以前拒绝。 */
    @Test void invalidRolesReusedIdentityAndCorruptionAreRejected() throws Exception {
        var plan = plan(); var reused = RaftPeer.newBuilder(plan.original().voters().getFirst()).setStartupRole(RaftPeerRole.LISTENER).build();
        assertThrows(IllegalArgumentException.class, () -> new RatisMemberReplacement(plan.operationId(), plan.groupId(), plan.original(), plan.removed(), reused));
        var duplicate = RaftPeer.newBuilder(plan.replacement()).setAddress(plan.original().voters().getFirst().getAddress()).build();
        assertThrows(IllegalArgumentException.class, () -> new RatisMemberReplacement(plan.operationId(), plan.groupId(), plan.original(), plan.removed(), duplicate));
        assertThrows(IllegalArgumentException.class, () -> new RatisMembershipView(1, plan.original().voters(), List.of(RaftPeer.newBuilder(plan.replacement()).setStartupRole(RaftPeerRole.FOLLOWER).build())));
        byte[] bad = plan.encode(); bad[20] ^= 1; assertThrows(IOException.class, () -> RatisMemberReplacement.decode(bad));
        assertThrows(IOException.class, () -> RatisMemberReplacement.decode(Arrays.copyOf(bad, 32)));
    }

    /** 构造固定原拓扑，操作 UUID 决定不可重用的新 Peer ID。 */
    private static RatisMemberReplacement plan() {
        var peers = new ArrayList<RaftPeer>(); for (int i = 0; i < 3; i++) { peers.add(RaftPeer.newBuilder().setId("n" + i).setAddress("127.0.0.1:" + (19001 + i)).build()); }
        UUID operation = UUID.randomUUID(); var fresh = RaftPeer.newBuilder().setId("r-" + operation).setAddress("127.0.0.1:19004").setStartupRole(RaftPeerRole.LISTENER).build();
        return new RatisMemberReplacement(operation, UUID.randomUUID(), new RatisMembershipView(7, peers, List.of()), peers.getLast().getId(), fresh);
    }
}
