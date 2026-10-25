package mvcc.wal.ha;

import org.apache.ratis.protocol.RaftPeer;
import java.util.*;

/** 已提交且稳定的成员视图，投票成员始终保持三个，额外成员只能作为非投票副本。 */
public record RatisMembershipView(long index, List<RaftPeer> voters, List<RaftPeer> learners) {
    /** 固定排序和完整端点，不能以相同 ID 掩盖地址或角色改变。 */
    public RatisMembershipView {
        voters = sorted(voters); learners = sorted(learners);
        if (index < -1 || voters.size() != 3 || learners.size() > 6) { throw new IllegalArgumentException("membership requires three voters and bounded learners"); }
        if (voters.stream().anyMatch(peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                || learners.stream().anyMatch(peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)) { throw new IllegalArgumentException("membership roles mismatch"); }
        var ids = new HashSet<String>(); var addresses = new HashSet<String>();
        for (var peer : java.util.stream.Stream.concat(voters.stream(), learners.stream()).toList()) {
            validate(peer);
            if (!ids.add(peer.getId().toString()) || !addresses.add(peer.getAddress())) { throw new IllegalArgumentException("duplicate membership identity or address"); }
        }
    }

    /** 成员描述支持正式主机名和 IP，明文入口另行限制回环，认证入口核对全部端点授权。 */
    static void validate(RaftPeer peer) {
        if (!peer.getId().toString().matches("[A-Za-z0-9._-]{1,64}") || peer.getAddress() == null) { throw new IllegalArgumentException("invalid member identity"); }
        RatisEndpoint.parse(peer.getAddress());
    }

    /** 列表按 ID 稳定排序，保留原始 protobuf 的所有身份和端点字段。 */
    private static List<RaftPeer> sorted(List<RaftPeer> peers) { return peers.stream().sorted(Comparator.comparing(peer -> peer.getId().toString())).toList(); }

    /** 配置比较覆盖完整协议字段，不依赖可能仅按 ID 比较的对象 equals。 */
    static boolean same(List<RaftPeer> left, List<RaftPeer> right) {
        var a = sorted(left); var b = sorted(right);
        if (a.size() != b.size()) { return false; }
        for (int i = 0; i < a.size(); i++) { if (!a.get(i).getRaftPeerProto().equals(b.get(i).getRaftPeerProto())) { return false; } } return true;
    }
}
