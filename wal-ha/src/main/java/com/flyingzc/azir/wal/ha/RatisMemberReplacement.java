package com.flyingzc.azir.wal.ha;

import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32C;

/** 可持久保存和跨领导者续做的成员替换票据，新 Peer ID 从操作 UUID 派生而非复用旧身份。 */
public record RatisMemberReplacement(UUID operationId, UUID groupId, RatisMembershipView original, RaftPeerId removed, RaftPeer replacement) {
    /** 原始成员、地址和新身份一经规划固定，不能让另一配置误用同一操作。 */
    public RatisMemberReplacement {
        Objects.requireNonNull(operationId); Objects.requireNonNull(groupId); Objects.requireNonNull(original); Objects.requireNonNull(removed); Objects.requireNonNull(replacement);
        RatisMembershipView.validate(replacement);
        if (!replacement.getId().toString().equals("r-" + operationId) || replacement.getStartupRole() != RaftPeerRole.LISTENER
                || original.voters().stream().noneMatch(peer -> peer.getId().equals(removed)) || original.learners().size() >= 6
                || java.util.stream.Stream.concat(original.voters().stream(), original.learners().stream()).anyMatch(peer -> peer.getId().equals(replacement.getId()) || peer.getAddress().equals(replacement.getAddress()))) {
            throw new IllegalArgumentException("invalid replacement identity or original roster");
        }
    }

    /** 新节点用本票据的非投票角色启动，不能把丢失投票状态的旧 ID 直接重新格式化。 */
    public RaftGroup joiningGroup() {
        var peers = new ArrayList<>(original.voters()); peers.addAll(original.learners()); peers.add(replacement); return RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
    }

    /** 等待追平阶段保持原来的三个投票成员。 */
    RatisMembershipView staged(long index) { var peers = new ArrayList<>(original.learners()); peers.add(replacement); return new RatisMembershipView(index, original.voters(), peers); }

    /** 最终配置同时替换一票，仍然维持三个投票成员。 */
    RatisMembershipView completed(long index) {
        var peers = new ArrayList<>(original.voters().stream().filter(peer -> !peer.getId().equals(removed)).toList());
        peers.add(RaftPeer.newBuilder(replacement).setStartupRole(RaftPeerRole.FOLLOWER).build()); return new RatisMembershipView(index, peers, original.learners());
    }

    /** 分类只认原始、中间和完成三种精确集合，其他变更必须由操作者重新规划。 */
    int phase(RatisMembershipView current) {
        if (current.index() > original.index() && same(current, completed(current.index()))) { return 2; }
        if (current.index() > original.index() && same(current, staged(current.index()))) { return 1; }
        if (same(current, original) && current.index() >= original.index()) { return 0; }
        throw new IllegalStateException("membership changed outside this replacement plan");
    }

    /** 完整协议字段比较，包括节点角色及地址。 */
    private static boolean same(RatisMembershipView a, RatisMembershipView b) { return RatisMembershipView.same(a.voters(), b.voters()) && RatisMembershipView.same(a.learners(), b.learners()); }

    /** 固定编码和 CRC 用于操作者保存票据，不包含后端凭据或业务数据。 */
    public byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream(); var out = new DataOutputStream(buffer); out.writeInt(0x415A4D52); out.writeInt(1); uuid(out, operationId); uuid(out, groupId);
        out.writeLong(original.index()); peers(out, original.voters()); peers(out, original.learners()); out.writeUTF(removed.toString()); peers(out, List.of(replacement)); out.flush();
        if (buffer.size() > 16_380) { throw new IOException("membership ticket exceeds budget"); } out.writeInt(crc(buffer.toByteArray())); out.flush(); return buffer.toByteArray();
    }

    /** 长度、计数和摘要先验证，损坏票据不能退化为新操作或不同节点。 */
    public static RatisMemberReplacement decode(byte[] bytes) throws IOException {
        if (bytes.length < 100 || bytes.length > 16_384 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) { throw new IOException("membership ticket checksum mismatch"); }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4)); if (in.readInt() != 0x415A4D52 || in.readInt() != 1) { throw new IOException("unsupported membership ticket"); }
            UUID operation = uuid(in), group = uuid(in); long index = in.readLong(); var voters = peers(in); var learners = peers(in); var removed = RaftPeerId.valueOf(in.readUTF()); var fresh = peers(in);
            if (fresh.size() != 1 || in.available() != 0) { throw new IOException("invalid membership ticket catalog"); }
            return new RatisMemberReplacement(operation, group, new RatisMembershipView(index, voters, learners), removed, fresh.getFirst());
        } catch (IllegalArgumentException failure) { throw new IOException("invalid membership ticket", failure); }
    }

    /** protobuf 保留所有端点字段，但单个成员仍有一 KiB 编码预算。 */
    private static void peers(DataOutputStream out, List<RaftPeer> peers) throws IOException {
        out.writeInt(peers.size()); for (var peer : peers) { byte[] bytes = peer.getRaftPeerProto().toByteArray(); if (bytes.length > 1024) { throw new IOException("member descriptor exceeds budget"); } out.writeInt(bytes.length); out.write(bytes); }
    }

    /** 分配前限制成员及字段大小，不能依据远端计数无限创建对象。 */
    private static List<RaftPeer> peers(DataInputStream in) throws IOException {
        int count = in.readInt(); if (count < 0 || count > 9) { throw new IOException("membership ticket count exceeds budget"); } var result = new ArrayList<RaftPeer>();
        for (int i = 0; i < count; i++) { int length = in.readInt(); if (length < 1 || length > 1024 || length > in.available()) { throw new IOException("member descriptor length invalid"); } result.add(org.apache.ratis.util.ProtoUtils.toRaftPeer(RaftPeerProto.parseFrom(in.readNBytes(length)))); }
        return result;
    }

    /** 固定 UUID 编码。 */
    private static void uuid(DataOutputStream out, UUID value) throws IOException { out.writeLong(value.getMostSignificantBits()); out.writeLong(value.getLeastSignificantBits()); }

    /** 固定 UUID 解码。 */
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(), in.readLong()); }

    /** 整份操作票据共同校验。 */
    private static int crc(byte[] bytes) { var crc = new CRC32C(); crc.update(bytes, 0, bytes.length); return (int) crc.getValue(); }
}
