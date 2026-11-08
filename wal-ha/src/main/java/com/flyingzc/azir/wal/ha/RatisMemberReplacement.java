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

import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;

import java.nio.ByteBuffer;

import java.util.zip.CRC32C;

/**
 * But durable saves and cross-leader continues the member replacement instrument, the new Peer ID is derived from operating UUID instead of reusing old identity.
 *
 * @param operationId operation id
 * @param groupId group id
 * @param original original
 * @param removed removed
 * @param replacement replacement
 */
public record RatisMemberReplacement(UUID operationId, UUID groupId, RatisMembershipView original, RaftPeerId removed, RaftPeer replacement) {

    /**
     * The original member, address and new identity cannot be misused by another configuration once it is planned to be fixed.
     *
     * @param operationId operation id
     * @param groupId group id
     * @param original original
     * @param removed removed
     * @param replacement replacement
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisMemberReplacement {
        Objects.requireNonNull(operationId);
        Objects.requireNonNull(groupId);
        Objects.requireNonNull(original);
        Objects.requireNonNull(removed);
        Objects.requireNonNull(replacement);
        RatisMembershipView.validate(replacement);
        if (!replacement.getId().toString().equals("r-" + operationId) || replacement.getStartupRole() != RaftPeerRole.LISTENER
                || original.voters().stream().noneMatch(peer -> peer.getId().equals(removed)) || original.learners().size() >= 6
                || java.util.stream.Stream.concat(original.voters().stream(), original.learners().stream())
                        .anyMatch(peer -> peer.getId().equals(replacement.getId()) || peer.getAddress().equals(replacement.getAddress()))) {
            throw new IllegalArgumentException("invalid replacement identity or original roster");
        }
    }

    /**
     * The new node is activated with the non-voting role of this instrument and cannot directly reformat old IDs that lost their voting status.
     *
     * @return the operation result
     */
    public RaftGroup joiningGroup() {
        var peers = new ArrayList<>(original.voters());
        peers.addAll(original.learners());
        peers.add(replacement);
        return RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
    }

    /**
     * Waiting for the phase to remain the same as the three voting members.
     *
     * @param index index
     * @return the operation result
     */
    RatisMembershipView staged(final long index) {
        var peers = new ArrayList<>(original.learners());
        peers.add(replacement);
        return new RatisMembershipView(index, original.voters(), peers);
    }

    /**
     * The final configuration replaces one vote at the same time and maintains three voting members.
     *
     * @param index index
     * @return the operation result
     */
    RatisMembershipView completed(final long index) {
        var peers = new ArrayList<>(original.voters().stream().filter(peer -> !peer.getId().equals(removed)).toList());
        peers.add(RaftPeer.newBuilder(replacement).setStartupRole(RaftPeerRole.FOLLOWER).build());
        return new RatisMembershipView(index, peers, original.learners());
    }

    /**
     * The classification recognizes only the original, intermediate and completed three precise aggregations, with other changes having to be reprogrammed by the operator.
     *
     * @param current current
     * @return the operation result
     * @throws IllegalStateException if the operation cannot complete
     */
    int phase(final RatisMembershipView current) {
        if (current.index() > original.index() && same(current, completed(current.index()))) {
            return 2;
        }
        if (current.index() > original.index() && same(current, staged(current.index()))) {
            return 1;
        }
        if (same(current, original) && current.index() >= original.index()) {
            return 0;
        }
        throw new IllegalStateException("membership changed outside this replacement plan");
    }

    /**
     * Full protocol field comparison, including node roles and addresses.
     *
     * @param a a
     * @param b b
     * @return the operation result
     */
    private static boolean same(final RatisMembershipView a, final RatisMembershipView b) {
        return RatisMembershipView.same(a.voters(), b.voters()) && RatisMembershipView.same(a.learners(), b.learners());
    }

    /**
     * Fixed coding and CRC are used by the operator to save a document, which does not contain backend receipt or business data.
     *
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public byte[] encode() throws IOException {
        var buffer = new ByteArrayOutputStream();
        var out = new DataOutputStream(buffer);
        out.writeInt(0x415A4D52);
        out.writeInt(1);
        uuid(out, operationId);
        uuid(out, groupId);
        out.writeLong(original.index());
        peers(out, original.voters());
        peers(out, original.learners());
        out.writeUTF(removed.toString());
        peers(out, List.of(replacement));
        out.flush();
        if (buffer.size() > 16_380) {
            throw new IOException("membership ticket exceeds budget");
        }
        out.writeInt(crc(buffer.toByteArray()));
        out.flush();
        return buffer.toByteArray();
    }

    /**
     * The length, count and summary are preceded by verify, so that the damaged instrument cannot be degraded to new operations or different nodes.
     *
     * @param bytes bytes
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    public static RatisMemberReplacement decode(final byte[] bytes) throws IOException {
        if (bytes.length < 100 || bytes.length > 16_384 || ByteBuffer.wrap(bytes).getInt(bytes.length - 4) != crc(Arrays.copyOf(bytes, bytes.length - 4))) {
            throw new IOException("membership ticket checksum mismatch");
        }
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
            if (in.readInt() != 0x415A4D52 || in.readInt() != 1) {
                throw new IOException("unsupported membership ticket");
            }
            UUID operation = uuid(in), group = uuid(in);
            long index = in.readLong();
            var voters = peers(in);
            var learners = peers(in);
            var removed = RaftPeerId.valueOf(in.readUTF());
            var fresh = peers(in);
            if (fresh.size() != 1 || in.available() != 0) {
                throw new IOException("invalid membership ticket catalog");
            }
            return new RatisMemberReplacement(operation, group, new RatisMembershipView(index, voters, learners), removed, fresh.getFirst());
        } catch (final IllegalArgumentException failure) {
            throw new IOException("invalid membership ticket", failure);
        }
    }

    /**
     * Protobuf keeps all end-point fields, but one KiB code budget remains for individual members.
     *
     * @param out out
     * @param peers peers
     * @throws IOException if the operation cannot complete
     */
    private static void peers(final DataOutputStream out, final List<RaftPeer> peers) throws IOException {
        out.writeInt(peers.size());
        for (var peer : peers) {
            byte[] bytes = peer.getRaftPeerProto().toByteArray();
            if (bytes.length > 1024) {
                throw new IOException("member descriptor exceeds budget");
            }
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    /**
     * Limit membership and field size before distribution. Object cannot be created on an infinity basis by long-range counting.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static List<RaftPeer> peers(final DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > 9) {
            throw new IOException("membership ticket count exceeds budget");
        }
        var result = new ArrayList<RaftPeer>();
        for (int i = 0; i < count; i++) {
            int length = in.readInt();
            if (length < 1 || length > 1024 || length > in.available()) {
                throw new IOException("member descriptor length invalid");
            }
            result.add(org.apache.ratis.util.ProtoUtils.toRaftPeer(RaftPeerProto.parseFrom(in.readNBytes(length))));
        }
        return result;
    }

    /**
     * Fixed UUID encoding.
     *
     * @param out out
     * @param value value to write
     * @throws IOException if the operation cannot complete
     */
    private static void uuid(final DataOutputStream out, final UUID value) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    /**
     * Fixed UUID decoding.
     *
     * @param in in
     * @return the operation result
     * @throws IOException if the operation cannot complete
     */
    private static UUID uuid(final DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    /**
     * The whole operating instrument is common validate.
     *
     * @param bytes bytes
     * @return the operation result
     */
    private static int crc(final byte[] bytes) {
        var crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
