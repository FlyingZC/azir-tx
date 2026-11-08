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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Commit and a stable view of the membership, with three voting members maintained at all times, and additional members could only vote as non-voting replica.
 *
 * @param index index
 * @param voters voters
 * @param learners learners
 */
public record RatisMembershipView(long index, List<RaftPeer> voters, List<RaftPeer> learners) {

    /**
     * Fixed sorting and complete endpoints cannot hide address or role changes with the same ID.
     *
     * @param index index
     * @param voters voters
     * @param learners learners
     * @throws IllegalArgumentException if the operation cannot complete
     */
    public RatisMembershipView {
        voters = sorted(voters);
        learners = sorted(learners);
        if (index < -1 || voters.size() != 3 || learners.size() > 6) {
            throw new IllegalArgumentException("membership requires three voters and bounded learners");
        }
        if (voters.stream().anyMatch(peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                || learners.stream().anyMatch(peer -> peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)) {
            throw new IllegalArgumentException("membership roles mismatch");
        }
        var ids = new HashSet<String>();
        var addresses = new HashSet<String>();
        for (var peer : java.util.stream.Stream.concat(voters.stream(), learners.stream()).toList()) {
            validate(peer);
            if (!ids.add(peer.getId().toString()) || !addresses.add(peer.getAddress())) {
                throw new IllegalArgumentException("duplicate membership identity or address");
            }
        }
    }

    /**
     * Members describe their support for the official host name and IP, with a separate limit on access and authentication of entry to check all endpoint authorizations.
     *
     * @param peer peer
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static void validate(final RaftPeer peer) {
        if (!peer.getId().toString().matches("[A-Za-z0-9._-]{1,64}") || peer.getAddress() == null) {
            throw new IllegalArgumentException("invalid member identity");
        }
        RatisEndpoint.parse(peer.getAddress());
    }

    /**
     * Lists are sorted by ID to stabilize, and all original protobuf identity and endpoint fields are kept.
     *
     * @param peers peers
     * @return the operation result
     */
    private static List<RaftPeer> sorted(final List<RaftPeer> peers) {
        return peers.stream().sorted(Comparator.comparing(peer -> peer.getId().toString())).toList();
    }

    /**
     * Configures to overwhelm the full protocol field without relying on objects that may be compared only by ID.
     *
     * @param left left
     * @param right right
     * @return the operation result
     */
    static boolean same(final List<RaftPeer> left, final List<RaftPeer> right) {
        var a = sorted(left);
        var b = sorted(right);
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).getRaftPeerProto().equals(b.get(i).getRaftPeerProto())) {
                return false;
            }
        }
        return true;
    }
}
