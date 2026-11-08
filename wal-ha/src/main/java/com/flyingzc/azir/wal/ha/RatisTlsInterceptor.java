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

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.Message;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * When a handshake is certified, binds the group, receiver and sender, original remote management and operations are written in unison.
 */
final class RatisTlsInterceptor implements ServerInterceptor {

    private final RatisTlsSecurity security;

    private final Predicate<String> activeMember;

    /**
     * Membership takes the actual configuration, and old members cannot continue sending protocols with TLS connection, which has not expired.
     *
     * @param security security
     * @param activeMember active member
     */
    RatisTlsInterceptor(final RatisTlsSecurity security, final Predicate<String> activeMember) {
        this.security = security;
        this.activeMember = activeMember;
    }

    /**
     * All services use the same interceptor, and missing two-way TLS will refuse before requesting resolution.
     *
     * @param <Q> value type
     * @param <S> value type
     * @param call call
     * @param headers headers
     * @param next next
     * @return the operation result
     * @throws java.security.cert.CertificateException if the operation cannot complete
     */
    @Override
    public <Q, S> ServerCall.Listener<Q> interceptCall(final ServerCall<Q, S> call, final Metadata headers, final ServerCallHandler<Q, S> next) {
        final X509Certificate certificate;
        try {
            if (!security.localIdentityValid()) {
                throw new java.security.cert.CertificateException("local TLS identity expired");
            }
            var session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            if (session == null || !(session.getPeerCertificates()[0] instanceof X509Certificate leaf)) {
                throw new java.security.cert.CertificateException("TLS peer missing");
            }
            certificate = leaf;
            security.authenticate(certificate);
        } catch (final Exception failure) {
            call.close(Status.UNAUTHENTICATED.withDescription("authenticated WAL peer required"), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        String service = call.getMethodDescriptor().getServiceName();
        if (service == null || !(service.endsWith(".RaftServerProtocolService") || service.endsWith(".RaftClientProtocolService"))) {
            call.close(Status.PERMISSION_DENIED.withDescription("use local WAL management API"), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {

            private boolean denied;
            /**
             * The flow log with snapshot rechecks eligibility and fingerprints for every message, not just the first.
             *
             * @param message message
             * @throws java.security.cert.CertificateException if the operation cannot complete
             */
            @Override
            public void onMessage(final Q message) {
                try {
                    if (!security.localIdentityValid()) {
                        throw new java.security.cert.CertificateException("local TLS identity expired");
                    }
                    validate(message, security.authenticate(certificate));
                } catch (final Exception failure) {
                    denied = true;
                    call.close(Status.PERMISSION_DENIED.withDescription("WAL RPC identity or operation rejected"), new Metadata());
                    return;
                }
                if (!denied) {
                    super.onMessage(message);
                }
            }

            /**
             * Half-closure notifications cannot be handed over to a service that may perform management actions after refusal.
             */
            @Override
            public void onHalfClose() {
                if (!denied) {
                    super.onHalfClose();
                }
            }
        };
    }

    /**
     * The identity that the protocol message carries must match the certified leaf certificate.
     *
     * @param message message
     * @param peer peer
     * @throws java.security.cert.CertificateException if the operation cannot complete
     */
    private void validate(final Object message, final String peer) throws java.security.cert.CertificateException {
        if (!activeMember.test(peer)) {
            throw new java.security.cert.CertificateException("peer outside current membership");
        }
        RaftProtos.RaftRpcRequestProto rpc = null;
        boolean server = true;
        if (message instanceof RaftProtos.RaftClientRequestProto client) {
            server = false;
            rpc = client.getRpcRequest();
            if (!client.hasStaleRead() || !client.hasMessage() || !Arrays.equals(client.getMessage().getContent().toByteArray(), RatisReplicaStatus.request())) {
                throw new java.security.cert.CertificateException("remote business request prohibited");
            }
        } else if (message instanceof Message proto) {
            for (var value : proto.getAllFields().values()) {
                if (value instanceof RaftProtos.RaftRpcRequestProto request) {
                    if (rpc != null) {
                        throw new java.security.cert.CertificateException("ambiguous RPC identity");
                    }
                    rpc = request;
                }
            }
        }
        if (rpc == null || !rpc.hasRaftGroupId() || !RaftGroupId.valueOf(rpc.getRaftGroupId().getId()).getUuid().equals(security.group())
                || !rpc.getReplyId().toStringUtf8().equals(security.node()) || server && !rpc.getRequestorId().toStringUtf8().equals(peer)) {
            throw new java.security.cert.CertificateException("RPC identity mismatch");
        }
    }
}
