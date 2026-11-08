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

package com.flyingzc.azir.storage.mysql;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Backring binary cut-side points write the length and text in a single time to avoid HTTP headers being separated from the body.
 */
final class MysqlVectorCutTcpServer implements AutoCloseable {

    private final MysqlVectorCutLeaderRuntime runtime;

    private final ServerSocket listener;

    private final ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();

    private final AtomicBoolean running = new AtomicBoolean();

    private volatile Thread acceptor;

    /**
     * Only binds the CDA back to the ring address, and the listening port must be the same as the central lease ownerId.
     *
     * @param runtime runtime
     * @param port port
     * @throws IOException if the operation cannot complete
     * @throws IllegalArgumentException if the operation cannot complete
     */
    MysqlVectorCutTcpServer(final MysqlVectorCutLeaderRuntime runtime, final int port) throws IOException {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("invalid vector cut TCP port");
        }
        listener = new ServerSocket();
        listener.setReuseAddress(true);
        listener.bind(new InetSocketAddress("127.0.0.1", port), 32);
    }

    /**
     * Get the actual loop port assigned by the system.
     *
     * @return the operation result
     */
    int port() {
        return listener.getLocalPort();
    }

    /**
     * Only when a valid lease has been made available at the time of release will remote requests start to be received.
     *
     * @throws IllegalStateException if the operation cannot complete
     */
    void start() {
        if (!runtime.isServing() || !running.compareAndSet(false, true)) {
            throw new IllegalStateException("vector cut TCP endpoint requires an active leader");
        }
        acceptor = Thread.ofVirtual().name("azir-vector-cut-tcp-accept").start(this::acceptLoop);
    }

    /**
     * Every time connection is reused, verify leaders are re-qualified to avoid HF RC reading out temporary ports.
     */
    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = listener.accept();
                requests.submit(() -> serveOne(socket));
            } catch (final SocketException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            } catch (final IOException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            } catch (final RuntimeException ex) {
                if (running.get()) {
                    runtime.endpointUnavailable(ex);
                    running.set(false);
                }
            }
        }
    }

    /**
     * Each request code returns a symbol length and full side, while the loss of lease goes back to error and closes connection.
     *
     * @param socket socket
     */
    private void serveOne(final Socket socket) {
        try (
                socket;
                var input = new DataInputStream(socket.getInputStream());
                var output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(2_000);
            while (running.get()) {
                int command;
                try {
                    command = input.readUnsignedByte();
                } catch (final EOFException ex) {
                    break;
                }
                if (command != 1) {
                    output.writeInt(-2);
                    output.flush();
                    break;
                }
                try {
                    byte[] body = MysqlVectorCutWire.encode(runtime.currentView());
                    output.writeInt(body.length);
                    output.write(body);
                    output.flush();
                } catch (final RuntimeException ex) {
                    output.writeInt(-1);
                    output.flush();
                    break;
                }
            }
        } catch (final IOException ignored) {
            // This read-only request is only affected by the end-to-end break, without changing the leader 's lease or release status.
        }
    }

    /**
     * Stop listening and requesting collection threads, while the old leader is still rejected by run-time lease fence.
     *
     * @throws Exception if the operation cannot complete
     */
    @Override
    public void close() throws Exception {
        runtime.close();
        running.set(false);
        listener.close();
        Thread activeAcceptor = acceptor;
        if (activeAcceptor != null) {
            activeAcceptor.interrupt();
            activeAcceptor.join(5_000);
        }
        requests.close();
    }
}
