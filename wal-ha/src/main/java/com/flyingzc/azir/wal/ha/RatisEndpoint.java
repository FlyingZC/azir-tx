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

import java.net.URI;

/**
 * No DNS parsing validate at fixed node endpoint, and the authentication policy matches the standard host and port.
 *
 * @param host host
 * @param port port
 */
record RatisEndpoint(String host, int port) {

    /**
     * Members are not accepted as members ' addresses with binding, user information, path or hidden port.
     *
     * @param address address
     * @return the operation result
     * @throws IllegalArgumentException if the operation cannot complete
     */
    static RatisEndpoint parse(final String address) {
        try {
            var uri = new URI("tcp://" + address);
            String host = uri.getHost();
            int port = uri.getPort();
            if (host == null || host.length() > 253 || port < 1 || port > 65535 || uri.getRawUserInfo() != null
                    || !uri.getRawPath().isEmpty() || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IllegalArgumentException("invalid member endpoint");
            }
            host = host.replace("[", "").replace("]", "").toLowerCase(java.util.Locale.ROOT);
            if (host.equals("0.0.0.0") || host.equals("::") || host.contains("%")) {
                throw new IllegalArgumentException("wildcard or scoped member endpoint unsupported");
            }
            return new RatisEndpoint(host, port);
        } catch (final java.net.URISyntaxException invalid) {
            throw new IllegalArgumentException("invalid member endpoint", invalid);
        }
    }

    /**
     * The TLS engine provides the host and port, IPv6 is repackaged in square brackets using the same rigorous resolution.
     *
     * @param host host
     * @param port port
     * @return the operation result
     */
    static RatisEndpoint of(final String host, final int port) {
        return parse((host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host) + ":" + port);
    }
}
