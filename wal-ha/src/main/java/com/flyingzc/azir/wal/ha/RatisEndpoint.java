package com.flyingzc.azir.wal.ha;

import java.net.URI;

/** 固定节点端点的无 DNS 解析校验，认证策略按规范主机和端口匹配。 */
record RatisEndpoint(String host, int port) {
    /** 不接受通配绑定、用户信息、路径或隐式端口作为成员地址。 */
    static RatisEndpoint parse(String address) {
        try {
            var uri = new URI("tcp://" + address); String host = uri.getHost(); int port = uri.getPort();
            if (host == null || host.length() > 253 || port < 1 || port > 65535 || uri.getRawUserInfo() != null
                    || !uri.getRawPath().isEmpty() || uri.getRawQuery() != null || uri.getRawFragment() != null) { throw new IllegalArgumentException("invalid member endpoint"); }
            host = host.replace("[", "").replace("]", "").toLowerCase(java.util.Locale.ROOT);
            if (host.equals("0.0.0.0") || host.equals("::") || host.contains("%")) { throw new IllegalArgumentException("wildcard or scoped member endpoint unsupported"); }
            return new RatisEndpoint(host, port);
        } catch (java.net.URISyntaxException invalid) { throw new IllegalArgumentException("invalid member endpoint", invalid); }
    }
    /** TLS 引擎提供主机与端口，IPv6 再包方括号后使用相同严格解析。 */
    static RatisEndpoint of(String host, int port) { return parse((host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host) + ":" + port); }
}
