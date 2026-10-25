package mvcc.wal.ha;

/** 认证传输的连接、调用和生命周期预算，时间单位为毫秒。 */
public record RatisTransportLimits(int connections, int calls, int streamsPerConnection, int metadataBytes,
                                   int listenBacklog, long handshakeMillis, long idleMillis, long connectionAgeMillis) {
    /** 所有外部连接预算必须有限，不能用零表示无限。 */
    public RatisTransportLimits {
        if (connections < 1 || connections > 4096 || calls < 1 || calls > 4096 || streamsPerConnection < 1 || streamsPerConnection > 1024
                || metadataBytes < 1024 || metadataBytes > 65536 || listenBacklog < 16 || listenBacklog > 4096
                || handshakeMillis < 100 || handshakeMillis > 30_000 || idleMillis < 1000 || idleMillis > 3_600_000
                || connectionAgeMillis < 1000 || connectionAgeMillis > 86_400_000) { throw new IllegalArgumentException("invalid authenticated transport limits"); }
    }
    /** 默认最多六十四连接、三十二调用，每连接八个流，握手最多三秒。 */
    public static RatisTransportLimits defaults() { return new RatisTransportLimits(64, 32, 8, 8192, 128, 3000, 60_000, 900_000); }
}
