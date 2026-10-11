package mvcc.tso;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 验证兼容批量包装器不能重新引入客户端缓存造成的发布时间倒退。 */
class TimestampPublicationOrderTest {
    /** 两个包装器和直接调用者交错取号，顺序始终来自同一权威序列。 */
    @Test
    void compatibilityWrappersPreserveAuthorityOrder() {
        var authority = new AtomicTimestampOracle(0);
        var first = new BatchingTimestampOracle(authority, 100);
        var second = new BatchingTimestampOracle(authority, 200);
        assertEquals(1, first.nextTimestamp());
        assertEquals(2, second.nextTimestamp());
        assertEquals(3, first.nextTimestamp());
        assertEquals(4, authority.nextTimestamp());
        assertEquals(5, second.nextTimestamp());
    }
}
