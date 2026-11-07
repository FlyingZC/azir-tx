package com.flyingzc.azir.wal.ha;

import com.flyingzc.azir.wal.WalReplicaIdentity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 认证模式标记的发布重试必须持久确认，不能在不确定同步后允许明文降级。 */
class RatisTransportIdentityIoTest {
    @TempDir Path directory;
    /** 已发生的原子移动不能因同步异常而被删除，重试同样执行真实文件及目录同步。 */
    @ParameterizedTest @EnumSource(RatisPolicyIoTest.Failure.class)
    void authenticationMarkerRetriesEveryIoFailureWithoutDowngrade(RatisPolicyIoTest.Failure failure) throws Exception {
        UUID group = UUID.randomUUID(); var owner = new WalReplicaIdentity(group, group, "n0"); var broken = new RatisPolicyIoTest.Fault(failure);
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, broken)); assertTrue(broken.fired);
        Path marker = directory.resolve("AUTHENTICATED"); byte[] previous = Files.exists(marker) ? Files.readAllBytes(marker) : null;
        if (previous != null) { assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false)); }
        var retry = new RatisPolicyIoTest.Fault(null); RatisTransportIdentity.persist(directory, owner, retry);
        assertTrue(retry.operations.contains("FILE_FORCE")); assertTrue(retry.operations.contains("DIRECTORY_FORCE"));
        RatisTransportIdentity.verify(directory, owner, true); assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false));
        if (previous != null) { assertArrayEquals(previous, Files.readAllBytes(marker)); assertFalse(retry.operations.contains("WRITE")); assertFalse(retry.operations.contains("MOVE")); }
    }
    /** 完整标记已存在时再次同步仍可能失败，不能靠 verify 已通过就返回成功。 */
    @Test void existingAuthenticationMarkerCannotSkipAnotherSyncFailure() throws Exception {
        UUID group = UUID.randomUUID(); var owner = new WalReplicaIdentity(group, group, "n0");
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, new RatisPolicyIoTest.Fault(RatisPolicyIoTest.Failure.BEFORE_DIRECTORY_FORCE)));
        assertThrows(IOException.class, () -> RatisTransportIdentity.persist(directory, owner, new RatisPolicyIoTest.Fault(RatisPolicyIoTest.Failure.BEFORE_FILE_FORCE)));
        assertThrows(IOException.class, () -> RatisTransportIdentity.verify(directory, owner, false)); RatisTransportIdentity.persist(directory, owner);
    }
}
