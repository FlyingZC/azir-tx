package mvcc.wal;

import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/** 新镜像必须阻止旧程序缺失冷冲突栅栏后继续运行，同时保留旧镜像升级读取能力。 */
class WalCheckpointFormatTest {
    /** 编码版本升到二，版本一仅作为旧镜像读取，重新编码后必须写出新版本。 */
    @Test void newImagesRejectLegacyReaderButLegacyImagesRemainReadable() throws Exception {
        var state = image(); byte[] current = state.encode();
        assertEquals(2, ByteBuffer.wrap(current).getInt(4));
        byte[] legacy = current.clone(); ByteBuffer.wrap(legacy).putInt(4, 1);
        var restored = WalCheckpointState.decode(legacy);
        assertEquals(state.engineId(), restored.engineId()); assertEquals(state.versions(), restored.versions());
        assertEquals(2, ByteBuffer.wrap(restored.encode()).getInt(4));
        // 上一版解码器要求格式一，不能把新镜像误当成无冲突历史的空引擎。
        assertNotEquals(1, ByteBuffer.wrap(current).getInt(4));
    }

    /** 未知格式不得猜测兼容，不能在未来镜像上丢失恢复字段后继续运行。 */
    @Test void unknownImageVersionsFailClosed() throws Exception {
        for (int format : new int[]{0, 3, Integer.MAX_VALUE}) {
            byte[] bytes = image().encode(); ByteBuffer.wrap(bytes).putInt(4, format);
            assertThrows(IOException.class, () -> WalCheckpointState.decode(bytes));
        }
    }

    /** 构造带已物化旧头的最小镜像，验证旧字段仍被完整解析而非静默丢弃。 */
    private static WalCheckpointState image() {
        var receipt = new WalReceipt(UUID.randomUUID(), 1, "0".repeat(64));
        return new WalCheckpointState(UUID.randomUUID(), Set.of("a"), Set.of("a"), 1, 0, receipt,
                List.of(new WalCheckpointState.Version(1, new WalMutation("a", "k", null))), List.of(), new byte[128], 0);
    }
}
