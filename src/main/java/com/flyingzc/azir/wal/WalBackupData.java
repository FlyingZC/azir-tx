package com.flyingzc.azir.wal;

import com.flyingzc.azir.storage.Utf8KeyOrder;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/** 后端快照的流式格式，严格键顺序和结束计数防止截断、重复及无界分配。 */
final class WalBackupData {
    private static final int MAGIC = 0x415A4244;
    private static final int ROW = 0x415A4252;
    private static final int END = 0x415A4245;

    /** 格式工具不允许实例化。 */
    private WalBackupData() { }

    /** 将后端版本逐行交给磁盘或摘要器，不要求把整个数据库装入内存。 */
    @FunctionalInterface interface Sink {
        /** 消费一条已验证的最新版本，异常必须终止备份或恢复。 */
        void accept(WalBackupRow row) throws IOException;
    }

    /** 保存确定性头部，备份 ID 不参与数据摘要，允许后端独立重新计算。 */
    record Header(UUID engineId, String shard, WalCheckpoint boundary) {
        /** 验证头部的引擎、路由和提交边界。 */
        Header { new WalRestoreSpec(new UUID(0, 0), engineId, shard, boundary); }
    }

    /** 单个有界行的编码器，结束标记只允许写一次。 */
    static final class Writer {
        private final DataOutputStream output;
        private final Header header;
        private String previous;
        private long rows;
        private boolean finished;

        /** 先写固定头部，随后按后端二进制键序接收数据。 */
        Writer(OutputStream output, Header header) throws IOException {
            this.output = new DataOutputStream(output); this.header = header;
            this.output.writeInt(MAGIC); this.output.writeInt(1);
            this.output.writeLong(header.engineId().getMostSignificantBits()); this.output.writeLong(header.engineId().getLeastSignificantBits());
            this.output.writeUTF(header.shard()); this.output.writeLong(header.boundary().sequence()); this.output.writeUTF(header.boundary().digest());
        }

        /** 检查严格递增键和版本边界，再写入一行完整数据。 */
        void accept(WalBackupRow row) throws IOException {
            if (finished || row.version().commitSequence() > header.boundary().sequence()
                    || previous != null && Utf8KeyOrder.compare(previous, row.key()) >= 0) { throw new IOException("invalid backup row order or boundary"); }
            byte[] value = row.version().value(); output.writeInt(ROW); output.writeUTF(row.key());
            output.writeLong(row.version().commitSequence()); output.writeInt(value == null ? -1 : value.length);
            if (value != null) { output.write(value); }
            previous = row.key(); rows++;
        }

        /** 明确写出最终行数并刷新流，缺少结束标记的文件不是完整备份。 */
        long finish() throws IOException {
            if (finished) { throw new IOException("backup stream already finished"); }
            output.writeInt(END); output.writeLong(rows); output.flush(); finished = true; return rows;
        }
    }

    /** 验证整个流后返回行数，恢复的消费方仍应在最终校验前保持恢复中状态。 */
    static long read(InputStream input, Header expected, Sink sink) throws IOException {
        var data = new DataInputStream(input);
        try {
            if (data.readInt() != MAGIC || data.readInt() != 1) { throw new IOException("unsupported backup data"); }
            var actual = new Header(new UUID(data.readLong(), data.readLong()), data.readUTF(), new WalCheckpoint(data.readLong(), data.readUTF()));
            if (!expected.equals(actual)) { throw new IOException("backup data identity mismatch"); }
            String previous = null; long rows = 0;
            while (true) {
                int kind = data.readInt();
                if (kind == END) {
                    if (data.readLong() != rows || data.read() != -1) { throw new IOException("invalid backup footer"); }
                    return rows;
                }
                if (kind != ROW) { throw new IOException("invalid backup row marker"); }
                String key = data.readUTF(); long sequence = data.readLong(); int size = data.readInt();
                if (size < -1 || size > 8 * 1024 * 1024 || sequence < 1 || sequence > expected.boundary().sequence()
                        || previous != null && Utf8KeyOrder.compare(previous, key) >= 0) { throw new IOException("invalid backup row"); }
                byte[] value = size < 0 ? null : data.readNBytes(size);
                if (value != null && value.length != size) { throw new IOException("truncated backup value"); }
                sink.accept(new WalBackupRow(key, new WalValue(sequence, value))); previous = key; rows++;
            }
        } catch (IllegalArgumentException failure) { throw new IOException("invalid backup data", failure); }
    }
}
