package com.flyingzc.azir.storage.mysql;

import com.flyingzc.azir.common.ErrorCode;
import com.flyingzc.azir.common.MvccException;
import com.flyingzc.azir.storage.VersionedValue;
import com.flyingzc.azir.storage.KeyRange;
import com.flyingzc.azir.storage.ScanPage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** 正式参与者向量点读在查询前后持有进程级 GC 保护句柄。 */
final class MysqlVectorProtectedReader {
    private final Map<String, MysqlStorageAdapter> adapters;
    private final MysqlVectorReaderProtectionRuntime protection;

    /** 路由必须指向同名真实分片，未知路由不能猜测物理数据源。 */
    MysqlVectorProtectedReader(Map<String, MysqlStorageAdapter> adapters,
                               MysqlVectorReaderProtectionRuntime protection) {
        this.adapters = Map.copyOf(adapters);
        this.protection = Objects.requireNonNull(protection, "protection");
        if (this.adapters.isEmpty() || this.adapters.entrySet().stream()
                .anyMatch(entry -> !entry.getKey().equals(entry.getValue().shardId()))) {
            throw new IllegalArgumentException("vector protected reader has invalid shard routes");
        }
    }

    /** RC 每条语句先登记当前发布切面，结束前再次核对租约资格。 */
    Optional<VersionedValue> getReadCommitted(long requesterTxId, String shardId,
                                              String key, int stripe) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            Optional<VersionedValue> value = adapter.getVector(
                    handle.snapshot(), requesterTxId, key, stripe);
            handle.requireActive();
            return value;
        }
    }

    /** RC 每页独立登记发布切面，分页游标保持原始键顺序。 */
    ScanPage scanPageReadCommitted(long requesterTxId, String shardId, KeyRange range,
                                   String afterKeyExclusive, int limit) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, afterKeyExclusive, limit);
            handle.requireActive();
            return page;
        }
    }

    /** 一次 RC 范围语句固定一个切面，跨内部页不会读到后续提交。 */
    List<VersionedValue> scanReadCommitted(long requesterTxId, String shardId, KeyRange range) {
        MysqlStorageAdapter adapter = adapter(shardId);
        try (var handle = protection.beginRead()) {
            List<VersionedValue> values = scanAll(adapter, handle, requesterTxId, range);
            handle.requireActive();
            return values;
        }
    }

    /** 有界页合并时每页核对租约，任何失效都禁止返回部分扫描结果。 */
    private static List<VersionedValue> scanAll(MysqlStorageAdapter adapter,
                                                 MysqlVectorReaderProtectionRuntime.ReadHandle handle,
                                                 long requesterTxId, KeyRange range) {
        List<VersionedValue> values = new ArrayList<>();
        String after = null;
        do {
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, after, 128);
            values.addAll(page.values());
            after = page.nextKey();
            handle.requireActive();
        } while (after != null);
        return List.copyOf(values);
    }

    /** RR 从 begin 起固定一个已登记切面，直至业务事务释放句柄。 */
    RepeatableRead beginRepeatableRead(long requesterTxId) {
        return new RepeatableRead(requesterTxId, protection.beginRead());
    }

    /** 无法解释的逻辑分片不能进入产品版本链读取。 */
    private MysqlStorageAdapter adapter(String shardId) {
        MysqlStorageAdapter adapter = adapters.get(shardId);
        if (adapter == null) {
            throw new MvccException(ErrorCode.ILLEGAL_STATE,
                    "vector protected read has no matching shard", false, null);
        }
        return adapter;
    }

    /** RR 事务的多次点读都使用同一向量，旧版本保留到句柄关闭。 */
    final class RepeatableRead implements AutoCloseable {
        private final long requesterTxId;
        private final MysqlVectorReaderProtectionRuntime.ReadHandle handle;

        /** 只允许由保护运行时成功登记后构造。 */
        private RepeatableRead(long requesterTxId,
                               MysqlVectorReaderProtectionRuntime.ReadHandle handle) {
            this.requesterTxId = requesterTxId;
            this.handle = handle;
        }

        /** 单键读取前后都核对同一进程租约，过期不能返回旧值。 */
        Optional<VersionedValue> get(String shardId, String key, int stripe) {
            MysqlStorageAdapter adapter = adapter(shardId);
            Optional<VersionedValue> value = adapter.getVector(
                    handle.snapshot(), requesterTxId, key, stripe);
            handle.requireActive();
            return value;
        }

        /** RR 分页始终沿用 begin 时登记的全局向量。 */
        ScanPage scanPage(String shardId, KeyRange range, String afterKeyExclusive, int limit) {
            MysqlStorageAdapter adapter = adapter(shardId);
            ScanPage page = adapter.scanVectorPage(handle.snapshot(), requesterTxId,
                    range, afterKeyExclusive, limit);
            handle.requireActive();
            return page;
        }

        /** RR 整个范围语句在同一已登记切面内逐页读取。 */
        List<VersionedValue> scan(String shardId, KeyRange range) {
            MysqlStorageAdapter adapter = adapter(shardId);
            List<VersionedValue> values = scanAll(adapter, handle, requesterTxId, range);
            handle.requireActive();
            return values;
        }

        /** 事务结束后交还本地最老读切面保护。 */
        @Override
        public void close() {
            handle.close();
        }
    }
}
