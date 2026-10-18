package mvcc.backend;

import mvcc.common.IsolationLevel;

/** 独立源码事务后端 SPI；实现持有自己的决议权威与存储空间。 */
public interface TransactionBackend extends AutoCloseable {
    /** 返回后端实现名称。 */
    String implementation();
    /** 返回不可变数据空间身份。 */
    String namespace();
    /** 创建指定全局隔离级别的业务事务。 */
    BackendTransaction begin(IsolationLevel isolation);
    /** 按原身份查询持久状态，缺失不代表已中止。 */
    BackendDecision lookup(BackendId id);
    /** 有界恢复持久终态或过期事务，不执行未经证明的物理版本 GC。 */
    int recover(int limit);
    /** 执行一轮恢复及安全版本清理；不支持 GC 的扩展实现只恢复。 */
    default mvcc.maintenance.MaintenanceResult maintain(int limit) {
        return new mvcc.maintenance.MaintenanceResult(recover(limit), 0, 0);
    }
    /** 查询持久快照保护和维护积压；未提供诊断的扩展实现必须明确拒绝。 */
    default mvcc.maintenance.MaintenanceDiagnostics diagnostics() {
        throw new UnsupportedOperationException("maintenance diagnostics unavailable");
    }
    /** 返回显式生产管理能力，不支持的后端不能静默降级。 */
    default mvcc.operations.MysqlProtocolAdmin administration() { throw new UnsupportedOperationException("administration unavailable"); }
    /** 释放后端自身的资源，不关闭调用方借出的连接池。 */
    @Override
    void close();
}
