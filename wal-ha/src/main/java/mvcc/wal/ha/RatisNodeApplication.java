package mvcc.wal.ha;

/** 可选的可信业务装配 SPI，只由配置明确指定类名，不扫描或自动执行类路径插件。 */
public interface RatisNodeApplication extends AutoCloseable {
    /** 构造器须无副作用；启动接收托管事务 API，业务不能绕过框架直接读写物化表。 */
    void start(RatisManagedNode node) throws Exception;

    /** 先停止接单再有界排空，支持部分启动失败后的清理和重复关闭；失败时宿主保留节点。 */
    @Override void close() throws Exception;
}
