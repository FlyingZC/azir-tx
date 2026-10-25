package mvcc.wal.ha;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** 专属进程入口，不修改原有 Azir 装配，也不把健康端口当成事务写入端口。 */
public final class RatisNodeMain {
    /** 入口类不创建实例。 */
    private RatisNodeMain() { }

    /** 前台运行供进程管理器托管；启动失败必须退出所属 JVM，不能在原目录原地重开。 */
    public static void main(String[] args) {
        if (args.length != 2 || !args[0].equals("--config")) {
            System.err.println("usage: azir-wal-node --config /absolute/path/node.properties");
            System.exit(2);
            return;
        }
        try {
            var configuration = RatisNodeConfiguration.load(Path.of(args[1]));
            var host = new RatisNodeHost(configuration);
            Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("wal-node-shutdown").unstarted(() -> close(host)));
            host.start();
            System.out.println("WAL node started; use local /ready and /writable probes for cached status");
            new CountDownLatch(1).await();
        } catch (Exception | Error failure) {
            // 连接器和插件异常可能携带密码或业务参数，不打印消息、堆栈或原因链。
            String phase = failure instanceof RatisNodeConfiguration.ConfigurationException invalid ? "; phase=" + invalid.phase() : "";
            System.err.println("WAL node startup/run failed; type=" + failure.getClass().getSimpleName() + phase);
            // 只交由关闭钩子清理，避免当前线程失败清理后重复等待并占用两份关闭预算。
            System.exit(1);
        }
    }

    /** 关闭失败明确报错，由进程管理器的停止期限处理，不中断在途 WAL 来伪装成功。 */
    private static void close(RatisNodeHost host) {
        try { host.close(); }
        catch (Exception | Error failure) {
            System.err.println("WAL node cleanup incomplete; terminate process before restart; type=" + failure.getClass().getSimpleName());
        }
    }
}
