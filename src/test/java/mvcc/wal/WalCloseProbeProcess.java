package mvcc.wal;

import java.nio.file.Path;

/** 用固定两个虚拟线程载体执行关闭回归，避免大核开发机掩盖 pinning。 */
public final class WalCloseProbeProcess {
    /** 独立子 JVM 探针不允许实例化。 */
    private WalCloseProbeProcess() { }

    /** 复用在途物化的真实关闭断言，处理器数量由父进程显式限制。 */
    public static void main(String[] args) throws Exception {
        var test = new WalCancellationAcceptanceTest(); test.directory = Path.of(args[0]);
        test.closeDoesNotInterruptAnInFlightMaterializer(); System.out.println("low-processor close completed");
    }
}
