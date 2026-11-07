package com.flyingzc.azir.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** 普通回归必须覆盖小处理器预算，不能依靠开发机的大量载体线程通过关闭测试。 */
class WalLowProcessorCloseTest {
    @TempDir Path directory;

    /** 在子 JVM 固定两个处理器，关闭等待可卸载载体且不能中断物化。 */
    @Test void virtualCloseProgressesWithTwoCarriers() throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx128m", "-XX:ActiveProcessorCount=2", "-cp", classpath, WalCloseProbeProcess.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(directory.resolve("child.log").toFile()).start();
        try {
            assertTrue(child.waitFor(45, TimeUnit.SECONDS), "owned low-processor child did not finish");
            assertEquals(0, child.exitValue(), Files.readString(directory.resolve("child.log")));
            assertTrue(Files.readString(directory.resolve("child.log")).contains("low-processor close completed"));
        } finally { if (child.isAlive()) { child.destroyForcibly(); assertTrue(child.waitFor(30, TimeUnit.SECONDS)); } }
    }
}
