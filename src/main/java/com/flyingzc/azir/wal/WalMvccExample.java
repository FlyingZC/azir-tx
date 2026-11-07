package com.flyingzc.azir.wal;

import com.flyingzc.azir.common.IsolationLevel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/** 可直接运行的新模式示例，演示本地持久提交和跨片转账。 */
public final class WalMvccExample {
    /** 示例参数格式；事务期限使用毫秒，省略时沿用默认值。 */
    private static final String USAGE = "用法：WalMvccExample [WAL目录] [--timeout-millis 毫秒数]";

    /** 禁止实例化示例入口。 */
    private WalMvccExample() { }

    /** 使用可选 WAL 目录与事务期限启动示例，未指定目录时创建隔离临时目录。 */
    public static void main(String[] args) throws Exception {
        Path directory = null;
        WalOptions options = WalOptions.defaults();
        for (int index = 0; index < args.length; index++) {
            switch (args[index]) {
                case "--timeout-millis" -> {
                    if (++index >= args.length) {
                        throw new IllegalArgumentException("--timeout-millis 缺少毫秒数。" + USAGE);
                    }
                    try {
                        options = options.withTimeout(Long.parseLong(args[index]));
                    } catch (IllegalArgumentException failure) {
                        throw new IllegalArgumentException("事务期限必须为 1..86400000 的整数毫秒数。" + USAGE, failure);
                    }
                }
                default -> {
                    if (args[index].startsWith("--") || directory != null) {
                        throw new IllegalArgumentException("未知参数或重复的 WAL 目录：" + args[index] + "。" + USAGE);
                    }
                    directory = Path.of(args[index]);
                }
            }
        }
        // 参数全部校验成功后才创建目录，避免无效调试配置留下临时文件。
        if (directory == null) {
            directory = Files.createTempDirectory("azir-wal-example-");
        }
        System.out.println("transactionTimeoutMillis=" + options.transactionTimeoutMillis());
        try (var runtime = WalMvccRuntime.open(directory, Set.of("left", "right"), options)) {
            var tx = runtime.begin(IsolationLevel.REPEATABLE_READ);
            int left = runtime.get(tx, "left", "balance").map(WalMvccExample::integer).orElse(100);
            int right = runtime.get(tx, "right", "balance").map(WalMvccExample::integer).orElse(100);
            runtime.put(tx, "left", "balance", bytes(left - 10));
            runtime.put(tx, "right", "balance", bytes(right + 10));
            WalReceipt receipt = runtime.commit(tx);
            var reader = runtime.begin(IsolationLevel.REPEATABLE_READ);
            System.out.println("walDirectory=" + directory.toAbsolutePath());
            System.out.println("receipt=" + receipt);
            System.out.println("left=" + integer(runtime.get(reader, "left", "balance").orElseThrow())
                    + ", right=" + integer(runtime.get(reader, "right", "balance").orElseThrow()));
            runtime.commit(reader);
            System.out.println("stats=" + runtime.stats());
        }
    }

    /** 将余额编码为 UTF-8 字节。 */
    private static byte[] bytes(int value) { return Integer.toString(value).getBytes(StandardCharsets.UTF_8); }

    /** 解码余额，显式保留简单示例的数据类型。 */
    private static int integer(byte[] value) { return Integer.parseInt(new String(value, StandardCharsets.UTF_8)); }
}
