package com.flyingzc.azir.storage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * 与 MySQL VARBINARY 一致的无符号 UTF-8 键编码和排序规则。
 */
public final class Utf8KeyOrder {
    /**
     * 阻止实例化键顺序工具。
     */
    private Utf8KeyOrder() {
    }

    /**
     * 编码业务键并拒绝会被 UTF-8 替换为同一字节序列的非法代理字符。
     */
    public static byte[] encode(String key) {
        Objects.requireNonNull(key, "key");
        byte[] encoded = key.getBytes(StandardCharsets.UTF_8);
        if (!key.equals(new String(encoded, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("key contains malformed UTF-16");
        }
        return encoded;
    }

    /**
     * 按无符号 UTF-8 字节顺序比较两个业务键。
     */
    public static int compare(String left, String right) {
        return Arrays.compareUnsigned(encode(left), encode(right));
    }
}
