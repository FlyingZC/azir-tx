package com.flyingzc.azir.wal;

import java.util.UUID;

/** 凭据负向过滤器，命中仍须核对精确 UUID 和内容，不能把概率命中当提交。 */
final class WalBloom {
    private final byte[] bits;

    /** 创建可序列化的过滤器，位数组大小必须是二的整数次幂。 */
    WalBloom(byte[] bits) {
        if (bits.length < 128 || Integer.bitCount(bits.length) != 1) { throw new IllegalArgumentException("invalid bloom size"); }
        this.bits = bits;
    }

    /** 按预计凭据数量分配有限位数组，超过预算只增加误判率而不产生假阴性。 */
    static WalBloom allocate(long records, int maximumBytes) {
        return new WalBloom(new byte[capacity(records, maximumBytes)]);
    }

    /** 在分配位数组之前计算大小，检查点可据此先检查完整编码预算。 */
    static int capacity(long records, int maximumBytes) {
        if (records < 0 || maximumBytes < 128 || Integer.bitCount(maximumBytes) != 1) { throw new IllegalArgumentException("invalid bloom capacity"); }
        int bytes = 128;
        while (bytes < maximumBytes && bytes * 8L < Math.min(records, Long.MAX_VALUE / 12) * 12L) { bytes *= 2; }
        return bytes;
    }

    /** 将一个已持久化凭据标记进过滤器。 */
    void add(UUID id) { probe(id, true); }

    /** 返回是否需要继续精确查找，假阴性会破坏去重及结果查询，必须被测试阻断。 */
    boolean mightContain(UUID id) { return probe(id, false); }

    /** 返回防御性副本，用于将过滤器纳入检查点整体校验。 */
    byte[] bytes() { return bits.clone(); }

    /** 返回位数组大小，容量检查不能为此复制整份过滤器。 */
    int byteSize() { return bits.length; }

    /** 用两个混合散列生成七个位位置，构造与查询共享完全相同的位运算。 */
    private boolean probe(UUID id, boolean insert) {
        long first = mix(id.getMostSignificantBits()); long step = mix(id.getLeastSignificantBits()) | 1;
        long mask = bits.length * 8L - 1;
        for (int i = 0; i < 7; i++) {
            long position = (first + i * step) & mask; int index = (int) (position >>> 3); int flag = 1 << (position & 7);
            if (insert) { bits[index] |= (byte) flag; }
            else if ((bits[index] & flag) == 0) { return false; }
        }
        return true;
    }

    /** 混合 UUID 的每个半部，避免顺序或相近输入集中落在少数位。 */
    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
