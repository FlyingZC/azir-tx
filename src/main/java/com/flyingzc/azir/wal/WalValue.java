package com.flyingzc.azir.wal;

/** 不可变的已提交版本，删除版本仍保留提交序号。 */
public record WalValue(long commitSequence, byte[] value) {
    /** 拷贝版本值以隔离业务代码对底层数据的修改。 */
    public WalValue { value = value == null ? null : value.clone(); }

    /** 返回版本值副本。 */
    @Override public byte[] value() { return value == null ? null : value.clone(); }

    /** 判断该版本是否为墓碑。 */
    public boolean deleted() { return value == null; }

    /** 查询载荷长度时不复制整行，供流式备份和恢复预算使用。 */
    public int valueLength() { return value == null ? 0 : value.length; }
}
