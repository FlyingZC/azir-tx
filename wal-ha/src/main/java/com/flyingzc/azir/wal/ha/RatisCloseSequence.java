package com.flyingzc.azir.wal.ha;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** 按依赖顺序释放资源，失败保留当前阶段，成功阶段不会在重试时重复执行。 */
final class RatisCloseSequence implements Closeable {
    private final List<Closeable> steps;
    private int next;

    /** 调用方明确安排先停生产者、再排空消费者、最后释放存储和保护登记。 */
    RatisCloseSequence(Closeable... steps) { this.steps = List.of(steps); }

    /** 任一步失败就停止，不提前释放仍可能被前一阶段使用的资源。 */
    @Override public synchronized void close() throws IOException {
        while (next < steps.size()) {
            steps.get(next).close();
            next++;
        }
    }
}
