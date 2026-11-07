package com.flyingzc.azir.wal;

import java.util.List;
import java.util.concurrent.CompletionStage;

/** 复制提交的可信共识端口；实现必须由真实共识层保证多数派持久性与任期隔离。 */
public interface WalCommitAuthority {
    /** 非阻塞返回当前可服务领导者任期，零表示当前不是有效领导者。 */
    long epoch();

    /** 完成多数派读屏障并等待本地应用，返回仍然有效的领导者任期。 */
    long readBarrier(long timeoutMillis) throws Exception;

    /** 提交已准备的完整批次；只能在多数派提交且本地安装后成功，不能跨任期自动重提。 */
    CompletionStage<Void> submit(long epoch, List<WalCommit> commits);
}
