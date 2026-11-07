package com.flyingzc.azir.metadata;

/** 已持久化快照租约后的事务开始结果。 */
public record BeginReservation(long txId, long startTs, long deadlineMillis, boolean metadataPersisted) {
}
