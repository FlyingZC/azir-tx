package com.flyingzc.azir.publication;

/** 每个提交时间戳的持久化事务身份、终态和接管截止时间。 */
public record PublicationEntry(long txId, long commitTs, PublicationEntryStatus status, long expiresAtMillis) {
}
