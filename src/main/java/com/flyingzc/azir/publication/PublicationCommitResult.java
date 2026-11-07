package com.flyingzc.azir.publication;

/** 原子提交决议及其在同一控制事务中是否完成发布。 */
public record PublicationCommitResult(long commitTs, boolean publishedImmediately) {
}
