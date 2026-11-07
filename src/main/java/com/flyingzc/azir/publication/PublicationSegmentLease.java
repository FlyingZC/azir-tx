package com.flyingzc.azir.publication;

/** 一段已持久预留的提交时间槽及其 owner fencing 代数。 */
public record PublicationSegmentLease(long firstTs, long lastTs, String ownerId,
                                      long generation, long expiresAtMillis) {
    /** 拒绝无效范围、owner 或代数，避免本地分配越过持久边界。 */
    public PublicationSegmentLease {
        if (firstTs < 1 || lastTs < firstTs || ownerId == null || ownerId.isBlank()
                || generation < 1 || expiresAtMillis < 1) {
            throw new IllegalArgumentException("invalid publication segment lease");
        }
    }
}
