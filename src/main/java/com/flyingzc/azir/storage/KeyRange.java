package com.flyingzc.azir.storage;

/**
 * 键范围对象，描述扫描的起止边界。
 */
public record KeyRange(String startKeyInclusive, String endKeyExclusive) {
}
