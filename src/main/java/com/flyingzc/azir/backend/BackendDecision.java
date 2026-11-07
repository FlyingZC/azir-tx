package com.flyingzc.azir.backend;

/** 持久状态查询结果；PENDING 表示尚未确认终态，不能解释为中止。 */
public record BackendDecision(State state, long commitTs) {
    /** 可查询的终态和明确的凭据退休状态。 */
    public enum State { PENDING, COMMITTED, ABORTED, RETIRED }

    /** 只有提交状态携带提交时间戳。 */
    public BackendDecision {
        java.util.Objects.requireNonNull(state, "state");
        if ((state == State.COMMITTED) != (commitTs > 0)) { throw new IllegalArgumentException("invalid decision"); }
    }
}
