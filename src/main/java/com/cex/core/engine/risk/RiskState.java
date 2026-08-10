package com.cex.core.engine.risk;

/**
 * 用户成交金额滑动窗口对应的风控结果。
 */
public enum RiskState {
    /** 当前十秒成交金额未超过阈值，允许保持正常流程。 */
    NORMAL,
    /** 当前十秒成交金额超过阈值，订单需要进入风控冻结。 */
    RISK_HOLD
}
