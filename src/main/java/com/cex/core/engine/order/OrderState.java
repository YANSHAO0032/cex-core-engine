package com.cex.core.engine.order;

/**
 * 订单生命周期状态。
 *
 * <p>状态表示聚合当前事实，外部触发原因由 EventType 表示。</p>
 */
public enum OrderState {
    /** 初始占位状态，表示订单聚合尚未进入创建完成态。 */
    INIT,
    /** 订单创建事件已生效，尚未发生有效成交。 */
    CREATED,
    /** 已发生部分成交，仍有剩余数量。 */
    PARTIAL_FILLED,
    /** 成交数量达到订单总量，订单进入完成终态。 */
    FILLED,
    /** 撤单事件已生效，订单进入撤销终态。 */
    CANCELLED,
    /** 风控阈值触发，订单暂停后续成交等待处理。 */
    RISK_HOLD
}
