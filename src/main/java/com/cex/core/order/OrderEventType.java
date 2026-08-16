package com.cex.core.order;

/**
 * 驱动订单状态机收敛的业务事件类型。
 * 核心能力是区分创建、成交、取消和审批结果；枚举不可变且线程安全。
 * 限制：事件可乱序或重复到达，具体收敛规则由 {@link OrderEngine} 执行。
 */
public enum OrderEventType {
    /** 创建订单的基础事实；到达后触发冻结并开启状态机。 */
    ORDER_CREATED,
    /** 撮合成交事实；创建事实齐备后触发结算并进入 FILLED。 */
    MATCH_FILLED,
    /** 订单取消事实；未成交时触发解冻并进入 CANCELED。 */
    ORDER_CANCELLED,
    /** 异步审批通过事实；触发风险挂起订单恢复到 NEW。 */
    APPROVAL_PASSED,
    /** 异步审批拒绝事实；触发未结算订单解冻并进入 CANCELED。 */
    APPROVAL_REJECTED
}
