package com.cex.core.engine.event;

/**
 * 订单生命周期事件类型。
 *
 * <p>枚举值描述外部事实或风控指令，不等同于订单当前状态。</p>
 */
public enum EventType {
    /** 订单创建事件，携带用户、交易对、价格和数量等初始信息。 */
    ORDER_CREATED,
    /** 订单撤单事件，要求终止未完成订单。 */
    ORDER_CANCELLED,
    /** 成交事件，携带本次撮合成交数量。 */
    MATCH_FILLED,
    /** 风控超阈值事件，要求订单进入风险冻结状态。 */
    RISK_HOLD
}
