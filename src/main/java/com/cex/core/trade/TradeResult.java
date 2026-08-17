package com.cex.core.trade;

/**
 * 双边成交入口向调用方返回的处理结果。
 *
 * <p>核心能力：区分仍待处理、已结算、确定拒绝及终态重复投递。</p>
 * <p>线程安全：枚举值不可变，可在线程间安全共享。</p>
 * <p>使用限制：不替代 {@link TradeExecutionState}；记录状态仍是存储中的权威事实。</p>
 */
public enum TradeResult {
    /** 成交已登记但暂未满足双方结算条件。 */
    PENDING,
    /** 成交已完成双方结算。 */
    SETTLED,
    /** 成交被确定性拒绝。 */
    REJECTED,
    /** 已终结成交的相同载荷重复投递。 */
    DUPLICATE
}
