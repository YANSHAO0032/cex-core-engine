package com.cex.core.order;

import com.cex.core.util.MoneyMath;

/**
 * 成交在单个订单权威序列中的轻量不可变引用。
 *
 * <p>核心能力：将双边成交标识投影为可按单订单序号登记的事件。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：不包含成交数量或资产信息，调用方须通过成交标识查询原始成交。</p>
 *
 * @param tradeId 严格为正的成交标识
 * @param orderId 严格为正的所属订单标识
 * @param orderSequence 严格为正的订单权威序号
 */
public record TradeOrderReference(long tradeId, long orderId, long orderSequence)
        implements SequencedOrderEvent {

    /**
     * 创建并校验成交订单引用。
     *
     * @param tradeId 严格为正的成交标识
     * @param orderId 严格为正的所属订单标识
     * @param orderSequence 严格为正的订单权威序号
     * @throws IllegalArgumentException 当任一标识或序号不为正数时抛出
     */
    public TradeOrderReference {
        MoneyMath.requirePositive(tradeId);
        MoneyMath.requirePositive(orderId);
        MoneyMath.requirePositive(orderSequence);
    }
}
