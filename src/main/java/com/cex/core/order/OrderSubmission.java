package com.cex.core.order;

import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 强类型且不可变的订单创建输入。
 *
 * <p>核心能力：同时承载资金预留、上游风控名义金额和订单权威起始序号。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：不进行价格计算、资产冻结或订单状态变更。</p>
 *
 * @param orderId 严格为正的订单标识
 * @param userId 严格为正的下单用户标识
 * @param side 订单买卖方向
 * @param pair 订单所属交易对
 * @param baseQuantity 严格为正的基础资产最小单位数量
 * @param reservedAmount 严格为正的冻结资产最小单位数量，BUY 为报价资产、SELL 为基础资产
 * @param riskQuoteAmount 严格为正的上游报价资产风控名义金额，单位为报价资产最小单位
 * @param orderSequence 严格为正的订单权威序号
 * @param submittedAtMillis 非负的提交毫秒时间戳
 */
public record OrderSubmission(
        long orderId, long userId, OrderSide side, TradingPair pair,
        long baseQuantity, long reservedAmount, long riskQuoteAmount,
        long orderSequence, long submittedAtMillis) implements SequencedOrderEvent {

    /**
     * 创建并校验订单提交输入。
     *
     * @param orderId 严格为正的订单标识
     * @param userId 严格为正的下单用户标识
     * @param side 订单买卖方向
     * @param pair 订单所属交易对
     * @param baseQuantity 严格为正的基础资产最小单位数量
     * @param reservedAmount 严格为正的冻结资产最小单位数量，BUY 为报价资产、SELL 为基础资产
     * @param riskQuoteAmount 严格为正的上游报价资产风控名义金额，单位为报价资产最小单位
     * @param orderSequence 严格为正的订单权威序号
     * @param submittedAtMillis 非负的提交毫秒时间戳
     * @throws NullPointerException 当订单方向或交易对为 {@code null} 时抛出
     * @throws IllegalArgumentException 当标识、金额或序号不符合边界时抛出
     */
    public OrderSubmission {
        MoneyMath.requirePositive(orderId);
        MoneyMath.requirePositive(userId);
        Objects.requireNonNull(side, "side");
        Objects.requireNonNull(pair, "pair");
        MoneyMath.requirePositive(baseQuantity);
        MoneyMath.requirePositive(reservedAmount);
        MoneyMath.requirePositive(riskQuoteAmount);
        MoneyMath.requirePositive(orderSequence);
        MoneyMath.requireNonNegative(submittedAtMillis);
    }
}
