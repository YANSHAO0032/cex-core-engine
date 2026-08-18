package com.cex.core.order;

import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 外部撮合提供的双边成交结果。
 *
 * <p>核心能力：携带权威基础/报价数量及两个订单各自的权威序号。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：不重算成交金额、不执行结算，且不允许同订单自成交。</p>
 *
 * @param tradeId 严格为正的成交标识
 * @param buyOrderId 严格为正的买方订单标识
 * @param sellOrderId 严格为正的卖方订单标识，且不同于买方订单标识
 * @param pair 成交所属交易对
 * @param baseQuantity 严格为正的权威基础资产成交数量，单位为基础资产最小单位并由卖方冻结余额交付
 * @param quoteQuantity 严格为正的权威报价资产成交数量，单位为报价资产最小单位并由买方冻结余额支付
 * @param buyOrderSequence 严格为正的买方订单权威序号
 * @param sellOrderSequence 严格为正的卖方订单权威序号
 * @param executedAtMillis 非负的执行毫秒时间戳
 */
public record TradeExecution(
        long tradeId, long buyOrderId, long sellOrderId, TradingPair pair,
        long baseQuantity, long quoteQuantity,
        long buyOrderSequence, long sellOrderSequence,
        long executedAtMillis) {

    /**
     * 创建并校验外部成交结果。
     *
     * @param tradeId 严格为正的成交标识
     * @param buyOrderId 严格为正的买方订单标识
     * @param sellOrderId 严格为正的卖方订单标识，且不同于买方订单标识
     * @param pair 成交所属交易对
     * @param baseQuantity 严格为正的权威基础资产成交数量，单位为基础资产最小单位并由卖方冻结余额交付
     * @param quoteQuantity 严格为正的权威报价资产成交数量，单位为报价资产最小单位并由买方冻结余额支付
     * @param buyOrderSequence 严格为正的买方订单权威序号
     * @param sellOrderSequence 严格为正的卖方订单权威序号
     * @param executedAtMillis 非负的执行毫秒时间戳
     * @throws NullPointerException 当交易对为 {@code null} 时抛出
     * @throws IllegalArgumentException 当标识、数量、序号或时间不符合边界时抛出
     */
    public TradeExecution {
        MoneyMath.requirePositive(tradeId);
        MoneyMath.requirePositive(buyOrderId);
        MoneyMath.requirePositive(sellOrderId);
        if (buyOrderId == sellOrderId) {
            throw new IllegalArgumentException("buy and sell order IDs must differ");
        }
        Objects.requireNonNull(pair, "pair");
        MoneyMath.requirePositive(baseQuantity);
        MoneyMath.requirePositive(quoteQuantity);
        MoneyMath.requirePositive(buyOrderSequence);
        MoneyMath.requirePositive(sellOrderSequence);
        MoneyMath.requireNonNegative(executedAtMillis);
    }
}
