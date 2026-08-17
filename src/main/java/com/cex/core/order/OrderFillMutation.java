package com.cex.core.order;

/**
 * 单订单成交准备阶段生成的不可变变更。
 *
 * <p>核心能力：携带全部预计算累计量、剩余量、序号、状态和买方余款释放额。</p>
 * <p>线程安全：对象不可变；关联订单仍须由所属用户锁保护。</p>
 * <p>使用限制：只能提交到准备它的订单，不能替代双边账本变更。</p>
 */
public final class OrderFillMutation {
    private final long cumulativeBaseFilled;
    private final long cumulativeQuoteFilled;
    private final long remainingBaseQuantity;
    private final long remainingReservedAmount;
    private final long buyerQuoteReleaseAmount;
    private final long orderSequence;
    private final OrderStatus status;

    /**
     * 创建已完成全部校验的订单成交变更。
     *
     * @param cumulativeBaseFilled 提交后的累计基础资产成交量
     * @param cumulativeQuoteFilled 提交后的累计报价资产成交量
     * @param remainingBaseQuantity 提交后的剩余基础资产量
     * @param remainingReservedAmount 提交后的剩余冻结资产量
     * @param buyerQuoteReleaseAmount 完全成交时应原子释放的买方报价资产余款
     * @param orderSequence 本次消费的订单权威序号
     * @param status 提交后的订单状态
     * @note 仅由订单状态机准备阶段创建；提交阶段不得重新计算或校验。
     */
    OrderFillMutation(
            long cumulativeBaseFilled,
            long cumulativeQuoteFilled,
            long remainingBaseQuantity,
            long remainingReservedAmount,
            long buyerQuoteReleaseAmount,
            long orderSequence,
            OrderStatus status) {
        this.cumulativeBaseFilled = cumulativeBaseFilled;
        this.cumulativeQuoteFilled = cumulativeQuoteFilled;
        this.remainingBaseQuantity = remainingBaseQuantity;
        this.remainingReservedAmount = remainingReservedAmount;
        this.buyerQuoteReleaseAmount = buyerQuoteReleaseAmount;
        this.orderSequence = orderSequence;
        this.status = status;
    }

    long cumulativeBaseFilled() { return cumulativeBaseFilled; }
    long cumulativeQuoteFilled() { return cumulativeQuoteFilled; }
    long remainingBaseQuantity() { return remainingBaseQuantity; }
    long remainingReservedAmount() { return remainingReservedAmount; }

    /**
     * 判断本次变更提交后订单是否已经完全成交。
     *
     * @return 提交后剩余基础资产数量为零时为 {@code true}
     */
    public boolean complete() { return remainingBaseQuantity == 0L; }

    /**
     * 返回完全成交买单尚未花费且应随成交原子释放的报价资产。
     *
     * @return 非负的买方报价资产释放额；非最终买单或卖单为零
     */
    public long buyerQuoteReleaseAmount() { return buyerQuoteReleaseAmount; }

    long orderSequence() { return orderSequence; }
    OrderStatus status() { return status; }
}
