package com.cex.core.order;

/**
 * 单订单成交准备阶段生成的不可变变更。
 *
 * <p>核心能力：携带全部预计算累计量、剩余量、序号、状态和买方余款释放额。</p>
 * <p>线程安全：对象不可变；关联订单仍须由所属用户锁保护。</p>
 * <p>使用限制：只能提交到准备它的订单，不能替代双边账本变更。</p>
 */
public final class OrderFillMutation {
    /** 提交后的累计基础资产成交量，单位为基础资产最小单位。 */
    private final long cumulativeBaseFilled;
    /** 提交后的累计报价资产成交量，单位为报价资产最小单位。 */
    private final long cumulativeQuoteFilled;
    /** 提交后的剩余基础资产委托量，单位为基础资产最小单位。 */
    private final long remainingBaseQuantity;
    /** 提交后仍冻结的预留资产数量；买单为报价资产、卖单为基础资产。 */
    private final long remainingReservedAmount;
    /** 最终买单价格改善产生的报价资产释放额，计入可用余额并保持资产守恒。 */
    private final long buyerQuoteReleaseAmount;
    /** 本次成交消费的订单权威序号。 */
    private final long orderSequence;
    /** 提交成交后的订单状态。 */
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

    /**
     * 获取提交后的累计基础资产成交量。
     *
     * @return 基础资产最小单位数量
     */
    long cumulativeBaseFilled() { return cumulativeBaseFilled; }

    /**
     * 获取提交后的累计报价资产成交量。
     *
     * @return 报价资产最小单位数量
     */
    long cumulativeQuoteFilled() { return cumulativeQuoteFilled; }

    /**
     * 获取提交后的剩余基础资产委托量。
     *
     * @return 基础资产最小单位数量
     */
    long remainingBaseQuantity() { return remainingBaseQuantity; }

    /**
     * 获取提交后仍冻结的预留资产数量。
     *
     * @return 买单报价资产或卖单基础资产的最小单位数量
     */
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

    /**
     * 获取本次成交消费的订单权威序号。
     *
     * @return 订单权威序号
     */
    long orderSequence() { return orderSequence; }

    /**
     * 获取提交成交后的订单状态。
     *
     * @return 部分成交、等待撤单或完全成交状态
     */
    OrderStatus status() { return status; }
}
