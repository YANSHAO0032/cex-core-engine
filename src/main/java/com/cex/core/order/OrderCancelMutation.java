package com.cex.core.order;

/**
 * 权威撤单确认准备阶段生成的不可变订单变更。
 *
 * <p>核心能力：携带应由账本释放的剩余冻结额及确认后的订单状态和序号。</p>
 * <p>线程安全：对象不可变；关联订单仍须由所属用户锁保护。</p>
 * <p>使用限制：本对象不执行解冻，账本提交成功后才能提交订单变更。</p>
 */
public final class OrderCancelMutation {
    /** 撤单确认后从冻结余额释放到可用余额的资产数量，单位为对应资产最小单位。 */
    private final long releaseAmount;
    /** 本次撤单确认消费的订单权威序号。 */
    private final long orderSequence;
    /** 提交撤单确认后的订单终态。 */
    private final OrderStatus status;

    /**
     * 创建已完成全部校验的撤单变更。
     *
     * @param releaseAmount 应释放的剩余冻结资产量
     * @param orderSequence 本次确认的订单权威序号
     * @param status 提交后的订单状态
     * @note 仅由订单状态机准备阶段创建；提交阶段只赋值，不再检查资金或状态。
     */
    OrderCancelMutation(long releaseAmount, long orderSequence, OrderStatus status) {
        this.releaseAmount = releaseAmount;
        this.orderSequence = orderSequence;
        this.status = status;
    }

    /**
     * 返回撤单确认应从冻结桶释放的资产数量。
     *
     * @return 非负的剩余冻结资产量；已完全成交订单为零
     */
    public long releaseAmount() { return releaseAmount; }

    /**
     * 获取本次撤单确认消费的权威序号。
     *
     * @return 订单权威序号
     */
    long orderSequence() { return orderSequence; }

    /**
     * 获取提交撤单确认后的订单状态。
     *
     * @return 已取消或保持已成交的终态
     */
    OrderStatus status() { return status; }
}
