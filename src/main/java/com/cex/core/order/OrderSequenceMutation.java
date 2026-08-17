package com.cex.core.order;

/**
 * 单订单权威序号消费的预计算不可变变更。
 *
 * <p>核心能力：将已验证的下一序号与其目标订单绑定，防止提交任意原始序号或交叉订单。</p>
 * <p>线程安全：对象不可变；准备和提交期间仍须持续持有目标订单所属用户锁。</p>
 * <p>使用限制：只能由 {@link OrderStateMachine} 创建和提交，不携带成交或资金变更。</p>
 */
public final class OrderSequenceMutation {
    private final OrderContext order;
    private final long orderSequence;

    /**
     * 创建已验证的序号消费变更。
     *
     * @param order 已验证下一事件所属的目标订单
     * @param orderSequence 已验证的下一权威序号
     * @note 构造器限制在订单包内；所有校验必须在创建前完成。
     */
    OrderSequenceMutation(OrderContext order, long orderSequence) {
        this.order = order;
        this.orderSequence = orderSequence;
    }

    OrderContext order() { return order; }
    long orderSequence() { return orderSequence; }
}
