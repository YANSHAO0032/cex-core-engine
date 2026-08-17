package com.cex.core.order;

import com.cex.core.util.MoneyMath;

/**
 * 带订单权威序号的不可变撤单确认事件。
 *
 * <p>核心能力：将撤单请求身份和权威确认序号关联至单个订单。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：不直接执行解冻或状态迁移，应由订单序列处理器消费。</p>
 *
 * @param cancelRequestId 严格为正的关联撤单请求标识
 * @param orderId 严格为正的所属订单标识
 * @param orderSequence 严格为正的订单权威序号
 * @param confirmedAtMillis 非负的确认毫秒时间戳
 */
public record CancelConfirmation(
        long cancelRequestId, long orderId, long orderSequence, long confirmedAtMillis)
        implements SequencedOrderEvent {

    /**
     * 创建并校验撤单确认。
     *
     * @param cancelRequestId 严格为正的关联撤单请求标识
     * @param orderId 严格为正的所属订单标识
     * @param orderSequence 严格为正的订单权威序号
     * @param confirmedAtMillis 非负的确认毫秒时间戳
     * @throws IllegalArgumentException 当标识或序号不为正数，或时间为负数时抛出
     */
    public CancelConfirmation {
        MoneyMath.requirePositive(cancelRequestId);
        MoneyMath.requirePositive(orderId);
        MoneyMath.requirePositive(orderSequence);
        MoneyMath.requireNonNegative(confirmedAtMillis);
    }
}
