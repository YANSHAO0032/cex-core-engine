package com.cex.core.order;

/**
 * 带单订单权威序号的不可变事件契约。
 *
 * <p>核心能力：统一提交、成交引用和撤单确认的订单身份与序号读取方式。</p>
 * <p>线程安全：允许的实现均为不可变记录，可在线程间安全传递。</p>
 * <p>使用限制：只描述单订单事件，不负责登记或执行序号推进。</p>
 */
public sealed interface SequencedOrderEvent
        permits OrderSubmission, TradeOrderReference, CancelConfirmation {

    /**
     * 返回事件所属订单的唯一标识。
     *
     * @return 严格为正的订单标识
     */
    long orderId();

    /**
     * 返回该订单内的权威事件序号。
     *
     * @return 严格为正的订单序号
     */
    long orderSequence();
}
