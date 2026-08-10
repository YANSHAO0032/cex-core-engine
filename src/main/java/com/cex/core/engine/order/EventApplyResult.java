package com.cex.core.engine.order;

/**
 * 订单状态机事件处理结果。
 *
 * <p>结果对象为不可变快照；高吞吐消费者可使用 applyFast 避免创建该对象。</p>
 */
public final class EventApplyResult {

    /** 事件处理结果类别。 */
    private final EventApplyStatus status;
    /** 处理后的订单快照；事件暂存且订单未知时为 null。 */
    private final Order order;

    /**
     * 创建事件处理结果。
     *
     * @param status 事件处理结果类别
     * @param order 处理后的订单快照，可为空
     */
    EventApplyResult(EventApplyStatus status, Order order) {
        this.status = status;
        this.order = order;
    }

    /**
     * 获取事件处理状态。
     *
     * @return 事件处理结果类别
     */
    public EventApplyStatus getStatus() {
        return status;
    }

    /**
     * 获取订单快照。
     *
     * @return 处理后的订单快照，未知订单事件暂存时返回 null
     */
    public Order getOrder() {
        return order;
    }
}
