package com.cex.core.order;

/**
 * 已接收订单事件的滞后事实缓存。
 * 核心能力是用位图保存乱序到达的事件并支持后续收敛；枚举不可变且线程安全。
 * 限制：事实只记录是否见过，不能表达事件顺序或重复次数。
 */
public enum OrderFact {
    /** 已收到 ORDER_CREATED，允许订单进入冻结和状态机流程。 */
    CREATED_SEEN(1 << 0),
    /** 已收到 MATCH_FILLED，创建事实齐备后触发结算。 */
    FILLED_SEEN(1 << 1),
    /** 已收到 ORDER_CANCELLED，创建事实齐备且未成交时触发解冻取消。 */
    CANCELLED_SEEN(1 << 2),
    /** 已收到审批通过事实，可将风控挂起订单恢复为新单。 */
    APPROVED_SEEN(1 << 3),
    /** 已收到审批拒绝事实，可将未结算订单解冻取消。 */
    REJECTED_SEEN(1 << 4);

    /** 当前事实在订单事实位图中的位掩码。 */
    private final int mask;

    /**
     * 创建订单事实及其唯一位掩码映射。
     *
     * @param mask 事实在原子位图中的唯一二进制掩码
     */
    OrderFact(int mask) {
        this.mask = mask;
    }

    /**
     * 返回当前事实的位掩码。
     *
     * @return 用于订单事实位图的唯一位掩码
     */
    public int mask() {
        return mask;
    }

    /**
     * 将事件类型映射为对应的滞后事实。
     *
     * @param eventType 待映射的订单事件类型，不能为空
     * @return 与事件类型一一对应的订单事实
     */
    public static OrderFact fromEventType(OrderEventType eventType) {
        return switch (eventType) {
            case ORDER_CREATED -> CREATED_SEEN;
            case MATCH_FILLED -> FILLED_SEEN;
            case ORDER_CANCELLED -> CANCELLED_SEEN;
            case APPROVAL_PASSED -> APPROVED_SEEN;
            case APPROVAL_REJECTED -> REJECTED_SEEN;
        };
    }
}
