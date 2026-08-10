package com.cex.core.engine.order;

/**
 * 订单事件处理结果类别。
 *
 * <p>用于区分事件已执行、乱序暂存、重复幂等和合法但无状态变化等场景。</p>
 */
public enum EventApplyStatus {
    /** 事件已执行并改变订单或完成创建。 */
    APPLIED,
    /** 订单尚未创建，事件已写入乱序待处理队列。 */
    BUFFERED,
    /** eventId 已处理或已在待处理队列中，禁止再次执行。 */
    DUPLICATE,
    /** 事件因终态或业务状态不允许而被忽略。 */
    IGNORED
}
