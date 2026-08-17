package com.cex.core.order;

/**
 * 表示权威成交与订单元数据、剩余数量或冻结额不相容。
 *
 * <p>核心能力：报告可以确定性拒绝且不得写入订单的成交。</p>
 * <p>线程安全：异常构造后不可变，可在线程间安全传播。</p>
 * <p>使用限制：不表示同序号载荷冲突，序号协议错误使用独立异常。</p>
 */
public final class InvalidTradeExecutionException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用成交拒绝原因创建异常。
     *
     * @param message 失败原因
     */
    public InvalidTradeExecutionException(String message) { super(message); }

    /**
     * 使用成交拒绝原因和底层算术异常创建异常。
     *
     * @param message 失败原因
     * @param cause 底层精确算术失败
     */
    public InvalidTradeExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
