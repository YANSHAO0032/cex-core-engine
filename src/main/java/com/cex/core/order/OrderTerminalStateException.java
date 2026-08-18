package com.cex.core.order;

/**
 * 表示成交试图修改已经进入终态的订单。
 *
 * <p>核心能力：将终态拒绝与普通成交数量错误区分。</p>
 * <p>线程安全：异常构造后不可变，可在线程间安全传播。</p>
 * <p>使用限制：只在准备阶段抛出，不表示账本或订单已经发生部分提交。</p>
 */
public final class OrderTerminalStateException extends IllegalStateException {
    /** 异常序列化版本标识。 */
    private static final long serialVersionUID = 1L;

    /**
     * 使用终态拒绝原因创建异常。
     *
     * @param message 失败原因
     */
    public OrderTerminalStateException(String message) { super(message); }
}
