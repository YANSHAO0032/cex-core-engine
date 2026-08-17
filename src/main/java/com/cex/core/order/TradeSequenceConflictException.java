package com.cex.core.order;

/**
 * 表示订单权威序号不连续或同序号载荷发生协议冲突。
 *
 * <p>核心能力：对序号空洞误执行和同序号不同事件提供确定性失败类型。</p>
 * <p>线程安全：异常构造后不可变，可在线程间安全传播。</p>
 * <p>使用限制：未来事件正常缓存不属于冲突；该异常仅在拒绝或错误提交时使用。</p>
 */
public final class TradeSequenceConflictException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用序号冲突原因创建异常。
     *
     * @param message 失败原因
     */
    public TradeSequenceConflictException(String message) { super(message); }
}
