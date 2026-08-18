package com.cex.core.trade;

/**
 * 表示挂起或总成交记录容量已满，新的成交标识必须受到背压。
 *
 * <p>核心能力：阻止内存存储超出其固定容量，而不逐出任何幂等记录。</p>
 * <p>线程安全：异常构造后不可变，可在线程间安全传播。</p>
 * <p>使用限制：相同 {@code tradeId} 的精确重复不应触发本异常。</p>
 */
public final class PendingCapacityExceededException extends IllegalStateException {
    /** 异常序列化版本标识。 */
    private static final long serialVersionUID = 1L;

    /**
     * 使用容量诊断信息创建异常。
     *
     * @param message 容量不足的说明
     */
    public PendingCapacityExceededException(String message) {
        super(message);
    }
}
