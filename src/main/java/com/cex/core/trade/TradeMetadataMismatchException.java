package com.cex.core.trade;

/**
 * 表示同一成交标识被投递了不同的权威载荷。
 *
 * <p>核心能力：阻止后到事件篡改已固化成交元数据。</p>
 * <p>线程安全：异常构造后不可变，可在线程间安全传播。</p>
 * <p>使用限制：仅表示 {@code tradeId} 载荷冲突，不表示订单或资产结算校验失败。</p>
 */
public final class TradeMetadataMismatchException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用冲突说明创建异常。
     *
     * @param message 元数据不一致的诊断信息
     */
    public TradeMetadataMismatchException(String message) {
        super(message);
    }
}
