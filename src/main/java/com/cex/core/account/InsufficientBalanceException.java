package com.cex.core.account;

/**
 * 表示资金不足导致的账本拒绝。
 *
 * <p>能力：区分余额不足与一般参数错误，同时保持旧版 {@link IllegalArgumentException} 兼容性。</p>
 * <p>线程安全：异常对象在构造后不可变，可在线程间安全传播。</p>
 * <p>限制：仅表示准备阶段的资金不足，不表示锁定、网络或持久化失败。</p>
 */
public final class InsufficientBalanceException extends IllegalArgumentException {
    /**
     * 使用资金不足原因创建异常。
     *
     * @param message 失败原因
     */
    public InsufficientBalanceException(String message) { super(message); }
}
