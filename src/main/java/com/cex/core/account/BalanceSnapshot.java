package com.cex.core.account;

import com.cex.core.util.MoneyMath;

/**
 * 单一资产余额的不可变读取快照。
 *
 * <p>能力：向调用方公开可用与冻结余额而不泄露可变余额桶。</p>
 * <p>线程安全：记录不可变，可在线程间安全传递。</p>
 * <p>限制：快照不保证跨用户或跨资产的一致性，需由调用方使用条带锁协调。</p>
 *
 * @param available 可用资产数量
 * @param frozen 冻结资产数量
 */
public record BalanceSnapshot(long available, long frozen) {
    /**
     * 创建并校验余额快照。
     *
     * @param available 可用资产数量
     * @param frozen 冻结资产数量
     * @throws IllegalArgumentException 当任一金额为负数时抛出
     */
    public BalanceSnapshot {
        MoneyMath.requireNonNegative(available);
        MoneyMath.requireNonNegative(frozen);
    }
}
