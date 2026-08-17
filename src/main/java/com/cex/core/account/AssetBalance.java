package com.cex.core.account;

import com.cex.core.util.MoneyMath;

/**
 * 单一资产的可用与冻结余额桶。
 *
 * <p>能力：保存账本已经校验的余额，并允许提交预计算变更。</p>
 * <p>线程安全：所有读写均由所属用户的条带锁保护。</p>
 * <p>限制：不执行跨账户校验或资产守恒计算。</p>
 */
final class AssetBalance {
    private long available;
    private long frozen;

    /**
     * 创建余额桶。
     *
     * @param available 初始可用数量
     * @param frozen 初始冻结数量
     * @throws IllegalArgumentException 当任一金额为负数时抛出
     * @note 由账本在已完成资产总额溢出校验后调用。
     */
    AssetBalance(long available, long frozen) {
        this.available = MoneyMath.requireNonNegative(available);
        this.frozen = MoneyMath.requireNonNegative(frozen);
    }

    long available() { return available; }

    long frozen() { return frozen; }

    /**
     * 提交预计算后的可用数量。
     *
     * @param available 已校验的可用数量
     * @note 仅提交阶段调用；调用方必须持有所属用户条带锁。
     */
    void setAvailable(long available) { this.available = available; }

    /**
     * 提交预计算后的冻结数量。
     *
     * @param frozen 已校验的冻结数量
     * @note 仅提交阶段调用；调用方必须持有所属用户条带锁。
     */
    void setFrozen(long frozen) { this.frozen = frozen; }
}
