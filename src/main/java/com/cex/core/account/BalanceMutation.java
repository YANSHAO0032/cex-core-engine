package com.cex.core.account;

/**
 * 单一余额桶的预计算变更。
 *
 * <p>能力：将准备阶段的最终余额安全传递给只赋值的提交阶段。</p>
 * <p>线程安全：对象不可变；关联余额桶仍必须由所属用户条带锁保护。</p>
 * <p>限制：不携带锁，也不在提交阶段重新计算或校验金额。</p>
 */
public final class BalanceMutation {
    private final AssetBalance balance;
    private final long availableAfter;
    private final long frozenAfter;

    /**
     * 创建预计算余额变更。
     *
     * @param balance 目标余额桶
     * @param availableAfter 提交后的可用数量
     * @param frozenAfter 提交后的冻结数量
     * @note 仅由账本准备阶段创建，所有金额在创建前已完成校验。
     */
    BalanceMutation(AssetBalance balance, long availableAfter, long frozenAfter) {
        this.balance = balance;
        this.availableAfter = availableAfter;
        this.frozenAfter = frozenAfter;
    }

    AssetBalance balance() { return balance; }
    long availableAfter() { return availableAfter; }
    long frozenAfter() { return frozenAfter; }
}
