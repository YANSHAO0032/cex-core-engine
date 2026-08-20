package com.cex.core.trade;

/**
 * 成交记录在双边结算协调过程中的生命周期状态。
 *
 * <p>核心能力：区分可重试的挂起成交和保留用于幂等识别的终态成交。</p>
 * <p>线程安全：枚举值不可变，可在线程间安全共享。</p>
 * <p>使用限制：状态不承载结算副作用；副作用由后续双边协调器负责。</p>
 */
public enum TradeExecutionState {
    /** 已登记且等待双边订单满足结算条件的成交。 */
    PENDING,
    /** 已原子结算双方订单和资产的成交。 */
    SETTLED,
    /** 已确定不能结算并保留拒绝原因的成交。 */
    REJECTED;

    /**
     * 判断当前状态是否已经终结。
     *
     * @return 当状态为已结算或已拒绝时为 {@code true}
     */
    public boolean isTerminal() {
        return this != PENDING;
    }
}
