package com.cex.core.risk;

import com.cex.core.util.MoneyMath;

/**
 * 基于近期已结算金额阈值的滑动窗口风控规则。
 * 核心能力：在窗口累计金额超过阈值时触发人工审核；线程安全：阈值不可变，规则实例天然线程安全；使用限制：窗口统计由调用方填入 {@link RiskContext}。
 */
public final class SlidingWindowAmountRule implements RiskRule {
    /** 窗口内已结算金额超过该业务阈值时触发风险挂起。 */
    private final long threshold;

    /**
     * 创建金额阈值规则。
     *
     * @param threshold 触发审核的非负窗口累计金额阈值
     * @throws IllegalArgumentException 当阈值为负数时抛出
     */
    public SlidingWindowAmountRule(long threshold) {
        this.threshold = MoneyMath.requireNonNegative(threshold);
    }

    /**
     * 判断窗口已结算金额是否超过阈值。
     *
     * @param context 包含近期已结算金额的风险上下文
     * @return 超过阈值时返回 {@link RiskDecision#HOLD}，否则返回 {@link RiskDecision#PASS}
     * @note 仅比较已结算金额，当前订单金额不会预先计入窗口。
     */
    @Override
    public RiskDecision evaluate(RiskContext context) {
        return context.recentSettledAmount() > threshold ? RiskDecision.HOLD : RiskDecision.PASS;
    }

    /** 获取触发人工审核的金额阈值。
     * @return 窗口累计金额阈值
     */
    public long threshold() {
        return threshold;
    }
}
