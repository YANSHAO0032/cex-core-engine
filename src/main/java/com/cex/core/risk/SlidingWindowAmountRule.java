package com.cex.core.risk;

import com.cex.core.util.MoneyMath;

public final class SlidingWindowAmountRule implements RiskRule {
    private final long threshold;

    public SlidingWindowAmountRule(long threshold) {
        this.threshold = MoneyMath.requireNonNegative(threshold);
    }

    @Override
    public RiskDecision evaluate(RiskContext context) {
        return context.recentSettledAmount() > threshold ? RiskDecision.HOLD : RiskDecision.PASS;
    }

    public long threshold() {
        return threshold;
    }
}
