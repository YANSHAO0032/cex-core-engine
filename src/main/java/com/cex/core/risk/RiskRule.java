package com.cex.core.risk;

@FunctionalInterface
public interface RiskRule {
    RiskDecision evaluate(RiskContext context);
}
