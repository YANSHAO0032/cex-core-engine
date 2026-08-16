package com.cex.core.risk;

/**
 * 单项订单风控判断规则。
 * 核心能力：根据风险上下文决定通过或挂起；线程安全：由规则实现方保证；使用限制：评估应快速完成且不得返回 {@code null}。
 */
@FunctionalInterface
public interface RiskRule {
    /**
     * 评估当前订单上下文。
     *
     * @param context 订单及近期结算金额快照
     * @return 风险通过或挂起决定
     */
    RiskDecision evaluate(RiskContext context);
}
