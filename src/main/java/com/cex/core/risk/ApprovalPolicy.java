package com.cex.core.risk;

import com.cex.core.order.OrderEvent;

/**
 * 定义人工审批的业务决策策略。
 * 核心能力：基于原始订单事件输出审批结论；线程安全：由实现方保证；使用限制：不得返回 {@code null}。
 */
@FunctionalInterface
public interface ApprovalPolicy {
    /**
     * 对待审批订单作出裁决。
     *
     * @param orderEvent 待审批的原始订单事件
     * @return 审批通过或拒绝的结果
     */
    ApprovalDecision decide(OrderEvent orderEvent);
}
