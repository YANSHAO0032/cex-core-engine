package com.cex.core.risk;

import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 人工审批回流至订单域的强类型不可变结果。
 *
 * <p>核心能力：以订单标识、审批结论和决定时间表达审批回调。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：不携带订单金额或执行撤单、解冻等后续动作。</p>
 *
 * @param orderId 严格为正的订单标识
 * @param decision 非空的审批结论
 * @param decidedAtMillis 非负的审批决定毫秒时间戳
 */
public record ApprovalResult(long orderId, ApprovalDecision decision, long decidedAtMillis) {

    /**
     * 创建并校验审批结果。
     *
     * @param orderId 严格为正的订单标识
     * @param decision 非空的审批结论
     * @param decidedAtMillis 非负的审批决定毫秒时间戳
     * @throws NullPointerException 当审批结论为 {@code null} 时抛出
     * @throws IllegalArgumentException 当订单标识不为正数或时间为负数时抛出
     */
    public ApprovalResult {
        MoneyMath.requirePositive(orderId);
        Objects.requireNonNull(decision, "decision");
        MoneyMath.requireNonNegative(decidedAtMillis);
    }
}
