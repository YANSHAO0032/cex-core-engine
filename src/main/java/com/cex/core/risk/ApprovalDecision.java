package com.cex.core.risk;

/**
 * 人工审批任务的最终裁决结果。
 * 核心能力：向订单引擎表达放行或拒绝；线程安全：枚举实例不可变且天然线程安全；使用限制：仅由审批策略返回。
 */
public enum ApprovalDecision {
    /** 审批通过，允许订单继续后续处理。 */
    PASS,
    /** 审批拒绝，订单应进入取消与解冻流程。 */
    REJECT
}
