package com.cex.core.risk;

import com.cex.core.order.OrderSubmission;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 将单笔强类型订单交给审批策略并回流审批结果的可运行任务。
 * 核心能力：映射审批结论为强类型审批结果；线程安全：任务字段不可变，可由任意执行线程运行；使用限制：每个实例仅对应一笔订单提交。
 */
public final class ApprovalTask implements Runnable {
    /** 审批所依据的强类型订单提交。 */
    private final OrderSubmission submission;
    /** 对订单提交作出结论的业务策略。 */
    private final ApprovalPolicy policy;
    /** 接收审批通过或拒绝结果的强类型回流端。 */
    private final Consumer<ApprovalResult> sink;

    /**
     * 构造审批任务。
     *
     * @param submission 强类型订单提交
     * @param policy 审批策略
     * @param sink 审批结果接收端
     * @throws NullPointerException 当任一参数为 {@code null} 时抛出
     */
    public ApprovalTask(
            OrderSubmission submission,
            ApprovalPolicy policy,
            Consumer<ApprovalResult> sink) {
        this.submission = Objects.requireNonNull(submission, "submission");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * 执行审批并发布与源订单关联的强类型结果。
     *
     * @note 审批结论必须非空；任务只发布结果，不修改订单状态、资金或撤单交付状态。
     */
    @Override
    public void run() {
        ApprovalDecision decision = Objects.requireNonNull(
                policy.decide(submission), "approval decision");
        sink.accept(new ApprovalResult(
                submission.orderId(), decision, System.currentTimeMillis()));
    }
}
