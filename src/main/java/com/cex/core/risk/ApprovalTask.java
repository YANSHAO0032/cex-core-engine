package com.cex.core.risk;

import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 将单笔订单交给审批策略并回流结果事件的可运行任务。
 * 核心能力：映射审批结论为订单审批事件；线程安全：任务字段不可变，可由任意执行线程运行；使用限制：每个实例仅对应一笔源事件。
 */
public final class ApprovalTask implements Runnable {
    /** 审批所依据的原始订单事件。 */
    private final OrderEvent source;
    /** 对原始订单作出结论的业务策略。 */
    private final ApprovalPolicy policy;
    /** 接收审批通过或拒绝事件的统一回流端。 */
    private final Consumer<OrderEvent> sink;

    /**
     * 构造审批任务。
     *
     * @param source 原始订单事件
     * @param policy 审批策略
     * @param sink 审批结果事件接收端
     * @throws NullPointerException 当任一参数为 {@code null} 时抛出
     */
    public ApprovalTask(OrderEvent source, ApprovalPolicy policy, Consumer<OrderEvent> sink) {
        this.source = Objects.requireNonNull(source, "source");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * 执行审批并发布与源订单关联的结果事件。
     *
     * @note 审批结果必须非空；回流事件保留订单标识、用户、金额和事件时间，以统一入口处理乱序成交门禁。
     */
    @Override
    public void run() {
        ApprovalDecision decision = Objects.requireNonNull(policy.decide(source), "approval decision");
        OrderEventType type = decision == ApprovalDecision.PASS
                ? OrderEventType.APPROVAL_PASSED : OrderEventType.APPROVAL_REJECTED;
        sink.accept(new OrderEvent(source.orderId(), source.userId(), source.amount(),
                source.eventTimeMillis(), type));
    }
}
