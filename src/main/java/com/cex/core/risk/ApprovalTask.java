package com.cex.core.risk;

import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import java.util.Objects;
import java.util.function.Consumer;

public final class ApprovalTask implements Runnable {
    private final OrderEvent source;
    private final ApprovalPolicy policy;
    private final Consumer<OrderEvent> sink;

    public ApprovalTask(OrderEvent source, ApprovalPolicy policy, Consumer<OrderEvent> sink) {
        this.source = Objects.requireNonNull(source, "source");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    @Override
    public void run() {
        ApprovalDecision decision = Objects.requireNonNull(policy.decide(source), "approval decision");
        OrderEventType type = decision == ApprovalDecision.PASS
                ? OrderEventType.APPROVAL_PASSED : OrderEventType.APPROVAL_REJECTED;
        sink.accept(new OrderEvent(source.orderId(), source.userId(), source.amount(),
                source.eventTimeMillis(), type));
    }
}
