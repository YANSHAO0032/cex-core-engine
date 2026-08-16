package com.cex.core.risk;

import com.cex.core.order.OrderEvent;

@FunctionalInterface
public interface ApprovalPolicy {
    ApprovalDecision decide(OrderEvent orderEvent);
}
