package com.cex.core.risk;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApprovalTest {
    @Test
    void approvalServiceOnlyEmitsAnEvent() throws Exception {
        ApprovalService service = new ApprovalService(1, 1);
        try {
            java.util.concurrent.atomic.AtomicReference<OrderEvent> received = new java.util.concurrent.atomic.AtomicReference<>();
            service.submit(new OrderEvent(1L, 1L, 10L, 1L, OrderEventType.ORDER_CREATED),
                    event -> ApprovalDecision.REJECT, received::set);
            service.awaitQuiescence(2, TimeUnit.SECONDS);
            assertNotNull(received.get());
            assertEquals(OrderEventType.APPROVAL_REJECTED, received.get().type());
            assertEquals(1L, service.submittedCount());
        } finally { service.close(); }
    }

    @Test
    void rejectUnfreezesThroughUnifiedOrderEntryExactlyOnce() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createAccount(1L, 1000L);
        ApprovalService approvals = new ApprovalService(1, 4);
        OrderEngine engine = new OrderEngine(ledger,
                new RiskPipeline(new SlidingWindowAmountRule(0L)), new ManualClock(1L), approvals,
            event -> ApprovalDecision.REJECT);
        try {
            engine.process(new OrderEvent(1L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.process(new OrderEvent(1L, 1L, 100L, 1L, OrderEventType.MATCH_FILLED));
            engine.process(new OrderEvent(2L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.process(new OrderEvent(2L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.awaitApprovals(2, TimeUnit.SECONDS);
            assertEquals(OrderStatus.CANCELED, engine.order(2L).status());
            assertEquals(900L, ledger.getRequiredAccount(1L).available());
            assertEquals(0L, ledger.getRequiredAccount(1L).frozen());
            assertEquals(1L, engine.metrics().approvalScheduledCount());
            assertEquals(1L, engine.metrics().unfreezeCount());
        } finally { engine.close(); }
    }
}
