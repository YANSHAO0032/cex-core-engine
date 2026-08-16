package com.cex.core.risk;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RiskEngineTest {
    @Test
    void thresholdUsesSettledAmountAndExpiresWithoutSleeping() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createAccount(1L, 1000L);
        ManualClock clock = new ManualClock(100L);
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(ledger,
                new RiskPipeline(new SlidingWindowAmountRule(100L)), clock, approvals, event -> {
                    approvalEntered.countDown();
                    try {
                        releaseApproval.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return ApprovalDecision.REJECT;
                    }
                    return ApprovalDecision.PASS;
                });
        try {
            engine.process(event(1L, 120L, OrderEventType.ORDER_CREATED));
            engine.process(event(1L, 120L, OrderEventType.MATCH_FILLED));
            engine.process(event(2L, 50L, OrderEventType.ORDER_CREATED));
            assertEquals(OrderStatus.RISK_HOLD, engine.order(2L).status());
            assertTrue(approvalEntered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            releaseApproval.countDown();
            engine.awaitApprovals(2, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(OrderStatus.NEW, engine.order(2L).status());

            clock.advanceMillis(10_001L);
            engine.process(event(3L, 50L, OrderEventType.ORDER_CREATED));
            assertEquals(OrderStatus.NEW, engine.order(3L).status());
            assertEquals(1L, engine.metrics().riskHoldCount());
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    @Test
    void pipelineShortCircuitsAndSupportsCopyOnWriteReplacement() {
        AtomicInteger second = new AtomicInteger();
        RiskRule hold = context -> RiskDecision.HOLD;
        RiskRule countingPass = context -> { second.incrementAndGet(); return RiskDecision.PASS; };
        RiskPipeline pipeline = new RiskPipeline(hold, countingPass);
        RiskContext context = new RiskContext(1L, 1L, 1L, 1L, 1L);
        assertEquals(RiskDecision.HOLD, pipeline.evaluate(context));
        assertEquals(0, second.get());
        pipeline.replaceRules(countingPass);
        assertEquals(RiskDecision.PASS, pipeline.evaluate(context));
        assertEquals(1, second.get());
        assertTrue(pipeline.removeRule(countingPass));
        assertEquals(RiskDecision.PASS, pipeline.evaluate(context));
    }

    @Test
    void tradeWindowEvictsExpiredEntries() {
        TradeWindow window = new TradeWindow(10_000L);
        window.record(100L, 40L);
        window.record(1000L, 50L);
        assertEquals(90L, window.currentSum(10_000L));
        assertEquals(50L, window.currentSum(10_101L));
    }

    private static OrderEvent event(long orderId, long amount, OrderEventType type) {
        return new OrderEvent(orderId, 1L, amount, 100L, type);
    }
}
