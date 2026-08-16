package com.cex.core.risk;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import java.util.concurrent.CountDownLatch;
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

    @Test
    void fillDuringRiskHoldWaitsAndRejectedApprovalCancelsWithoutSettlement() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();
            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());

            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.MATCH_FILLED);

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(900L, fixture.ledger.getRequiredAccount(1L).available());
            assertEquals(0L, fixture.ledger.getRequiredAccount(1L).frozen());
            assertEquals(1L, fixture.engine.metrics().settleCount());
            assertEquals(1L, fixture.engine.metrics().unfreezeCount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    @Test
    void approvedRiskHoldAppliesCachedFillExactlyOnce() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.PASS)) {
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();
            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.MATCH_FILLED);

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.FILLED, fixture.engine.order(2L).status());
            assertEquals(200L, fixture.ledger.systemSettledAmount());
            assertEquals(2L, fixture.engine.metrics().settleCount());
            assertEquals(0L, fixture.engine.metrics().unfreezeCount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    @Test
    void fillBeforeCreateStillEntersRiskHoldBeforeSettlement() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    private static final class BlockingApprovalFixture implements AutoCloseable {
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        private final CountDownLatch approvalEntered = new CountDownLatch(1);
        private final CountDownLatch releaseApproval = new CountDownLatch(1);
        private final ApprovalService approvals = new ApprovalService(1, 8);
        private final OrderEngine engine;

        private BlockingApprovalFixture(ApprovalDecision decision) {
            ledger.createAccount(1L, 1_000L);
            engine = new OrderEngine(
                    ledger,
                    new RiskPipeline(new SlidingWindowAmountRule(0L)),
                    new ManualClock(1L),
                    approvals,
                    event -> {
                        approvalEntered.countDown();
                        try {
                            releaseApproval.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return ApprovalDecision.REJECT;
                        }
                        return decision;
                    });
            process(1L, OrderEventType.ORDER_CREATED);
            process(1L, OrderEventType.MATCH_FILLED);
        }

        private void process(long orderId, OrderEventType type) {
            engine.process(new OrderEvent(orderId, 1L, 100L, 1L, type));
        }

        private void awaitApprovalEntry() throws InterruptedException {
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
        }

        @Override
        public void close() {
            releaseApproval.countDown();
            engine.close();
        }
    }
}
