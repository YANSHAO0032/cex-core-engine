package com.cex.core.engine.risk;

import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.ledger.LedgerBalance;
import com.cex.core.engine.ledger.LedgerService;
import com.cex.core.engine.order.OrderState;
import com.cex.core.engine.order.OrderStateMachine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the async in-memory approval workflow for risk-held orders. */
class ApprovalTaskServiceTest {

    @Test
    void approvalDebitFundsAndReleasesTheHeldOrder() {
        ApprovalFixture fixture = new ApprovalFixture(ApprovalDecision.APPROVED);
        fixture.ledger.freeze(7L, 100L);
        fixture.stateMachine.apply(OrderEvent.created(1L, 10L, 7L,
                "BTC-USDT", 50_000L, 100L));

        try (ApprovalTaskService approvals = fixture.approvals()) {
            approvals.start();
            ApprovalTask task = fixture.riskEngine.recordTradeAndSubmitApproval(
                    fixture.stateMachine, approvals, 1L, 2L, 3L,
                    7L, 10L, 4L, 100L, 1_000L);

            assertNotNull(task);
            assertTrue(approvals.awaitStatus(1L, ApprovalTaskStatus.APPROVED, 5_000L));
            assertEquals(OrderState.CREATED, fixture.stateMachine.get(10L).getState());

            LedgerBalance balance = fixture.ledger.snapshot(7L);
            assertEquals(900L, balance.getAvailable());
            assertEquals(0L, balance.getFrozen());
            assertEquals(100L, balance.getTraded());
            assertTrue(balance.isConserved());
        }
    }

    @Test
    void rejectionUnfreezesFundsAndCancelsTheHeldOrder() {
        ApprovalFixture fixture = new ApprovalFixture(ApprovalDecision.REJECTED);
        fixture.ledger.freeze(7L, 100L);
        fixture.stateMachine.apply(OrderEvent.created(1L, 10L, 7L,
                "BTC-USDT", 50_000L, 100L));

        try (ApprovalTaskService approvals = fixture.approvals()) {
            approvals.start();
            ApprovalTask task = fixture.riskEngine.recordTradeAndSubmitApproval(
                    fixture.stateMachine, approvals, 1L, 2L, 3L,
                    7L, 10L, 4L, 100L, 1_000L);

            assertNotNull(task);
            assertTrue(approvals.awaitStatus(1L, ApprovalTaskStatus.REJECTED, 5_000L));
            assertEquals(OrderState.CANCELLED, fixture.stateMachine.get(10L).getState());

            LedgerBalance balance = fixture.ledger.snapshot(7L);
            assertEquals(1_000L, balance.getAvailable());
            assertEquals(0L, balance.getFrozen());
            assertEquals(0L, balance.getTraded());
            assertTrue(balance.isConserved());
        }
    }

    @Test
    void duplicateTaskIdIsProcessedOnlyOnce() {
        ApprovalFixture fixture = new ApprovalFixture(ApprovalDecision.APPROVED);
        fixture.ledger.freeze(7L, 100L);
        fixture.stateMachine.apply(OrderEvent.created(1L, 10L, 7L,
                "BTC-USDT", 50_000L, 100L));
        fixture.stateMachine.apply(OrderEvent.riskHold(2L, 10L));

        try (ApprovalTaskService approvals = fixture.approvals()) {
            ApprovalTask first = approvals.submit(1L, 3L, 7L, 10L, 100L, 1_000L);
            ApprovalTask duplicate = approvals.submit(1L, 3L, 7L, 10L, 100L, 1_000L);

            assertSame(first, duplicate);
            assertEquals(1, approvals.pendingQueueSize());

            approvals.start();
            assertTrue(approvals.awaitStatus(1L, ApprovalTaskStatus.APPROVED, 5_000L));

            LedgerBalance balance = fixture.ledger.snapshot(7L);
            assertEquals(900L, balance.getAvailable());
            assertEquals(0L, balance.getFrozen());
            assertEquals(100L, balance.getTraded());
            assertTrue(balance.isConserved());
        }
    }

    private static final class ApprovalFixture {

        private final LedgerService ledger = new LedgerService(16);
        private final OrderStateMachine stateMachine = new OrderStateMachine();
        private final RiskEngine riskEngine = new RiskEngine(50L, 10_000L, 16);
        private final ApprovalDecision decision;

        private ApprovalFixture(ApprovalDecision decision) {
            this.decision = decision;
            ledger.openAccount(7L, 1_000L);
        }

        private ApprovalTaskService approvals() {
            return new ApprovalTaskService(stateMachine, ledger, task -> decision);
        }
    }
}
