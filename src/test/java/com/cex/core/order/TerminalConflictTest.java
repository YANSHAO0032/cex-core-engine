package com.cex.core.order;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.risk.ManualClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TerminalConflictTest {
    @Test
    void lateCancelAfterFillDoesNotUnfreeze() {
        Fixture f = new Fixture();
        try {
            f.engine.process(e(OrderEventType.ORDER_CREATED));
            f.engine.process(e(OrderEventType.MATCH_FILLED));
            f.engine.process(e(OrderEventType.ORDER_CANCELLED));
            assertEquals(OrderStatus.FILLED, f.engine.order(1L).status());
            assertEquals(1L, f.engine.metrics().settleCount());
            assertEquals(0L, f.engine.metrics().unfreezeCount());
            assertEquals(1L, f.engine.metrics().conflictingTerminalEvents());
        } finally { f.close(); }
    }

    @Test
    void lateFillAfterCancelDoesNotSettle() {
        Fixture f = new Fixture();
        try {
            f.engine.process(e(OrderEventType.ORDER_CREATED));
            f.engine.process(e(OrderEventType.ORDER_CANCELLED));
            f.engine.process(e(OrderEventType.MATCH_FILLED));
            assertEquals(OrderStatus.CANCELED, f.engine.order(1L).status());
            assertEquals(0L, f.engine.metrics().settleCount());
            assertEquals(1L, f.engine.metrics().unfreezeCount());
            assertEquals(1L, f.engine.metrics().conflictingTerminalEvents());
        } finally { f.close(); }
    }

    private static OrderEvent e(OrderEventType type) {
        return new OrderEvent(1L, 1L, 100L, 1L, type);
    }

    private static final class Fixture implements AutoCloseable {
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        private final ApprovalService approvals = new ApprovalService(1, 8);
        private final OrderEngine engine;
        private Fixture() {
            ledger.createAccount(1L, 1000L);
            engine = new OrderEngine(ledger, new RiskPipeline(), new ManualClock(1L), approvals,
                    event -> ApprovalDecision.PASS);
        }
        @Override public void close() { engine.close(); }
    }
}
