package com.cex.core.order;

import com.cex.core.account.Account;
import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.RiskPipeline;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OutOfOrderStateMachineTest {
    @Test
    void filledBeforeCreateConvergesToFilled() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(9L, 1L, 100L, OrderEventType.MATCH_FILLED));
            fixture.engine.process(event(9L, 1L, 100L, OrderEventType.ORDER_CREATED));

            assertEquals(OrderStatus.FILLED, fixture.engine.order(9L).status());
            assertEquals(900L, fixture.account().available());
            assertEquals(0L, fixture.account().frozen());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().settleCount());
        } finally {
            fixture.close();
        }
    }

    @Test
    void cancelledBeforeCreateConvergesToCanceled() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(10L, 1L, 100L, OrderEventType.ORDER_CANCELLED));
            fixture.engine.process(event(10L, 1L, 100L, OrderEventType.ORDER_CREATED));

            assertEquals(OrderStatus.CANCELED, fixture.engine.order(10L).status());
            assertEquals(1000L, fixture.account().available());
            assertEquals(0L, fixture.account().frozen());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().unfreezeCount());
        } finally {
            fixture.close();
        }
    }

    @Test
    void duplicateEventsStillReconcileExactlyOnce() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            IntStream.range(0, 20).forEach(i -> fixture.engine.process(
                    event(11L, 1L, 100L, OrderEventType.MATCH_FILLED)));
            IntStream.range(0, 10).forEach(i -> fixture.engine.process(
                    event(11L, 1L, 100L, OrderEventType.ORDER_CREATED)));

            assertEquals(OrderStatus.FILLED, fixture.engine.order(11L).status());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().settleCount());
            assertTrue(fixture.engine.metrics().duplicateEvents() >= 28L);
        } finally {
            fixture.close();
        }
    }

    @Test
    void sameOrderConcurrentDuplicatesAndInterruptComplete() throws Exception {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            Thread.currentThread().interrupt();
            fixture.engine.process(event(12L, 1L, 100L, OrderEventType.MATCH_FILLED));
            fixture.engine.process(event(12L, 1L, 100L, OrderEventType.ORDER_CREATED));
            assertTrue(Thread.interrupted());
            CountDownLatch start = new CountDownLatch(1);
            Thread[] threads = new Thread[32];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        start.await();
                        fixture.engine.process(event(12L, 1L, 100L, OrderEventType.MATCH_FILLED));
                        fixture.engine.process(event(12L, 1L, 100L, OrderEventType.ORDER_CREATED));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        fail(e);
                    }
                });
                threads[i].start();
            }
            start.countDown();
            for (Thread thread : threads) {
                thread.join(5000L);
                assertFalse(thread.isAlive());
            }
            assertEquals(OrderStatus.FILLED, fixture.engine.order(12L).status());
            assertEquals(1L, fixture.engine.metrics().settleCount());
        } finally {
            Thread.interrupted();
            fixture.close();
        }
    }

    @Test
    void metadataMismatchIsRejected() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(13L, 1L, 100L, OrderEventType.ORDER_CREATED));
            assertThrows(OrderMetadataMismatchException.class,
                    () -> fixture.engine.process(event(13L, 2L, 100L, OrderEventType.MATCH_FILLED)));
            assertEquals(1L, fixture.engine.metrics().metadataConflictEvents());
        } finally {
            fixture.close();
        }
    }

    private static OrderEvent event(long orderId, long userId, long amount, OrderEventType type) {
        return new OrderEvent(orderId, userId, amount, 1L, type);
    }

    private static final class Fixture implements AutoCloseable {
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        private final ApprovalService approvals = new ApprovalService(1, 16);
        private final OrderEngine engine;

        private Fixture(com.cex.core.risk.ApprovalPolicy policy) {
            ledger.createAccount(1L, 1000L);
            engine = new OrderEngine(ledger, new RiskPipeline(), new com.cex.core.risk.ManualClock(1L), approvals, policy);
        }

        private Account account() { return ledger.getRequiredAccount(1L); }
        @Override public void close() { engine.close(); }
    }
}
