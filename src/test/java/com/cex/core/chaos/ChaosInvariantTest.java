package com.cex.core.chaos;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.InvariantChecker;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.ManualClock;
import com.cex.core.risk.RiskPipeline;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChaosInvariantTest {
    private static final long CHAOS_SEED = Long.getLong("CHAOS_SEED", 20260816L);
    private static final int WORKERS = 16;
    private static final int ORDERS = 270_000;

    @Test
    void sixteenLongLivedWorkersConvergeWithWatchdogAndHalfMillionTransitions() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= ORDERS; userId++) {
            ledger.createAccount(userId, 2L);
        }
        ApprovalService approvals = new ApprovalService(1, 32);
        OrderEngine engine = new OrderEngine(ledger, new RiskPipeline(), new ManualClock(CHAOS_SEED), approvals,
                event -> ApprovalDecision.PASS);
        InvariantChecker checker = new InvariantChecker(ledger);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean invariantFailure = new AtomicBoolean(false);
        AtomicInteger operations = new AtomicInteger();
        Thread watchdog = new Thread(() -> {
            while (running.get()) {
                if (!checker.check()) {
                    invariantFailure.set(true);
                    return;
                }
                LockSupport.parkNanos(1_000_000L);
            }
        }, "asset-invariant-watchdog");
        ExecutorService workers = Executors.newFixedThreadPool(WORKERS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            watchdog.start();
            for (int worker = 0; worker < WORKERS; worker++) {
                final int workerIndex = worker;
                workers.submit(() -> {
                    try {
                        start.await();
                        for (int orderIndex = workerIndex; orderIndex < ORDERS; orderIndex += WORKERS) {
                            long orderId = orderIndex + 1L;
                            OrderEvent created = new OrderEvent(orderId, orderId, 1L, CHAOS_SEED + orderIndex,
                                    OrderEventType.ORDER_CREATED);
                            OrderEvent filled = new OrderEvent(orderId, orderId, 1L, CHAOS_SEED + orderIndex,
                                    OrderEventType.MATCH_FILLED);
                            if ((orderIndex % 20) == 0) {
                                engine.process(filled);
                                engine.process(filled);
                                engine.process(created);
                            } else {
                                engine.process(created);
                                engine.process(created);
                                engine.process(filled);
                            }
                            if ((orderIndex % 17) == 0) {
                                Thread.currentThread().interrupt();
                                engine.process(filled);
                                Thread.interrupted();
                            }
                            if ((orderIndex & 255) == 0) {
                                Thread.yield();
                                LockSupport.parkNanos(1L);
                            }
                            int op = operations.incrementAndGet();
                            if ((op & 1023) == 0 && !checker.check()) {
                                invariantFailure.set(true);
                            }
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("worker interrupted", interrupted);
                    }
                });
            }
            start.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(90L, TimeUnit.SECONDS), "worker termination timeout; seed=" + CHAOS_SEED);
            long[] deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
            assertNull(deadlocked, "deadlock detected; seed=" + CHAOS_SEED);
            running.set(false);
            watchdog.join(5000L);
            assertFalse(watchdog.isAlive(), "watchdog did not terminate");
            assertFalse(invariantFailure.get(), "invariant failure; seed=" + CHAOS_SEED);
            assertTrue(checker.check());
            assertEquals(0L, checker.failureCount());
            assertTrue(engine.metrics().stateTransitions() >= 500_000L,
                    () -> "stateTransitions=" + engine.metrics().stateTransitions());
            assertEquals(ORDERS, engine.metrics().settleCount());
            assertEquals(0L, engine.metrics().unfreezeCount());
            assertEquals(0L, engine.metrics().conflictingTerminalEvents());
            assertTrue(engine.metrics().duplicateEvents() > 0L);
            assertTrue(engine.metrics().outOfOrderEvents() > 0L);
            assertEquals(0L, ledger.currentTotalAsset() - ledger.initialTotalAsset());
            System.out.println("CHAOS SEED = " + CHAOS_SEED);
            System.out.println("Processed events: " + engine.metrics().processedEvents());
            System.out.println("Accepted facts: " + engine.metrics().acceptedFacts());
            System.out.println("Duplicate events: " + engine.metrics().duplicateEvents());
            System.out.println("Out-of-order events: " + engine.metrics().outOfOrderEvents());
            System.out.println("State transitions: " + engine.metrics().stateTransitions());
            System.out.println("Freeze count: " + engine.metrics().freezeCount());
            System.out.println("Settle count: " + engine.metrics().settleCount());
            System.out.println("Unfreeze count: " + engine.metrics().unfreezeCount());
            System.out.println("Invariant snapshots: " + checker.snapshotCount());
            System.out.println("Invariant failures: " + checker.failureCount());
            System.out.println("Deadlock check: PASS");
            System.out.println("Termination check: PASS");
        } finally {
            running.set(false);
            workers.shutdownNow();
            watchdog.join(5000L);
            engine.close();
        }
    }
}
