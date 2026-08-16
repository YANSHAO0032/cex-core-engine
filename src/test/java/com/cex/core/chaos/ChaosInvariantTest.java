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
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChaosInvariantTest {
    private static final long CHAOS_SEED = Long.getLong("CHAOS_SEED", 20260816L);
    private static final int WORKERS = 16;
    private static final int ORDERS = 300_000;
    private static final long EVENT_TIME_BASE = Math.floorMod(CHAOS_SEED, Long.MAX_VALUE - ORDERS);
    private static final Scenario[] SCENARIO_CYCLE = shuffledScenarioCycle();

    @Test
    void seededLifecycleChaosConvergesWithoutInvariantFailureOrDeadlock() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= ORDERS; userId++) {
            ledger.createAccount(userId, 2L);
        }
        ApprovalService approvals = new ApprovalService(1, 32);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(),
                new ManualClock(EVENT_TIME_BASE),
                approvals,
                event -> ApprovalDecision.PASS);
        InvariantChecker checker = new InvariantChecker(ledger);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean invariantFailure = new AtomicBoolean(false);
        AtomicReference<Throwable> watchdogFailure = new AtomicReference<>();
        LongAdder yieldInjections = new LongAdder();
        LongAdder parkInjections = new LongAdder();
        LongAdder interruptInjections = new LongAdder();
        Thread watchdog = new Thread(() -> {
            try {
                while (running.get()) {
                    if (!checker.check()) {
                        invariantFailure.set(true);
                        return;
                    }
                    LockSupport.parkNanos(1_000_000L);
                }
            } catch (Throwable failure) {
                watchdogFailure.set(failure);
            }
        }, "asset-invariant-watchdog");
        ExecutorService workers = Executors.newFixedThreadPool(WORKERS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            watchdog.start();
            List<Future<?>> futures = new ArrayList<>(WORKERS);
            for (int worker = 0; worker < WORKERS; worker++) {
                final int workerIndex = worker;
                futures.add(workers.submit(() -> runWorker(
                        workerIndex,
                        start,
                        engine,
                        checker,
                        invariantFailure,
                        yieldInjections,
                        parkInjections,
                        interruptInjections)));
            }
            start.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(90L, TimeUnit.SECONDS),
                    "worker termination timeout; seed=" + CHAOS_SEED);
            for (Future<?> future : futures) {
                future.get();
            }

            assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads(),
                    "deadlock detected; seed=" + CHAOS_SEED);
            running.set(false);
            watchdog.join(5_000L);
            assertFalse(watchdog.isAlive(), "watchdog did not terminate");
            assertNull(watchdogFailure.get(), "watchdog failed");
            assertFalse(invariantFailure.get(), "invariant failure; seed=" + CHAOS_SEED);
            assertTrue(checker.check());
            assertEquals(0L, checker.failureCount());

            long expectedFilled = 0L;
            long expectedCanceled = 0L;
            for (int orderIndex = 0; orderIndex < ORDERS; orderIndex++) {
                long orderId = orderIndex + 1L;
                Scenario scenario = scenarioFor(orderIndex);
                assertEquals(scenario.expectedStatus, engine.order(orderId).status(),
                        "orderId=" + orderId + ", scenario=" + scenario);
                if (scenario.expectedStatus == OrderStatus.FILLED) {
                    expectedFilled++;
                } else {
                    expectedCanceled++;
                }
            }

            assertEquals(180_000L, expectedFilled);
            assertEquals(120_000L, expectedCanceled);
            assertEquals(expectedFilled, engine.metrics().settleCount());
            assertEquals(expectedCanceled, engine.metrics().unfreezeCount());
            assertEquals(60_000L, engine.metrics().conflictingTerminalEvents());
            assertEquals(540_000L, engine.metrics().stateTransitions());
            assertEquals(ORDERS, engine.metrics().freezeCount());
            assertTrue(engine.metrics().duplicateEvents() > 0L);
            assertTrue(engine.metrics().outOfOrderEvents() > 0L);
            assertTrue(yieldInjections.sum() > 0L);
            assertTrue(parkInjections.sum() > 0L);
            assertTrue(interruptInjections.sum() > 0L);
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
            System.out.println("Terminal conflicts: " + engine.metrics().conflictingTerminalEvents());
            System.out.println("Expected filled: " + expectedFilled);
            System.out.println("Expected canceled: " + expectedCanceled);
            System.out.println("Yield injections: " + yieldInjections.sum());
            System.out.println("Park injections: " + parkInjections.sum());
            System.out.println("Interrupt injections: " + interruptInjections.sum());
            System.out.println("Invariant snapshots: " + checker.snapshotCount());
            System.out.println("Invariant failures: " + checker.failureCount());
            System.out.println("Asset delta: " + (ledger.currentTotalAsset() - ledger.initialTotalAsset()));
            System.out.println("Deadlock check: PASS");
            System.out.println("Termination check: PASS");
        } finally {
            running.set(false);
            workers.shutdownNow();
            watchdog.join(5_000L);
            engine.close();
        }
    }

    private static void runWorker(
            int workerIndex,
            CountDownLatch start,
            OrderEngine engine,
            InvariantChecker checker,
            AtomicBoolean invariantFailure,
            LongAdder yieldInjections,
            LongAdder parkInjections,
            LongAdder interruptInjections) {
        try {
            start.await();
            SplittableRandom random = new SplittableRandom(CHAOS_SEED + workerIndex);
            for (int orderIndex = workerIndex; orderIndex < ORDERS; orderIndex += WORKERS) {
                long orderId = orderIndex + 1L;
                processScenario(engine, orderId, scenarioFor(orderIndex));
                if ((orderIndex & 255) == 0) {
                    switch (random.nextInt(3)) {
                        case 0 -> {
                            yieldInjections.increment();
                            Thread.yield();
                        }
                        case 1 -> {
                            parkInjections.increment();
                            LockSupport.parkNanos(random.nextLong(10_000L, 100_001L));
                        }
                        default -> {
                            interruptInjections.increment();
                            Thread.currentThread().interrupt();
                            engine.process(event(orderId, OrderEventType.ORDER_CREATED));
                            Thread.interrupted();
                        }
                    }
                }
                if ((orderIndex & 1023) == 0 && !checker.check()) {
                    invariantFailure.set(true);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("worker interrupted", interrupted);
        } finally {
            Thread.interrupted();
        }
    }

    private static void processScenario(OrderEngine engine, long orderId, Scenario scenario) {
        OrderEvent created = event(orderId, OrderEventType.ORDER_CREATED);
        OrderEvent filled = event(orderId, OrderEventType.MATCH_FILLED);
        OrderEvent cancelled = event(orderId, OrderEventType.ORDER_CANCELLED);
        switch (scenario) {
            case FILL_IN_ORDER -> {
                engine.process(created);
                engine.process(created);
                engine.process(filled);
            }
            case FILL_OUT_OF_ORDER -> {
                engine.process(filled);
                engine.process(filled);
                engine.process(created);
            }
            case CANCEL_IN_ORDER -> {
                engine.process(created);
                engine.process(cancelled);
                engine.process(cancelled);
            }
            case CANCEL_OUT_OF_ORDER -> {
                engine.process(cancelled);
                engine.process(cancelled);
                engine.process(created);
            }
            case CONFLICT_BEFORE_CREATE -> {
                engine.process(cancelled);
                engine.process(filled);
                engine.process(created);
            }
        }
    }

    private static OrderEvent event(long orderId, OrderEventType type) {
        return new OrderEvent(orderId, orderId, 1L, EVENT_TIME_BASE + orderId, type);
    }

    private static Scenario[] shuffledScenarioCycle() {
        Scenario[] scenarios = Scenario.values().clone();
        SplittableRandom random = new SplittableRandom(CHAOS_SEED);
        for (int index = scenarios.length - 1; index > 0; index--) {
            int swapIndex = random.nextInt(index + 1);
            Scenario current = scenarios[index];
            scenarios[index] = scenarios[swapIndex];
            scenarios[swapIndex] = current;
        }
        return scenarios;
    }

    private static Scenario scenarioFor(long orderIndex) {
        return SCENARIO_CYCLE[(int) (orderIndex % SCENARIO_CYCLE.length)];
    }

    private enum Scenario {
        FILL_IN_ORDER(OrderStatus.FILLED),
        FILL_OUT_OF_ORDER(OrderStatus.FILLED),
        CANCEL_IN_ORDER(OrderStatus.CANCELED),
        CANCEL_OUT_OF_ORDER(OrderStatus.CANCELED),
        CONFLICT_BEFORE_CREATE(OrderStatus.FILLED);

        private final OrderStatus expectedStatus;

        Scenario(OrderStatus expectedStatus) {
            this.expectedStatus = expectedStatus;
        }
    }
}
