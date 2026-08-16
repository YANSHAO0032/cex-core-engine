package com.cex.core.performance;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.metrics.GcMetrics;
import com.cex.core.metrics.LatencyHistogram;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.ManualClock;
import com.cex.core.risk.RiskPipeline;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PerformanceTest {
    private static final int THREADS = 16;
    private static final int WARMUP = 50_000;
    private static final int MEASUREMENTS = 500_000;
    private static final int LIFECYCLE_ORDERS = 300_000;
    private static final int LIFECYCLE_USERS = 4_096;

    @Test
    void duplicateFilledEventHotPathIsMeasuredSeparately() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ApprovalService approvals = new ApprovalService(1, 16);
        OrderEngine engine = new OrderEngine(ledger, new RiskPipeline(), new ManualClock(1L), approvals,
                event -> ApprovalDecision.PASS);
        OrderEvent[] duplicateFills = new OrderEvent[THREADS];
        try {
            for (int i = 0; i < THREADS; i++) {
                long userId = i + 1L;
                ledger.createAccount(userId, 2_000_000L);
                long orderId = i + 1L;
                engine.process(new OrderEvent(orderId, userId, 1L, 1L, OrderEventType.ORDER_CREATED));
                engine.process(new OrderEvent(orderId, userId, 1L, 1L, OrderEventType.MATCH_FILLED));
                duplicateFills[i] = new OrderEvent(orderId, userId, 1L, 1L, OrderEventType.MATCH_FILLED);
            }
            for (int i = 0; i < WARMUP; i++) {
                engine.process(duplicateFills[i & (THREADS - 1)]);
            }

            LatencyHistogram histogram = new LatencyHistogram();
            GcMetrics gcBefore = GcMetrics.snapshot();
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            long started = System.nanoTime();
            for (int thread = 0; thread < THREADS; thread++) {
                final int threadIndex = thread;
                executor.submit(() -> {
                    try {
                        start.await();
                        int perThread = MEASUREMENTS / THREADS;
                        for (int i = 0; i < perThread; i++) {
                            OrderEvent event = duplicateFills[(threadIndex + i) & (THREADS - 1)];
                            long operationStart = System.nanoTime();
                            engine.process(event);
                            histogram.record(System.nanoTime() - operationStart);
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                });
            }
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(60L, TimeUnit.SECONDS));
            long elapsed = System.nanoTime() - started;
            GcMetrics gcAfter = GcMetrics.snapshot();
            long count = histogram.count();
            double tps = count / (elapsed / 1_000_000_000.0);
            double averageMillis = histogram.averageMicros() / 1_000.0;
            long maxMemory = Runtime.getRuntime().maxMemory();
            long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

            assertEquals(MEASUREMENTS, count);
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0, () -> "average latency ms=" + averageMillis);
            System.out.println("====================================");
            System.out.println("CEX DUPLICATE IDEMPOTENCY HOT-PATH REPORT");
            System.out.println("====================================");
            System.out.println("Measurement Operations: " + count);
            System.out.println("Threads:                " + THREADS);
            System.out.printf("TPS:                    %.2f%n", tps);
            System.out.printf("Average latency:        %.2f us%n", histogram.averageMicros());
            System.out.println("P50:                    " + histogram.p50Nanos() / 1_000.0 + " us");
            System.out.println("P95:                    " + histogram.p95Nanos() / 1_000.0 + " us");
            System.out.println("P99:                    " + histogram.p99Nanos() / 1_000.0 + " us");
            System.out.println("MAX:                    " + histogram.maxNanos() / 1_000.0 + " us");
            System.out.println("Heap Max:               " + maxMemory / (1024L * 1024L) + " MB");
            System.out.println("Heap Used:              " + usedMemory / (1024L * 1024L) + " MB");
            System.out.println("GC Count:               " + (gcAfter.collectionCount() - gcBefore.collectionCount()));
            System.out.println("GC Time:                " + (gcAfter.collectionTimeMillis() - gcBefore.collectionTimeMillis()) + " ms");
            System.out.println("Old/Full GC Count:      " + (gcAfter.oldCollectorCount() - gcBefore.oldCollectorCount()));
            System.out.println("Old/Full GC Time:       " + (gcAfter.oldCollectorTimeMillis() - gcBefore.oldCollectorTimeMillis()) + " ms");
            System.out.println("====================================");
        } finally {
            engine.close();
        }
    }

    @Test
    void representativeLifecycleMeetsLatencyThroughputAndMemoryTargets() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= LIFECYCLE_USERS; userId++) {
            ledger.createAccount(userId, 1_000L);
        }
        ApprovalService approvals = new ApprovalService(1, 16);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(),
                new ManualClock(1L),
                approvals,
                event -> ApprovalDecision.PASS);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            LatencyHistogram histogram = new LatencyHistogram();
            GcMetrics gcBefore = GcMetrics.snapshot();
            long transitionBefore = engine.metrics().stateTransitions();
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>(THREADS);
            long started = System.nanoTime();
            for (int thread = 0; thread < THREADS; thread++) {
                final int threadIndex = thread;
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        for (int index = threadIndex; index < LIFECYCLE_ORDERS; index += THREADS) {
                            long orderId = 1_000_000L + index;
                            long userId = (index & (LIFECYCLE_USERS - 1)) + 1L;
                            OrderEvent created = lifecycleEvent(
                                    orderId, userId, OrderEventType.ORDER_CREATED);
                            OrderEvent terminal = lifecycleEvent(
                                    orderId,
                                    userId,
                                    (index & 1) == 0
                                            ? OrderEventType.MATCH_FILLED
                                            : OrderEventType.ORDER_CANCELLED);
                            OrderEvent first = (index & 2) == 0 ? created : terminal;
                            OrderEvent second = (index & 2) == 0 ? terminal : created;
                            long firstStarted = System.nanoTime();
                            engine.process(first);
                            histogram.record(System.nanoTime() - firstStarted);
                            long secondStarted = System.nanoTime();
                            engine.process(second);
                            histogram.record(System.nanoTime() - secondStarted);
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }));
            }
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(60L, TimeUnit.SECONDS));
            for (Future<?> future : futures) {
                future.get();
            }
            long elapsed = System.nanoTime() - started;
            GcMetrics gcAfter = GcMetrics.snapshot();
            long operations = histogram.count();
            long transitions = engine.metrics().stateTransitions() - transitionBefore;
            long expectedTransitions = LIFECYCLE_ORDERS * 7L / 4L;
            double tps = operations / (elapsed / 1_000_000_000.0);
            double averageMillis = histogram.averageMicros() / 1_000.0;

            assertEquals(LIFECYCLE_ORDERS * 2L, operations);
            assertEquals(expectedTransitions, transitions);
            assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().settleCount());
            assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().unfreezeCount());
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0, () -> "average latency ms=" + averageMillis);
            assertTrue(ledger.invariantHolds());

            long maxMemory = Runtime.getRuntime().maxMemory();
            long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            System.out.println("====================================");
            System.out.println("CEX REPRESENTATIVE LIFECYCLE REPORT");
            System.out.println("====================================");
            System.out.println("Measurement Operations: " + operations);
            System.out.println("State Transitions:       " + transitions);
            System.out.println("Threads:                 " + THREADS);
            System.out.printf("TPS:                     %.2f%n", tps);
            System.out.printf("Average latency:         %.2f us%n", histogram.averageMicros());
            System.out.println("P50:                     " + histogram.p50Nanos() / 1_000.0 + " us");
            System.out.println("P95:                     " + histogram.p95Nanos() / 1_000.0 + " us");
            System.out.println("P99:                     " + histogram.p99Nanos() / 1_000.0 + " us");
            System.out.println("MAX:                     " + histogram.maxNanos() / 1_000.0 + " us");
            System.out.println("Freeze count:            " + engine.metrics().freezeCount());
            System.out.println("Settle count:            " + engine.metrics().settleCount());
            System.out.println("Unfreeze count:          " + engine.metrics().unfreezeCount());
            System.out.println("Heap Max:                " + maxMemory / (1024L * 1024L) + " MB");
            System.out.println("Heap Used:               " + usedMemory / (1024L * 1024L) + " MB");
            System.out.println("GC Count:                "
                    + (gcAfter.collectionCount() - gcBefore.collectionCount()));
            System.out.println("GC Time:                 "
                    + (gcAfter.collectionTimeMillis() - gcBefore.collectionTimeMillis()) + " ms");
            System.out.println("Old/Full GC Count:       "
                    + (gcAfter.oldCollectorCount() - gcBefore.oldCollectorCount()));
            System.out.println("Old/Full GC Time:        "
                    + (gcAfter.oldCollectorTimeMillis() - gcBefore.oldCollectorTimeMillis()) + " ms");
            System.out.println("Invariant result:        PASS");
            System.out.println("====================================");
        } finally {
            executor.shutdownNow();
            engine.close();
        }
    }

    private static OrderEvent lifecycleEvent(long orderId, long userId, OrderEventType type) {
        return new OrderEvent(orderId, userId, 1L, 1L, type);
    }
}
