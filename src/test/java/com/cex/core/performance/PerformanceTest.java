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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PerformanceTest {
    private static final int THREADS = 16;
    private static final int WARMUP = 50_000;
    private static final int MEASUREMENTS = 500_000;

    @Test
    void coreStateMachineMeetsMeasuredLatencyAndThroughputTargets() throws Exception {
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
            System.out.println("CEX CORE PERFORMANCE REPORT");
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
}
