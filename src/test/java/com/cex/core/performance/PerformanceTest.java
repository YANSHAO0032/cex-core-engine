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

/**
 * CEX 内存订单核心性能验收测试，分别测量重复幂等热路径和代表性订单生命周期。
 *
 * <p>测试以固定线程池并发调用线程安全的订单引擎，采集 TPS、延迟分布、堆占用及 GC 指标；
 * 结果仅代表进程内状态机测评，不包含网络、序列化、数据库或撮合订单簿成本。</p>
 *
 * @note 重复事件数组和固定容量直方图用于复用对象、减少 GC，以适配 {@code -Xmx256m}；
 *       生命周期场景同时校验冻结、结算、解冻次数和总资产不变量，防止以跳过业务副作用换取虚假吞吐。
 */
class PerformanceTest {
    /** 性能测量使用的并发工作线程数量 */
    private static final int THREADS = 16;

    /** 重复幂等热路径正式测量前的预热操作次数 */
    private static final int WARMUP = 50_000;

    /** 重复幂等热路径正式采样的事件处理次数 */
    private static final int MEASUREMENTS = 500_000;

    /** 代表性生命周期基准创建的独立订单数量 */
    private static final int LIFECYCLE_ORDERS = 300_000;

    /** 代表性生命周期复用的用户数量，必须为二的幂以支持位运算映射 */
    private static final int LIFECYCLE_USERS = 4_096;

    /**
     * 单独测量已完成订单重复成交事件的幂等热路径吞吐和延迟。
     *
     * @throws Exception 工作线程等待、结果获取或测试资源关闭失败时抛出
     * @note 测量前先完成真实冻结和结算，再重复投递相同成交事实；Fact Bit 判重和 Effect Bit
     *       保证线程竞争下不会重复结算，预创建事件数组用于对象复用并降低 256MB 堆内 GC 压力。
     */
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
                // 复用固定事件对象，避免测量阶段为每次重复投递分配对象并放大 GC 噪声。
                duplicateFills[i] = new OrderEvent(orderId, userId, 1L, 1L, OrderEventType.MATCH_FILLED);
            }
            // 先触发 JIT 编译和热点路径稳定化，预热数据不计入正式延迟样本。
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
            // 统一释放启动闩锁，减少线程创建先后对吞吐测量的偏差。
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

    /**
     * 验证代表性创建、成交、撤单及乱序生命周期满足吞吐、延迟、内存和 GC 目标。
     *
     * @throws Exception 工作线程等待、结果获取或测试资源关闭失败时抛出
     * @note 订单按用户映射到条带锁并发执行，终态先于创建的事实先缓存，CREATE 到达后补偿冻结及结算/解冻；
     *       每个资金副作用由 Effect Bit 幂等保护，最终同时验证资金守恒、最大堆不超过 256MB 且 Full GC 受控。
     */
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
                            // 通过索引位交替构造顺序和乱序事件，确保性能样本覆盖后置补偿路径。
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
            long maxMemory = Runtime.getRuntime().maxMemory();
            long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long oldGcCount = gcAfter.oldCollectorCount() - gcBefore.oldCollectorCount();

            assertEquals(LIFECYCLE_ORDERS * 2L, operations);
            assertEquals(expectedTransitions, transitions);
            assertEquals(LIFECYCLE_ORDERS, engine.metrics().freezeCount());
            assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().settleCount());
            assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().unfreezeCount());
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0, () -> "average latency ms=" + averageMillis);
            // 堆上限和 Full GC 断言确保吞吐结果符合测评的 256MB 内存约束。
            assertTrue(maxMemory <= 256L * 1024L * 1024L, () -> "max heap bytes=" + maxMemory);
            assertTrue(oldGcCount <= 1L, () -> "old/full GC count=" + oldGcCount);
            assertTrue(ledger.invariantHolds());

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
            System.out.println("Old/Full GC Count:       " + oldGcCount);
            System.out.println("Old/Full GC Time:        "
                    + (gcAfter.oldCollectorTimeMillis() - gcBefore.oldCollectorTimeMillis()) + " ms");
            System.out.println("Invariant result:        PASS");
            System.out.println("====================================");
        } finally {
            executor.shutdownNow();
            engine.close();
        }
    }

    /**
     * 创建代表性生命周期使用的一个资金单位订单事件。
     *
     * @param orderId 全局唯一订单ID
     * @param userId 承担冻结、结算或解冻资金副作用的用户ID
     * @param type 生命周期中的创建、成交或撤单事件类型
     * @return 可直接提交订单引擎的不可变订单事件
     */
    private static OrderEvent lifecycleEvent(long orderId, long userId, OrderEventType type) {
        return new OrderEvent(orderId, userId, 1L, 1L, type);
    }
}
