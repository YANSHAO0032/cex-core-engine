package com.cex.core.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.metrics.GcMetrics;
import com.cex.core.metrics.LatencyHistogram;
import com.cex.core.order.AssetId;
import com.cex.core.order.CancelConfirmation;
import com.cex.core.order.CancelRequest;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * CEX 强类型内存订单核心性能测试，分别测量成交重复热路径和代表性生命周期。
 *
 * <p>保留原有线程数、预热量、500,000 次热路径采样和 300,000 个生命周期主订单；
 * 共享卖方订单只负责将旧单边成交机械映射为权威双边成交。</p>
 *
 * @note 复用不可变成交数组与每 worker 卖方订单，减少 GC 并适配 {@code -Xmx256m}。
 * @note 撤单终态业务操作内部按规范依次发送请求和确认，延迟样本覆盖完整强类型撤单边界。
 */
class PerformanceTest {
    /** 性能测量使用的并发工作线程数量。 */
    private static final int THREADS = 16;
    /** 重复热路径正式测量前的预热操作次数。 */
    private static final int WARMUP = 50_000;
    /** 重复热路径正式采样的成交处理次数。 */
    private static final int MEASUREMENTS = 500_000;
    /** 代表性生命周期基准创建的独立主订单数量。 */
    private static final int LIFECYCLE_ORDERS = 300_000;
    /** 代表性生命周期复用的买方用户数量。 */
    private static final int LIFECYCLE_USERS = 4_096;
    /** 生命周期共享卖单标识起点。 */
    private static final long SELL_ORDER_BASE = 2_000_000L;
    /** 生命周期共享卖方用户标识起点。 */
    private static final long SELLER_USER_BASE = 10_000L;
    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 性能场景使用的交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);

    /**
     * 单独测量已结算成交重复投递的幂等热路径吞吐和延迟。
     *
     * @throws Exception 工作线程等待、结果获取或测试资源关闭失败时抛出
     */
    @Test
    void duplicateFilledEventHotPathIsMeasuredSeparately() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        long sellerUserId = 100L;
        long sellerOrderId = 100L;
        ledger.createBalance(sellerUserId, BTC, THREADS);
        ledger.createBalance(sellerUserId, USDT, 0L);
        OrderEngine engine = new OrderEngine(ledger);
        TradeExecution[] duplicateTrades = new TradeExecution[THREADS];
        try {
            engine.submit(new OrderSubmission(
                    sellerOrderId, sellerUserId, OrderSide.SELL, BTC_USDT,
                    THREADS, THREADS, THREADS, 1L, 1L));
            for (int index = 0; index < THREADS; index++) {
                long buyerUserId = index + 1L;
                long buyerOrderId = index + 1L;
                ledger.createBalance(buyerUserId, BTC, 0L);
                ledger.createBalance(buyerUserId, USDT, 2_000_000L);
                engine.submit(new OrderSubmission(
                        buyerOrderId, buyerUserId, OrderSide.BUY, BTC_USDT,
                        1L, 1L, 1L, 1L, 1L));
                TradeExecution execution = new TradeExecution(
                        index + 1L,
                        buyerOrderId,
                        sellerOrderId,
                        BTC_USDT,
                        1L,
                        1L,
                        2L,
                        index + 2L,
                        2L);
                engine.onTrade(execution);
                duplicateTrades[index] = execution;
            }
            for (int index = 0; index < WARMUP; index++) {
                engine.onTrade(duplicateTrades[index & (THREADS - 1)]);
            }

            LatencyHistogram histogram = new LatencyHistogram();
            GcMetrics gcBefore = GcMetrics.snapshot();
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            List<Future<?>> futures = new ArrayList<>(THREADS);
            long started = System.nanoTime();
            try {
                for (int thread = 0; thread < THREADS; thread++) {
                    final int threadIndex = thread;
                    futures.add(executor.submit(() -> {
                        try {
                            start.await();
                            int perThread = MEASUREMENTS / THREADS;
                            for (int index = 0; index < perThread; index++) {
                                TradeExecution execution = duplicateTrades[
                                        (threadIndex + index) & (THREADS - 1)];
                                long operationStart = System.nanoTime();
                                engine.onTrade(execution);
                                histogram.record(
                                        System.nanoTime() - operationStart);
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
            } finally {
                executor.shutdownNow();
            }
            long elapsed = System.nanoTime() - started;
            GcMetrics gcAfter = GcMetrics.snapshot();
            long count = histogram.count();
            double tps = count / (elapsed / 1_000_000_000.0);
            double averageMillis = histogram.averageMicros() / 1_000.0;
            long maxMemory = Runtime.getRuntime().maxMemory();
            long usedMemory = Runtime.getRuntime().totalMemory()
                    - Runtime.getRuntime().freeMemory();

            assertEquals(MEASUREMENTS, count);
            assertEquals(THREADS, engine.metrics().settledTradeCount());
            assertTrue(ledger.allAssetInvariantsHold());
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0,
                    () -> "average latency ms=" + averageMillis);
            printReport(
                    "CEX DUPLICATE IDEMPOTENCY HOT-PATH REPORT",
                    count, tps, histogram, maxMemory, usedMemory,
                    gcBefore, gcAfter);
        } finally {
            engine.close();
        }
    }

    /**
     * 验证代表性创建、成交、撤单及乱序生命周期满足吞吐、延迟、内存和 GC 目标。
     *
     * @throws Exception 工作线程等待、结果获取或测试资源关闭失败时抛出
     */
    @Test
    void representativeLifecycleMeetsLatencyThroughputAndMemoryTargets()
            throws Exception {
        AccountLedger ledger = lifecycleLedger();
        OrderEngine engine = new OrderEngine(ledger);
        long[] fillCounts = lifecycleFillCounts();
        submitLifecycleSellers(engine, fillCounts);
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            LatencyHistogram histogram = new LatencyHistogram();
            GcMetrics gcBefore = GcMetrics.snapshot();
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>(THREADS);
            long started = System.nanoTime();
            for (int thread = 0; thread < THREADS; thread++) {
                final int threadIndex = thread;
                futures.add(executor.submit(() -> runLifecycleWorker(
                        threadIndex, start, engine, histogram)));
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
            double tps = operations / (elapsed / 1_000_000_000.0);
            double averageMillis = histogram.averageMicros() / 1_000.0;
            long maxMemory = Runtime.getRuntime().maxMemory();
            long usedMemory = Runtime.getRuntime().totalMemory()
                    - Runtime.getRuntime().freeMemory();
            long oldGcCount = gcAfter.oldCollectorCount()
                    - gcBefore.oldCollectorCount();

            long filled = 0L;
            long canceled = 0L;
            for (int index = 0; index < LIFECYCLE_ORDERS; index++) {
                OrderStatus status = engine.order(1_000_000L + index).status();
                if (status == OrderStatus.FILLED) {
                    filled++;
                } else if (status == OrderStatus.CANCELED) {
                    canceled++;
                }
            }

            assertEquals(LIFECYCLE_ORDERS * 2L, operations);
            assertEquals(LIFECYCLE_ORDERS / 2L, filled);
            assertEquals(LIFECYCLE_ORDERS / 2L, canceled);
            assertEquals(filled, engine.metrics().settledTradeCount());
            assertEquals(0, engine.pendingTradeCount());
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0,
                    () -> "average latency ms=" + averageMillis);
            assertTrue(maxMemory <= 256L * 1024L * 1024L,
                    () -> "max heap bytes=" + maxMemory);
            assertTrue(oldGcCount <= 1L,
                    () -> "old/full GC count=" + oldGcCount);
            assertTrue(ledger.allAssetInvariantsHold());
            assertTrue(ledger.allBalancesNonNegative());

            printReport(
                    "CEX REPRESENTATIVE LIFECYCLE REPORT",
                    operations, tps, histogram, maxMemory, usedMemory,
                    gcBefore, gcAfter);
            System.out.println("Filled orders:           " + filled);
            System.out.println("Canceled orders:         " + canceled);
            System.out.println("Invariant result:        PASS");
        } finally {
            executor.shutdownNow();
            engine.close();
        }
    }

    /**
     * 执行一个 worker 的主订单生命周期分片。
     *
     * @param workerIndex worker 索引
     * @param start 统一启动闩锁
     * @param engine 强类型订单引擎
     * @param histogram 并发延迟直方图
     */
    private static void runLifecycleWorker(
            int workerIndex,
            CountDownLatch start,
            OrderEngine engine,
            LatencyHistogram histogram) {
        try {
            start.await();
            long sellerSequence = 2L;
            for (int index = workerIndex;
                    index < LIFECYCLE_ORDERS; index += THREADS) {
                long orderId = 1_000_000L + index;
                OrderSubmission submission = lifecycleSubmission(orderId, index);
                boolean filled = (index & 1) == 0;
                boolean inOrder = (index & 2) == 0;
                Runnable terminal;
                if (filled) {
                    TradeExecution execution = lifecycleExecution(
                            orderId, workerIndex, sellerSequence++);
                    terminal = () -> engine.onTrade(execution);
                } else {
                    terminal = () -> cancelLifecycle(engine, orderId);
                }
                Runnable create = () -> engine.submit(submission);
                Runnable first = inOrder ? create : terminal;
                Runnable second = inOrder ? terminal : create;

                long firstStarted = System.nanoTime();
                first.run();
                histogram.record(System.nanoTime() - firstStarted);
                long secondStarted = System.nanoTime();
                second.run();
                if (!filled && !inOrder) {
                    engine.requestCancel(cancelRequest(orderId));
                }
                histogram.record(System.nanoTime() - secondStarted);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    /**
     * 执行一个强类型撤单终态业务操作。
     *
     * @param engine 强类型订单引擎
     * @param orderId 待撤买单标识
     */
    private static void cancelLifecycle(OrderEngine engine, long orderId) {
        if (engine.order(orderId) == null) {
            engine.onCancelConfirmed(cancelConfirmation(orderId));
            return;
        }
        engine.requestCancel(cancelRequest(orderId));
        engine.onCancelConfirmed(cancelConfirmation(orderId));
    }

    /**
     * 创建代表性生命周期买单。
     *
     * @param orderId 主订单标识
     * @param index 从零开始的订单索引
     * @return 一个资金单位的买单提交
     */
    private static OrderSubmission lifecycleSubmission(long orderId, int index) {
        long userId = (index & (LIFECYCLE_USERS - 1)) + 1L;
        return new OrderSubmission(
                orderId, userId, OrderSide.BUY, BTC_USDT,
                1L, 1L, 1L, 1L, 1L);
    }

    /**
     * 创建代表性生命周期全量成交。
     *
     * @param orderId 主买单标识并作为成交标识
     * @param workerIndex 共享卖单 worker 索引
     * @param sellerSequence 卖单下一权威序号
     * @return 权威双边成交
     */
    private static TradeExecution lifecycleExecution(
            long orderId, int workerIndex, long sellerSequence) {
        return new TradeExecution(
                orderId,
                orderId,
                sellerOrderId(workerIndex),
                BTC_USDT,
                1L,
                1L,
                2L,
                sellerSequence,
                2L);
    }

    /**
     * 创建稳定撤单请求。
     *
     * @param orderId 待撤订单标识
     * @return 与订单一一对应的撤单请求
     */
    private static CancelRequest cancelRequest(long orderId) {
        return new CancelRequest(orderId, orderId, 2L);
    }

    /**
     * 创建下一权威序号的撤单确认。
     *
     * @param orderId 待撤订单标识
     * @return 序号二的撤单确认
     */
    private static CancelConfirmation cancelConfirmation(long orderId) {
        return new CancelConfirmation(orderId, orderId, 2L, 3L);
    }

    /**
     * 创建生命周期复用用户和共享卖方的账本。
     *
     * @return 初始化完成的多资产账本
     */
    private static AccountLedger lifecycleLedger() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= LIFECYCLE_USERS; userId++) {
            ledger.createBalance(userId, BTC, 0L);
            ledger.createBalance(userId, USDT, 1_000L);
        }
        long[] fillCounts = lifecycleFillCounts();
        for (int worker = 0; worker < THREADS; worker++) {
            long sellerUserId = sellerUserId(worker);
            ledger.createBalance(sellerUserId, BTC, fillCounts[worker]);
            ledger.createBalance(sellerUserId, USDT, 0L);
        }
        return ledger;
    }

    /**
     * 提交每个 worker 的共享卖单。
     *
     * @param engine 强类型订单引擎
     * @param fillCounts 每个 worker 的成交订单数量
     */
    private static void submitLifecycleSellers(
            OrderEngine engine, long[] fillCounts) {
        for (int worker = 0; worker < THREADS; worker++) {
            long quantity = fillCounts[worker];
            if (quantity == 0L) {
                continue;
            }
            engine.submit(new OrderSubmission(
                    sellerOrderId(worker),
                    sellerUserId(worker),
                    OrderSide.SELL,
                    BTC_USDT,
                    quantity,
                    quantity,
                    quantity,
                    1L,
                    1L));
        }
    }

    /**
     * 统计每个 worker 的成交型主订单数量。
     *
     * @return 共享卖单分别需要的基础资产数量
     */
    private static long[] lifecycleFillCounts() {
        long[] counts = new long[THREADS];
        for (int worker = 0; worker < THREADS; worker++) {
            for (int index = worker;
                    index < LIFECYCLE_ORDERS; index += THREADS) {
                if ((index & 1) == 0) {
                    counts[worker]++;
                }
            }
        }
        return counts;
    }

    /**
     * 返回 worker 共享卖单标识。
     *
     * @param workerIndex worker 索引
     * @return 不与主订单重叠的卖单标识
     */
    private static long sellerOrderId(int workerIndex) {
        return SELL_ORDER_BASE + workerIndex;
    }

    /**
     * 返回 worker 共享卖方用户标识。
     *
     * @param workerIndex worker 索引
     * @return 不与买方用户重叠的卖方用户标识
     */
    private static long sellerUserId(int workerIndex) {
        return SELLER_USER_BASE + workerIndex;
    }

    /**
     * 输出统一的 TPS、延迟、堆和 GC 报告。
     *
     * @param title 报告标题
     * @param operations 正式采样操作数
     * @param tps 每秒处理操作数
     * @param histogram 延迟直方图
     * @param maxMemory JVM 最大堆字节数
     * @param usedMemory 当前已用堆字节数
     * @param gcBefore 采样前 GC 快照
     * @param gcAfter 采样后 GC 快照
     */
    private static void printReport(
            String title,
            long operations,
            double tps,
            LatencyHistogram histogram,
            long maxMemory,
            long usedMemory,
            GcMetrics gcBefore,
            GcMetrics gcAfter) {
        System.out.println("====================================");
        System.out.println(title);
        System.out.println("====================================");
        System.out.println("Measurement Operations: " + operations);
        System.out.println("Threads:                 " + THREADS);
        System.out.printf("TPS:                     %.2f%n", tps);
        System.out.printf("Average latency:         %.2f us%n",
                histogram.averageMicros());
        System.out.println("P50:                     "
                + histogram.p50Nanos() / 1_000.0 + " us");
        System.out.println("P95:                     "
                + histogram.p95Nanos() / 1_000.0 + " us");
        System.out.println("P99:                     "
                + histogram.p99Nanos() / 1_000.0 + " us");
        System.out.println("MAX:                     "
                + histogram.maxNanos() / 1_000.0 + " us");
        System.out.println("Heap Max:                "
                + maxMemory / (1024L * 1024L) + " MB");
        System.out.println("Heap Used:               "
                + usedMemory / (1024L * 1024L) + " MB");
        System.out.println("GC Count:                "
                + (gcAfter.collectionCount() - gcBefore.collectionCount()));
        System.out.println("GC Time:                 "
                + (gcAfter.collectionTimeMillis()
                - gcBefore.collectionTimeMillis()) + " ms");
        System.out.println("Old/Full GC Count:       "
                + (gcAfter.oldCollectorCount() - gcBefore.oldCollectorCount()));
        System.out.println("Old/Full GC Time:        "
                + (gcAfter.oldCollectorTimeMillis()
                - gcBefore.oldCollectorTimeMillis()) + " ms");
        System.out.println("====================================");
    }
}
