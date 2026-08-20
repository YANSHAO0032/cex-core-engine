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
 * <p>保留 16 线程、50,000 次预热和两个各至少 500,000 次的正式采样负载；
 * 代表性生命周期执行两次部分成交后全成或确认撤单，重复热路径复用不可变成交。</p>
 *
 * @note 重复热路径复用不可变成交数组；代表性生命周期为每个订单对独立提交买卖双方，减少 GC 并适配 {@code -Xmx256m}。
 * @note 撤单终态业务操作内部按规范依次发送请求和确认，延迟样本覆盖完整强类型撤单边界。
 */
class PerformanceTest {
    /** 创建性能验收测试实例。 */
    PerformanceTest() {
    }

    /** 性能测量使用的并发工作线程数量。 */
    private static final int THREADS = 16;
    /** 重复热路径正式测量前的预热操作次数。 */
    private static final int WARMUP = 50_000;
    /** 重复热路径正式采样的成交处理次数。 */
    private static final int MEASUREMENTS = 500_000;
    /** 代表性生命周期基准创建的独立买卖订单对数量。 */
    private static final int LIFECYCLE_PAIRS = 60_000;
    /** 代表性生命周期必须完成的正式采样调用次数。 */
    private static final long LIFECYCLE_MEASUREMENTS = 600_000L;
    /** 每个生命周期买单的原始基础资产数量。 */
    private static final long LIFECYCLE_BASE_QUANTITY = 3L;
    /** 每个生命周期买单冻结的报价资产数量。 */
    private static final long LIFECYCLE_QUOTE_RESERVE = 300L;
    /** 每次部分或最终成交交割的报价资产数量。 */
    private static final long LIFECYCLE_QUOTE_FILL = 90L;
    /** 代表性生命周期每一侧复用的用户数量。 */
    private static final int LIFECYCLE_USERS_PER_SIDE = 4_096;
    /** 生命周期卖单标识起点。 */
    private static final long SELL_ORDER_BASE = 2_000_000L;
    /** 生命周期卖方用户标识起点。 */
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
            long oldGcCount = gcAfter.oldCollectorCount()
                    - gcBefore.oldCollectorCount();

            assertEquals(MEASUREMENTS, count);
            assertEquals(THREADS, engine.metrics().settledTradeCount());
            assertEquals(WARMUP + MEASUREMENTS,
                    engine.metrics().duplicateTradeCount());
            assertEquals(0L, ledger.balance(sellerUserId, BTC).frozen());
            for (long buyerUserId = 1L; buyerUserId <= THREADS; buyerUserId++) {
                assertEquals(0L, ledger.balance(buyerUserId, USDT).frozen());
            }
            assertTrue(ledger.allAssetInvariantsHold());
            assertTrue(ledger.allBalancesNonNegative());
            assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
            assertTrue(averageMillis < 1.0,
                    () -> "average latency ms=" + averageMillis);
            assertTrue(maxMemory <= 256L * 1024L * 1024L,
                    () -> "max heap bytes=" + maxMemory);
            assertTrue(oldGcCount <= 1L,
                    () -> "old/full GC count=" + oldGcCount);
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
            for (int index = 0; index < LIFECYCLE_PAIRS; index++) {
                com.cex.core.order.OrderContext order =
                        engine.order(1_000_000L + index);
                com.cex.core.order.OrderContext counterparty =
                        engine.order(SELL_ORDER_BASE + index);
                OrderStatus status = order.status();
                if (status == OrderStatus.FILLED) {
                    filled++;
                } else if (status == OrderStatus.CANCELED) {
                    canceled++;
                }
                assertEquals(0L, order.remainingReservedAmount(),
                        "terminal order retains frozen reserve: " + order.orderId());
                assertEquals(status, counterparty.status());
                assertEquals(0L, counterparty.remainingReservedAmount(),
                        "counterparty retains frozen reserve: " + counterparty.orderId());
            }
            long stateChanges = LIFECYCLE_PAIRS * 2L
                    + engine.metrics().settledTradeCount() * 2L
                    + engine.metrics().pendingCancelCount() * 2L;

            assertEquals(LIFECYCLE_MEASUREMENTS, operations);
            assertEquals(LIFECYCLE_PAIRS / 2L, filled);
            assertEquals(LIFECYCLE_PAIRS / 2L, canceled);
            assertEquals(3L,
                    engine.order(1_000_000L).cumulativeBaseFilled(),
                    "filled lifecycle must include two partials and a final fill");
            assertEquals(2L,
                    engine.order(1_000_001L).cumulativeBaseFilled(),
                    "canceled lifecycle must retain two partial fills");
            assertEquals(150_000L, engine.metrics().settledTradeCount());
            assertEquals(210_000L, engine.metrics().duplicateTradeCount());
            assertEquals(60_000L, engine.metrics().pendingCancelCount());
            assertTrue(engine.metrics().partialFillCount() >= 120_000L,
                    "representative lifecycle must execute two partial fills per order");
            assertTrue(stateChanges >= 500_000L,
                    () -> "state changes=" + stateChanges);
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
            for (long userId = 1L; userId <= LIFECYCLE_USERS_PER_SIDE; userId++) {
                assertEquals(0L, ledger.balance(userId, USDT).frozen());
                assertEquals(0L,
                        ledger.balance(SELLER_USER_BASE + userId, BTC).frozen());
            }

            printReport(
                    "CEX REPRESENTATIVE LIFECYCLE REPORT",
                    operations, tps, histogram, maxMemory, usedMemory,
                    gcBefore, gcAfter);
            System.out.println("Filled orders:           " + filled);
            System.out.println("Canceled orders:         " + canceled);
            System.out.println("State changes:           " + stateChanges);
            System.out.println("Settled trades:          "
                    + engine.metrics().settledTradeCount());
            System.out.println("Duplicate trades:        "
                    + engine.metrics().duplicateTradeCount());
            System.out.println("Partial fills:           "
                    + engine.metrics().partialFillCount());
            System.out.println("Pending cancels:         "
                    + engine.metrics().pendingCancelCount());
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
            for (int index = workerIndex;
                    index < LIFECYCLE_PAIRS; index += THREADS) {
                long orderId = 1_000_000L + index;
                long counterpartyOrderId = SELL_ORDER_BASE + index;
                OrderSubmission submission = lifecycleSubmission(orderId, index);
                OrderSubmission counterpartySubmission =
                        lifecycleSellSubmission(counterpartyOrderId, index);
                boolean filled = (index & 1) == 0;
                long firstTradeId = index * 3L + 1L;
                TradeExecution first = lifecycleExecution(
                        firstTradeId, orderId, counterpartyOrderId, 2L);
                TradeExecution second = lifecycleExecution(
                        firstTradeId + 1L, orderId, counterpartyOrderId, 3L);

                long operationStarted = System.nanoTime();
                engine.submit(submission);
                histogram.record(System.nanoTime() - operationStarted);

                operationStarted = System.nanoTime();
                engine.submit(counterpartySubmission);
                histogram.record(System.nanoTime() - operationStarted);

                operationStarted = System.nanoTime();
                engine.onTrade(first);
                histogram.record(System.nanoTime() - operationStarted);

                operationStarted = System.nanoTime();
                engine.onTrade(second);
                histogram.record(System.nanoTime() - operationStarted);

                if (filled) {
                    TradeExecution terminal = lifecycleExecution(
                            firstTradeId + 2L, orderId, counterpartyOrderId, 4L);
                    operationStarted = System.nanoTime();
                    engine.onTrade(terminal);
                    histogram.record(System.nanoTime() - operationStarted);
                    // 重复样本复用同一不可变终态成交，避免分配替代执行对象。
                    operationStarted = System.nanoTime();
                    engine.onTrade(terminal);
                    histogram.record(System.nanoTime() - operationStarted);
                    operationStarted = System.nanoTime();
                    engine.onTrade(terminal);
                    histogram.record(System.nanoTime() - operationStarted);
                    operationStarted = System.nanoTime();
                    engine.onTrade(terminal);
                    histogram.record(System.nanoTime() - operationStarted);
                } else {
                    operationStarted = System.nanoTime();
                    engine.requestCancel(cancelRequest(orderId));
                    histogram.record(System.nanoTime() - operationStarted);
                    operationStarted = System.nanoTime();
                    engine.onCancelConfirmed(cancelConfirmation(orderId));
                    histogram.record(System.nanoTime() - operationStarted);
                    operationStarted = System.nanoTime();
                    engine.requestCancel(cancelRequest(counterpartyOrderId));
                    histogram.record(System.nanoTime() - operationStarted);
                    operationStarted = System.nanoTime();
                    engine.onCancelConfirmed(cancelConfirmation(counterpartyOrderId));
                    histogram.record(System.nanoTime() - operationStarted);
                    // 已结算部分成交的重复热投递复用 second，不产生临时成交对象。
                    for (int duplicate = 0; duplicate < 4; duplicate++) {
                        operationStarted = System.nanoTime();
                        engine.onTrade(second);
                        histogram.record(System.nanoTime() - operationStarted);
                    }
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    /**
     * 创建代表性生命周期买单。
     *
     * @param orderId 主订单标识
     * @param index 从零开始的订单索引
     * @return 可接受两次部分成交并最终全成或撤单的买单提交
     */
    private static OrderSubmission lifecycleSubmission(long orderId, int index) {
        long userId = (index & (LIFECYCLE_USERS_PER_SIDE - 1)) + 1L;
        return new OrderSubmission(
                orderId, userId, OrderSide.BUY, BTC_USDT,
                LIFECYCLE_BASE_QUANTITY, LIFECYCLE_QUOTE_RESERVE,
                LIFECYCLE_QUOTE_RESERVE, 1L, 1L);
    }

    /**
     * 创建代表性生命周期卖单。
     *
     * @param orderId 卖单标识
     * @param index 从零开始的订单对索引
     * @return 与买单逐对创建并冻结基础资产的卖单提交
     */
    private static OrderSubmission lifecycleSellSubmission(
            long orderId, int index) {
        long userId = SELLER_USER_BASE
                + (index & (LIFECYCLE_USERS_PER_SIDE - 1)) + 1L;
        return new OrderSubmission(
                orderId, userId, OrderSide.SELL, BTC_USDT,
                LIFECYCLE_BASE_QUANTITY, LIFECYCLE_BASE_QUANTITY,
                LIFECYCLE_QUOTE_RESERVE, 1L, 1L);
    }

    /**
     * 创建代表性生命周期的一单位部分或最终成交。
     *
     * @param tradeId 成交幂等标识
     * @param orderId 主买单标识
     * @param counterpartyOrderId 对手卖单标识
     * @param orderSequence 买卖双方相同的权威序号
     * @return 权威双边成交
     */
    private static TradeExecution lifecycleExecution(
            long tradeId,
            long orderId,
            long counterpartyOrderId,
            long orderSequence) {
        return new TradeExecution(
                tradeId,
                orderId,
                counterpartyOrderId,
                BTC_USDT,
                1L,
                LIFECYCLE_QUOTE_FILL,
                orderSequence,
                orderSequence,
                tradeId);
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
     * @return 两次部分成交后的序号四撤单确认
     */
    private static CancelConfirmation cancelConfirmation(long orderId) {
        return new CancelConfirmation(orderId, orderId, 4L, 3L);
    }

    /**
     * 创建生命周期复用的买方和卖方用户余额。
     *
     * @return 初始化完成的多资产账本
     */
    private static AccountLedger lifecycleLedger() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= LIFECYCLE_USERS_PER_SIDE; userId++) {
            ledger.createBalance(userId, BTC, 0L);
            ledger.createBalance(userId, USDT, 10_000L);
            long sellerUserId = SELLER_USER_BASE + userId;
            ledger.createBalance(sellerUserId, BTC, 100L);
            ledger.createBalance(sellerUserId, USDT, 0L);
        }
        return ledger;
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
