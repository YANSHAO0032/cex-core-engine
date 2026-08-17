package com.cex.core.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.InvariantChecker;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.CancelConfirmation;
import com.cex.core.order.CancelRequest;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

/**
 * 强类型订单生命周期混沌测试，验证重复、乱序、线程故障与资产守恒。
 *
 * <p>保留原有 300,000 个主订单工作量；每个 worker 使用一个顺序推进的卖方订单，
 * 将旧单边成交机械映射为外部权威双边成交，不扩展 Task 9 的部分成交场景矩阵。</p>
 *
 * @note 注入线程让出、短暂停顿和线程中断，并由 watchdog 与 worker 周期性校验逐资产不变量。
 * @note 买卖双方成交通过固定条带顺序原子提交，任何时刻不得出现负余额或单边成交。
 */
class ChaosInvariantTest {
    /** 混沌场景随机种子，默认值保持旧测试序列可复现。 */
    private static final long CHAOS_SEED =
            Long.getLong("CHAOS_SEED", 20260816L);
    /** 并发处理主订单的长生命周期工作线程数量。 */
    private static final int WORKERS = 16;
    /** 混沌测试主订单总数。 */
    private static final int ORDERS = 300_000;
    /** 为减少账户常驻对象而复用的买方用户数量。 */
    private static final int BUYER_USERS = 4_096;
    /** 每个买方用户预置的报价资产数量。 */
    private static final long QUOTE_PER_BUYER = 128L;
    /** 共享卖方订单标识起点。 */
    private static final long SELL_ORDER_BASE = 1_000_000L;
    /** 共享卖方用户标识起点。 */
    private static final long SELLER_USER_BASE = 10_000L;
    /** 事件时间基准，保证生成非负且不溢出的业务时间。 */
    private static final long EVENT_TIME_BASE =
            Math.floorMod(CHAOS_SEED, Long.MAX_VALUE - ORDERS);
    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 混沌成交使用的固定交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 按固定种子洗牌后的场景循环。 */
    private static final Scenario[] SCENARIO_CYCLE = shuffledScenarioCycle();
    /** 每个 worker 需要冻结并交付的基础资产数量。 */
    private static final long[] WORKER_FILL_COUNTS = workerFillCounts();

    /**
     * 验证带故障注入的强类型订单生命周期最终收敛且无死锁或线程泄漏。
     *
     * @throws Exception worker 结果获取、线程等待或测试资源关闭失败时抛出
     */
    @Test
    void seededLifecycleChaosConvergesWithoutInvariantFailureOrDeadlock()
            throws Exception {
        AccountLedger ledger = ledger();
        OrderEngine engine = new OrderEngine(ledger);
        submitWorkerSellers(engine);
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
                        workerIndex, start, engine, checker, invariantFailure,
                        yieldInjections, parkInjections, interruptInjections)));
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
            assertEquals(expectedFilled, engine.metrics().settledTradeCount());
            assertEquals(0, engine.pendingTradeCount());
            assertTrue(yieldInjections.sum() > 0L);
            assertTrue(parkInjections.sum() > 0L);
            assertTrue(interruptInjections.sum() > 0L);
            assertEquals(ledger.initialTotalAssets(), ledger.currentTotalAssets());
            assertTrue(ledger.allBalancesNonNegative());

            System.out.println("CHAOS SEED = " + CHAOS_SEED);
            System.out.println("Settled trades: "
                    + engine.metrics().settledTradeCount());
            System.out.println("Rejected trades: "
                    + engine.metrics().tradeRejectedCount());
            System.out.println("Expected filled: " + expectedFilled);
            System.out.println("Expected canceled: " + expectedCanceled);
            System.out.println("Yield injections: " + yieldInjections.sum());
            System.out.println("Park injections: " + parkInjections.sum());
            System.out.println("Interrupt injections: " + interruptInjections.sum());
            System.out.println("Invariant snapshots: " + checker.snapshotCount());
            System.out.println("Invariant failures: " + checker.failureCount());
            System.out.println("Asset deltas: "
                    + assetDeltas(ledger.initialTotalAssets(), ledger.currentTotalAssets()));
            System.out.println("Deadlock check: PASS");
            System.out.println("Termination check: PASS");
        } finally {
            running.set(false);
            workers.shutdownNow();
            watchdog.join(5_000L);
            engine.close();
        }
    }

    /**
     * 执行单个 worker 的订单分片并注入调度与中断故障。
     *
     * @param workerIndex 工作线程索引
     * @param start 全部工作线程的统一启动闩锁
     * @param engine 强类型订单引擎
     * @param checker 逐资产不变量检查器
     * @param invariantFailure 共享不变量失败标识
     * @param yieldInjections 线程让出注入计数
     * @param parkInjections 短暂停顿注入计数
     * @param interruptInjections 线程中断注入计数
     */
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
            long sellerSequence = 2L;
            for (int orderIndex = workerIndex;
                    orderIndex < ORDERS; orderIndex += WORKERS) {
                long orderId = orderIndex + 1L;
                Scenario scenario = scenarioFor(orderIndex);
                long tradeSellerSequence = scenario.fillsOrder
                        ? sellerSequence++ : 0L;
                processScenario(
                        engine, workerIndex, orderId, tradeSellerSequence, scenario);
                if ((orderIndex & 255) == 0) {
                    switch (random.nextInt(3)) {
                        case 0 -> {
                            yieldInjections.increment();
                            Thread.yield();
                        }
                        case 1 -> {
                            parkInjections.increment();
                            LockSupport.parkNanos(
                                    random.nextLong(10_000L, 100_001L));
                        }
                        default -> {
                            interruptInjections.increment();
                            Thread.currentThread().interrupt();
                            engine.submit(submission(orderId));
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

    /**
     * 按指定场景投递强类型创建、成交与撤单输入。
     *
     * @param engine 强类型订单引擎
     * @param workerIndex 共享卖单所属 worker 索引
     * @param orderId 当前主订单标识
     * @param sellerSequence 成交在共享卖单中的权威序号；撤单场景为零
     * @param scenario 当前混沌场景
     */
    private static void processScenario(
            OrderEngine engine,
            int workerIndex,
            long orderId,
            long sellerSequence,
            Scenario scenario) {
        OrderSubmission submission = submission(orderId);
        TradeExecution trade = scenario.fillsOrder
                ? execution(orderId, workerIndex, sellerSequence) : null;
        CancelRequest cancel = new CancelRequest(
                orderId, orderId, EVENT_TIME_BASE + orderId);
        long cancelSequence = scenario == Scenario.CONFLICT_BEFORE_CREATE
                ? 3L : 2L;
        CancelConfirmation confirmation = new CancelConfirmation(
                orderId, orderId, cancelSequence, EVENT_TIME_BASE + orderId);
        switch (scenario) {
            case FILL_IN_ORDER -> {
                engine.submit(submission);
                engine.submit(submission);
                engine.onTrade(trade);
            }
            case FILL_OUT_OF_ORDER -> {
                engine.onTrade(trade);
                engine.onTrade(trade);
                engine.submit(submission);
            }
            case CANCEL_IN_ORDER -> {
                engine.submit(submission);
                engine.requestCancel(cancel);
                engine.onCancelConfirmed(confirmation);
                engine.onCancelConfirmed(confirmation);
            }
            case CANCEL_OUT_OF_ORDER -> {
                engine.onCancelConfirmed(confirmation);
                engine.onCancelConfirmed(confirmation);
                engine.submit(submission);
                engine.requestCancel(cancel);
            }
            case CONFLICT_BEFORE_CREATE -> {
                engine.onCancelConfirmed(confirmation);
                engine.onTrade(trade);
                engine.submit(submission);
                engine.requestCancel(cancel);
            }
        }
    }

    /**
     * 创建一个基础数量和报价预留均为一的买单。
     *
     * @param orderId 主订单标识
     * @return 确定性的强类型买单提交
     */
    private static OrderSubmission submission(long orderId) {
        return new OrderSubmission(
                orderId,
                buyerUserId(orderId),
                OrderSide.BUY,
                BTC_USDT,
                1L,
                1L,
                1L,
                1L,
                EVENT_TIME_BASE + orderId);
    }

    /**
     * 创建一个主买单与当前 worker 共享卖单之间的全量成交。
     *
     * @param orderId 主买单标识，同时作为成交标识
     * @param workerIndex 共享卖单 worker 索引
     * @param sellerSequence 卖单下一权威序号
     * @return 权威双边成交
     */
    private static TradeExecution execution(
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
                EVENT_TIME_BASE + orderId);
    }

    /**
     * 创建复用买方与每 worker 共享卖方的多资产账本。
     *
     * @return 已初始化所有余额桶的账本
     */
    private static AccountLedger ledger() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= BUYER_USERS; userId++) {
            ledger.createBalance(userId, BTC, 0L);
            ledger.createBalance(userId, USDT, QUOTE_PER_BUYER);
        }
        for (int worker = 0; worker < WORKERS; worker++) {
            long sellerId = sellerUserId(worker);
            ledger.createBalance(sellerId, BTC, WORKER_FILL_COUNTS[worker]);
            ledger.createBalance(sellerId, USDT, 0L);
        }
        return ledger;
    }

    /**
     * 在 worker 启动前提交并冻结所有共享卖方订单。
     *
     * @param engine 强类型订单引擎
     */
    private static void submitWorkerSellers(OrderEngine engine) {
        for (int worker = 0; worker < WORKERS; worker++) {
            long quantity = WORKER_FILL_COUNTS[worker];
            engine.submit(new OrderSubmission(
                    sellerOrderId(worker),
                    sellerUserId(worker),
                    OrderSide.SELL,
                    BTC_USDT,
                    quantity,
                    quantity,
                    quantity,
                    1L,
                    EVENT_TIME_BASE));
        }
    }

    /**
     * 计算主订单复用的买方用户标识。
     *
     * @param orderId 主订单标识
     * @return 一到买方用户数量之间的用户标识
     */
    private static long buyerUserId(long orderId) {
        return ((orderId - 1L) & (BUYER_USERS - 1L)) + 1L;
    }

    /**
     * 计算 worker 共享卖单标识。
     *
     * @param workerIndex worker 索引
     * @return 不与主订单重叠的卖单标识
     */
    private static long sellerOrderId(int workerIndex) {
        return SELL_ORDER_BASE + workerIndex;
    }

    /**
     * 计算 worker 共享卖方用户标识。
     *
     * @param workerIndex worker 索引
     * @return 不与买方用户重叠的卖方标识
     */
    private static long sellerUserId(int workerIndex) {
        return SELLER_USER_BASE + workerIndex;
    }

    /**
     * 统计每个 worker 的成交型主订单数量。
     *
     * @return 每个 worker 对应的基础资产预留量
     */
    private static long[] workerFillCounts() {
        long[] counts = new long[WORKERS];
        for (int worker = 0; worker < WORKERS; worker++) {
            for (int orderIndex = worker;
                    orderIndex < ORDERS; orderIndex += WORKERS) {
                if (scenarioFor(orderIndex).fillsOrder) {
                    counts[worker]++;
                }
            }
        }
        return counts;
    }

    /**
     * 计算各资产当前总额相对初始总额的差值。
     *
     * @param initial 初始逐资产总额
     * @param current 当前逐资产总额
     * @return 逐资产差值文本映射
     */
    private static Map<AssetId, Long> assetDeltas(
            Map<AssetId, Long> initial, Map<AssetId, Long> current) {
        java.util.HashMap<AssetId, Long> deltas = new java.util.HashMap<>();
        for (AssetId asset : initial.keySet()) {
            deltas.put(asset, current.getOrDefault(asset, 0L) - initial.get(asset));
        }
        return Map.copyOf(deltas);
    }

    /**
     * 使用固定混沌种子洗牌场景枚举。
     *
     * @return 洗牌后的独立场景数组
     */
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

    /**
     * 根据订单索引选择可复现场景。
     *
     * @param orderIndex 从零开始的订单索引
     * @return 当前订单场景
     */
    private static Scenario scenarioFor(long orderIndex) {
        return SCENARIO_CYCLE[(int) (orderIndex % SCENARIO_CYCLE.length)];
    }

    /** 混沌强类型输入排列场景。 */
    private enum Scenario {
        /** 创建后成交，预期完成双边结算。 */
        FILL_IN_ORDER(OrderStatus.FILLED, true),
        /** 成交先于创建到达，预期创建后自动重试结算。 */
        FILL_OUT_OF_ORDER(OrderStatus.FILLED, true),
        /** 创建后撤单，预期确认后解冻。 */
        CANCEL_IN_ORDER(OrderStatus.CANCELED, false),
        /** 撤单确认先于创建，预期请求登记后解冻。 */
        CANCEL_OUT_OF_ORDER(OrderStatus.CANCELED, false),
        /** 创建前同时缓存成交与较高序号撤单确认，预期较低成交先完成。 */
        CONFLICT_BEFORE_CREATE(OrderStatus.FILLED, true);

        /** 场景完成后的主订单预期状态。 */
        private final OrderStatus expectedStatus;
        /** 场景是否消费一笔双边成交。 */
        private final boolean fillsOrder;

        /**
         * 创建场景元数据。
         *
         * @param expectedStatus 主订单预期终态
         * @param fillsOrder 是否需要共享卖方成交
         */
        Scenario(OrderStatus expectedStatus, boolean fillsOrder) {
            this.expectedStatus = expectedStatus;
            this.fillsOrder = fillsOrder;
        }
    }
}
