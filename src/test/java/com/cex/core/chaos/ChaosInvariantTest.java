package com.cex.core.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
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
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.ManualClock;
import com.cex.core.risk.RiskDecision;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeResult;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 强类型订单生命周期混沌测试，验证重复、乱序、线程故障与资产守恒。
 *
 * <p>保留 300,000 个主订单工作量，并额外覆盖部分成交、撤单确认、双序号空洞、
 * 风控审批、成交幂等和相反用户锁顺序的八类最终验收场景。</p>
 *
 * @note 注入线程让出、短暂停顿和线程中断，并由 watchdog 与 worker 周期性校验逐资产不变量。
 * @note 买卖双方成交通过固定条带顺序原子提交，任何时刻不得出现负余额或单边成交。
 */
class ChaosInvariantTest {
    /** 混沌场景随机种子，默认值固定为最终验收种子。 */
    private static final long CHAOS_SEED = 20260817L;
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

    /** 验证混沌验收使用规定种子并覆盖完整的八类双边场景。 */
    @Test
    void acceptanceConfigurationUsesRequiredSeedAndEightScenarios() {
        assertEquals(20260817L, CHAOS_SEED);
        assertEquals(8, AcceptanceScenario.values().length);
    }

    /**
     * 验证带故障注入的强类型订单生命周期最终收敛且无死锁或线程泄漏。
     *
     * @throws Exception worker 结果获取、线程等待或测试资源关闭失败时抛出
     */
    @Test
    void seededLifecycleChaosConvergesWithoutInvariantFailureOrDeadlock()
            throws Exception {
        ChaosReport report = new ChaosReport();
        runAcceptanceScenarioMatrix(report);
        AccountLedger ledger = ledger();
        OrderEngine engine = new OrderEngine(ledger);
        submitWorkerSellers(engine);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean invariantFailure = new AtomicBoolean(false);
        AtomicReference<Throwable> watchdogFailure = new AtomicReference<>();
        LongAdder yieldInjections = new LongAdder();
        LongAdder parkInjections = new LongAdder();
        LongAdder interruptInjections = new LongAdder();
        Thread watchdog = new Thread(() -> {
            try {
                while (running.get()) {
                    if (!recordInvariantSnapshot(ledger, report)) {
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
                        workerIndex, start, engine, ledger, report, invariantFailure,
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
            assertInvariantSnapshot(ledger, report);
            assertEquals(0L, report.invariantFailures());

            long expectedFilled = 0L;
            long expectedCanceled = 0L;
            for (int orderIndex = 0; orderIndex < ORDERS; orderIndex++) {
                long orderId = orderIndex + 1L;
                Scenario scenario = scenarioFor(orderIndex);
                com.cex.core.order.OrderContext order = engine.order(orderId);
                assertEquals(scenario.expectedStatus, order.status(),
                        "orderId=" + orderId + ", scenario=" + scenario);
                assertEquals(0L, order.remainingReservedAmount(),
                        "terminal order retains reserve: orderId=" + orderId);
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
            for (long buyerId = 1L; buyerId <= BUYER_USERS; buyerId++) {
                assertEquals(0L, ledger.balance(buyerId, USDT).frozen());
            }
            for (int worker = 0; worker < WORKERS; worker++) {
                assertEquals(0L,
                        ledger.balance(sellerUserId(worker), BTC).frozen());
            }
            assertTrue(yieldInjections.sum() > 0L);
            assertTrue(parkInjections.sum() > 0L);
            assertTrue(interruptInjections.sum() > 0L);
            assertEquals(ledger.initialTotalAssets(), ledger.currentTotalAssets());
            assertTrue(ledger.allBalancesNonNegative());

            report.addEngineMetrics(engine);

            System.out.println("CHAOS SEED = " + CHAOS_SEED);
            System.out.println("Processed executions: " + report.processedExecutions());
            System.out.println("Settled trades: " + report.settledTrades());
            System.out.println("Duplicate trades: " + report.duplicateTrades());
            System.out.println("Rejected trades: " + report.rejectedTrades());
            System.out.println("Partial fills: " + report.partialFills());
            System.out.println("Pending cancels: " + report.pendingCancels());
            System.out.println("Sequence gaps: " + report.sequenceGaps());
            System.out.println("Expected filled: " + expectedFilled);
            System.out.println("Expected canceled: " + expectedCanceled);
            System.out.println("Yield injections: " + yieldInjections.sum());
            System.out.println("Park injections: " + parkInjections.sum());
            System.out.println("Interrupt injections: " + interruptInjections.sum());
            System.out.println("Invariant snapshots: " + report.invariantSnapshots());
            System.out.println("Invariant failures: " + report.invariantFailures());
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
     * 顺序执行最终验收要求的八类双边场景，并将指标聚合到统一报告。
     *
     * @param report 并发安全的混沌验收报告
     * @throws Exception 审批或并发场景未在限定时间内结束时抛出
     */
    private static void runAcceptanceScenarioMatrix(ChaosReport report) throws Exception {
        for (AcceptanceScenario scenario : AcceptanceScenario.values()) {
            switch (scenario) {
                case TRADE_BEFORE_BOTH_SUBMISSIONS ->
                        tradeBeforeBothSubmissions(report);
                case TWO_PARTIALS_THEN_FULL -> twoPartialsThenFull(report);
                case PARTIAL_THEN_CANCEL_CONFIRMATION ->
                        partialThenCancelConfirmation(report);
                case HUNDRED_DUPLICATES -> hundredDuplicateDeliveries(report);
                case CROSSED_SEQUENCE_GAPS -> crossedSequenceGaps(report);
                case APPROVAL_HOLD_THEN_PASS -> approvalHoldThenPass(report);
                case APPROVAL_REJECT_THEN_EARLIER_TRADE ->
                        approvalRejectThenEarlierTrade(report);
                case REVERSED_USER_LOCK_CONTENTION ->
                        reversedUserLockContention(report);
            }
            report.scenarioCovered();
        }
        assertEquals(AcceptanceScenario.values().length, report.coveredScenarios());
    }

    /**
     * 场景：成交先于买卖双方创建，双方冻结完成后自动结算且终态无冻结余额。
     *
     * @param report 并发安全的混沌验收报告
     */
    private static void tradeBeforeBothSubmissions(ChaosReport report) {
        AccountLedger ledger = bilateralLedger(101L, 102L, 1_000L, 1L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            TradeExecution execution = execution(
                    10_001L, 1_001L, 1_002L, 1L, 100L, 2L, 2L);
            assertEquals(TradeResult.PENDING, deliverTrade(engine, execution, report));
            engine.submit(buySubmission(1_001L, 101L, 1L, 100L));
            assertEquals(TradeExecutionState.PENDING, engine.trade(10_001L).state());
            engine.submit(sellSubmission(1_002L, 102L, 1L, 100L));
            assertEquals(TradeExecutionState.SETTLED, engine.trade(10_001L).state());
            assertEquals(0L, ledger.balance(101L, USDT).frozen());
            assertEquals(0L, ledger.balance(102L, BTC).frozen());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 场景：两次部分成交后全量成交，买方价格改善余款与卖方预留一并归零。
     *
     * @param report 并发安全的混沌验收报告
     */
    private static void twoPartialsThenFull(ChaosReport report) {
        AccountLedger ledger = bilateralLedger(201L, 202L, 1_000L, 6L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            engine.submit(buySubmission(2_001L, 201L, 6L, 600L));
            engine.submit(sellSubmission(2_002L, 202L, 6L, 600L));
            assertEquals(TradeResult.SETTLED, deliverTrade(engine,
                    execution(20_001L, 2_001L, 2_002L, 2L, 190L, 2L, 2L), report));
            assertEquals(OrderStatus.PARTIALLY_FILLED, engine.order(2_001L).status());
            assertEquals(TradeResult.SETTLED, deliverTrade(engine,
                    execution(20_002L, 2_001L, 2_002L, 2L, 200L, 3L, 3L), report));
            assertEquals(TradeResult.SETTLED, deliverTrade(engine,
                    execution(20_003L, 2_001L, 2_002L, 2L, 190L, 4L, 4L), report));
            assertEquals(OrderStatus.FILLED, engine.order(2_001L).status());
            assertEquals(OrderStatus.FILLED, engine.order(2_002L).status());
            assertEquals(0L, engine.order(2_001L).remainingReservedAmount());
            assertEquals(0L, ledger.balance(201L, USDT).frozen());
            assertEquals(0L, ledger.balance(202L, BTC).frozen());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 场景：部分成交后进入等待撤单，权威确认只释放买方剩余报价预留。
     *
     * @param report 并发安全的混沌验收报告
     */
    private static void partialThenCancelConfirmation(ChaosReport report) {
        AccountLedger ledger = bilateralLedger(301L, 302L, 1_000L, 5L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            engine.submit(buySubmission(3_001L, 301L, 5L, 500L));
            engine.submit(sellSubmission(3_002L, 302L, 5L, 500L));
            deliverTrade(engine,
                    execution(30_001L, 3_001L, 3_002L, 2L, 200L, 2L, 2L), report);
            engine.requestCancel(new CancelRequest(30_090L, 3_001L, 30L));
            assertEquals(OrderStatus.PENDING_CANCEL, engine.order(3_001L).status());
            engine.onCancelConfirmed(new CancelConfirmation(
                    30_090L, 3_001L, 3L, 31L));
            assertEquals(OrderStatus.CANCELED, engine.order(3_001L).status());
            assertEquals(2L, engine.order(3_001L).cumulativeBaseFilled());
            assertEquals(0L, ledger.balance(301L, USDT).frozen());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 场景：同一不可变成交完成后再投递一百次，只允许首次双边结算。
     *
     * @param report 并发安全的混沌验收报告
     */
    private static void hundredDuplicateDeliveries(ChaosReport report) {
        AccountLedger ledger = bilateralLedger(401L, 402L, 1_000L, 1L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            engine.submit(buySubmission(4_001L, 401L, 1L, 100L));
            engine.submit(sellSubmission(4_002L, 402L, 1L, 100L));
            TradeExecution execution = execution(
                    40_001L, 4_001L, 4_002L, 1L, 100L, 2L, 2L);
            assertEquals(TradeResult.SETTLED, deliverTrade(engine, execution, report));
            for (int duplicate = 0; duplicate < 100; duplicate++) {
                assertEquals(TradeResult.DUPLICATE,
                        deliverTrade(engine, execution, report));
            }
            assertEquals(100L, engine.metrics().duplicateTradeCount());
            assertEquals(1L, engine.metrics().settledTradeCount());
            assertEquals(0L, ledger.balance(401L, USDT).frozen());
            assertEquals(0L, ledger.balance(402L, BTC).frozen());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 场景：买卖序号形成交叉空洞时不得提交任何单边状态或资产变化。
     *
     * @param report 并发安全的混沌验收报告
     */
    private static void crossedSequenceGaps(ChaosReport report) {
        AccountLedger ledger = bilateralLedger(501L, 502L, 1_000L, 2L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            engine.submit(buySubmission(5_001L, 501L, 2L, 200L));
            engine.submit(sellSubmission(5_002L, 502L, 2L, 200L));
            assertEquals(TradeResult.PENDING, deliverTrade(engine,
                    execution(50_001L, 5_001L, 5_002L, 1L, 100L, 2L, 3L), report));
            assertEquals(TradeResult.PENDING, deliverTrade(engine,
                    execution(50_002L, 5_001L, 5_002L, 1L, 100L, 3L, 2L), report));
            assertEquals(0L, engine.order(5_001L).cumulativeBaseFilled());
            assertEquals(0L, engine.order(5_002L).cumulativeBaseFilled());
            assertEquals(2, engine.pendingTradeCount());
            assertTrue(engine.metrics().sequenceGapCount() > 0L);
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 场景：风控暂挂期间缓存双边成交，审批通过后按双方序号自动结算。
     *
     * @param report 并发安全的混沌验收报告
     * @throws Exception 审批线程未在限定时间内进入或排空时抛出
     */
    private static void approvalHoldThenPass(ChaosReport report) throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = bilateralLedger(601L, 602L, 1_000L, 1L);
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> context.userId() == 601L
                        ? RiskDecision.HOLD : RiskDecision.PASS),
                new ManualClock(100L),
                approvals,
                submission -> {
                    approvalEntered.countDown();
                    try {
                        releaseApproval.await();
                        return ApprovalDecision.PASS;
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return ApprovalDecision.REJECT;
                    }
                });
        try {
            engine.submit(buySubmission(6_001L, 601L, 1L, 100L));
            engine.submit(sellSubmission(6_002L, 602L, 1L, 100L));
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
            TradeExecution execution = execution(
                    60_001L, 6_001L, 6_002L, 1L, 100L, 2L, 2L);
            assertEquals(TradeResult.PENDING, deliverTrade(engine, execution, report));
            releaseApproval.countDown();
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(TradeExecutionState.SETTLED, engine.trade(60_001L).state());
            assertEquals(OrderStatus.FILLED, engine.order(6_001L).status());
            assertEquals(0L, ledger.balance(601L, USDT).frozen());
            assertEquals(0L, ledger.balance(602L, BTC).frozen());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    /**
     * 场景：审批拒绝后确认先到，较早成交先结算，剩余资产再撤销且高序号成交被拒绝。
     *
     * @param report 并发安全的混沌验收报告
     * @throws Exception 审批服务未在限定时间内排空时抛出
     */
    private static void approvalRejectThenEarlierTrade(ChaosReport report)
            throws Exception {
        AccountLedger ledger = bilateralLedger(701L, 702L, 1_000L, 3L);
        ApprovalService approvals = new ApprovalService(1, 8);
        List<CancelRequest> requests = new java.util.concurrent.CopyOnWriteArrayList<>();
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> context.userId() == 701L
                        ? RiskDecision.HOLD : RiskDecision.PASS),
                new ManualClock(100L),
                approvals,
                submission -> ApprovalDecision.REJECT,
                requests::add);
        try {
            engine.submit(buySubmission(7_001L, 701L, 3L, 300L));
            engine.submit(sellSubmission(7_002L, 702L, 3L, 300L));
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(1, requests.size());
            CancelRequest request = requests.getFirst();
            engine.onCancelConfirmed(new CancelConfirmation(
                    request.cancelRequestId(), 7_001L, 3L, 120L));
            assertEquals(TradeResult.SETTLED, deliverTrade(engine,
                    execution(70_001L, 7_001L, 7_002L, 1L, 100L, 2L, 2L), report));
            assertEquals(OrderStatus.CANCELED, engine.order(7_001L).status());
            assertEquals(TradeResult.REJECTED, deliverTrade(engine,
                    execution(70_002L, 7_001L, 7_002L, 1L, 100L, 4L, 3L), report));
            assertEquals(0L, ledger.balance(701L, USDT).frozen());
            assertEquals(1L, engine.metrics().tradeRejectedCount());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        } finally {
            engine.close();
        }
    }

    /**
     * 场景：相同两个用户以相反买卖方向竞争条带锁，重复热投递仍无死锁且仅结算两笔。
     *
     * @param report 并发安全的混沌验收报告
     * @throws Exception worker 超时、执行失败或中断时抛出
     */
    private static void reversedUserLockContention(ChaosReport report)
            throws Exception {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = new AccountLedger(locks);
        createDualAssetBalance(ledger, 801L, 2L, 200L);
        createDualAssetBalance(ledger, 802L, 2L, 200L);
        try (OrderEngine engine = new OrderEngine(ledger)) {
            engine.submit(buySubmission(8_001L, 801L, 1L, 100L));
            engine.submit(sellSubmission(8_002L, 802L, 1L, 100L));
            engine.submit(buySubmission(8_003L, 802L, 1L, 100L));
            engine.submit(sellSubmission(8_004L, 801L, 1L, 100L));
            TradeExecution forward = execution(
                    80_001L, 8_001L, 8_002L, 1L, 100L, 2L, 2L);
            TradeExecution reverse = execution(
                    80_002L, 8_003L, 8_004L, 1L, 100L, 2L, 2L);
            ExecutorService executor = Executors.newFixedThreadPool(WORKERS);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>(WORKERS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            try {
                for (int worker = 0; worker < WORKERS; worker++) {
                    final int workerIndex = worker;
                    futures.add(executor.submit(() -> {
                        try {
                            start.await();
                            for (int delivery = 0; delivery < 1_000; delivery++) {
                                TradeExecution candidate = ((delivery + workerIndex) & 1) == 0
                                        ? forward : reverse;
                                deliverTrade(engine, candidate, report);
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(interrupted);
                        }
                    }));
                }
                start.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(
                        remainingNanos(deadline), TimeUnit.NANOSECONDS));
                for (Future<?> future : futures) {
                    future.get(remainingNanos(deadline), TimeUnit.NANOSECONDS);
                }
            } finally {
                executor.shutdownNow();
            }
            assertEquals(2L, engine.metrics().settledTradeCount());
            assertEquals(OrderStatus.FILLED, engine.order(8_001L).status());
            assertEquals(OrderStatus.FILLED, engine.order(8_003L).status());
            assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads());
            assertInvariantSnapshot(ledger, report);
            report.addEngineMetrics(engine);
        }
    }

    /**
     * 执行单个 worker 的订单分片并注入调度与中断故障。
     *
     * @param workerIndex 工作线程索引
     * @param start 全部工作线程的统一启动闩锁
     * @param engine 强类型订单引擎
     * @param ledger 多资产账本
     * @param report 并发安全的混沌验收报告
     * @param invariantFailure 共享不变量失败标识
     * @param yieldInjections 线程让出注入计数
     * @param parkInjections 短暂停顿注入计数
     * @param interruptInjections 线程中断注入计数
     */
    private static void runWorker(
            int workerIndex,
            CountDownLatch start,
            OrderEngine engine,
            AccountLedger ledger,
            ChaosReport report,
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
                        engine, report, workerIndex, orderId,
                        tradeSellerSequence, scenario);
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
                if ((orderIndex & 1023) == 0
                        && !recordInvariantSnapshot(ledger, report)) {
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
     * @param report 并发安全的混沌验收报告
     * @param workerIndex 共享卖单所属 worker 索引
     * @param orderId 当前主订单标识
     * @param sellerSequence 成交在共享卖单中的权威序号；撤单场景为零
     * @param scenario 当前混沌场景
     */
    private static void processScenario(
            OrderEngine engine,
            ChaosReport report,
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
                deliverTrade(engine, trade, report);
            }
            case FILL_OUT_OF_ORDER -> {
                deliverTrade(engine, trade, report);
                deliverTrade(engine, trade, report);
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
                deliverTrade(engine, trade, report);
                engine.submit(submission);
                engine.requestCancel(cancel);
            }
        }
    }

    /**
     * 统计一次权威成交投递并调用真实引擎入口。
     *
     * @param engine 强类型订单引擎
     * @param execution 复用或新建的不可变权威成交
     * @param report 并发安全的混沌验收报告
     * @return 引擎对本次投递的处理结果
     */
    private static TradeResult deliverTrade(
            OrderEngine engine, TradeExecution execution, ChaosReport report) {
        report.processedExecution();
        return engine.onTrade(execution);
    }

    /**
     * 创建固定初始序号的买单。
     *
     * @param orderId 买单标识
     * @param userId 买方用户标识
     * @param baseQuantity 原始基础资产数量
     * @param reserve 报价资产冻结数量
     * @return 强类型买单提交
     */
    private static OrderSubmission buySubmission(
            long orderId, long userId, long baseQuantity, long reserve) {
        return new OrderSubmission(
                orderId, userId, OrderSide.BUY, BTC_USDT,
                baseQuantity, reserve, reserve, 1L, EVENT_TIME_BASE + orderId);
    }

    /**
     * 创建固定初始序号的卖单。
     *
     * @param orderId 卖单标识
     * @param userId 卖方用户标识
     * @param baseQuantity 原始基础资产数量
     * @param riskQuoteAmount 上游提供的报价资产风控金额
     * @return 强类型卖单提交
     */
    private static OrderSubmission sellSubmission(
            long orderId, long userId, long baseQuantity, long riskQuoteAmount) {
        return new OrderSubmission(
                orderId, userId, OrderSide.SELL, BTC_USDT,
                baseQuantity, baseQuantity, riskQuoteAmount,
                1L, EVENT_TIME_BASE + orderId);
    }

    /**
     * 创建带双订单权威序号的外部成交。
     *
     * @param tradeId 成交幂等标识
     * @param buyOrderId 买单标识
     * @param sellOrderId 卖单标识
     * @param baseQuantity 基础资产最小单位数量
     * @param quoteQuantity 报价资产最小单位数量
     * @param buySequence 买单权威序号
     * @param sellSequence 卖单权威序号
     * @return 不可变权威成交
     */
    private static TradeExecution execution(
            long tradeId,
            long buyOrderId,
            long sellOrderId,
            long baseQuantity,
            long quoteQuantity,
            long buySequence,
            long sellSequence) {
        return new TradeExecution(
                tradeId, buyOrderId, sellOrderId, BTC_USDT,
                baseQuantity, quoteQuantity, buySequence, sellSequence,
                EVENT_TIME_BASE + tradeId);
    }

    /**
     * 创建一组买方报价资产与卖方基础资产余额。
     *
     * @param buyerId 买方用户标识
     * @param sellerId 卖方用户标识
     * @param buyerQuote 买方初始可用报价资产
     * @param sellerBase 卖方初始可用基础资产
     * @return 已创建四个必需余额桶的多资产账本
     */
    private static AccountLedger bilateralLedger(
            long buyerId, long sellerId, long buyerQuote, long sellerBase) {
        AccountLedger ledger = new AccountLedger(new StripedLockManager(16));
        createDualAssetBalance(ledger, buyerId, 0L, buyerQuote);
        createDualAssetBalance(ledger, sellerId, sellerBase, 0L);
        return ledger;
    }

    /**
     * 为用户创建基础资产和报价资产余额桶。
     *
     * @param ledger 多资产账本
     * @param userId 用户标识
     * @param baseAvailable 初始可用基础资产
     * @param quoteAvailable 初始可用报价资产
     */
    private static void createDualAssetBalance(
            AccountLedger ledger,
            long userId,
            long baseAvailable,
            long quoteAvailable) {
        ledger.createBalance(userId, BTC, baseAvailable);
        ledger.createBalance(userId, USDT, quoteAvailable);
    }

    /**
     * 在全条带一致快照中分别断言逐资产守恒和所有余额非负。
     *
     * @param ledger 待检查的多资产账本
     * @param report 并发安全的混沌验收报告
     */
    private static void assertInvariantSnapshot(
            AccountLedger ledger, ChaosReport report) {
        int state = invariantSnapshotState(ledger, report);
        assertTrue((state & 1) != 0, "per-asset invariant failed");
        assertTrue((state & 2) != 0, "negative balance detected");
    }

    /**
     * 在全条带一致快照中记录资产守恒与余额非负结果。
     *
     * @param ledger 待检查的多资产账本
     * @param report 并发安全的混沌验收报告
     * @return 两项检查均通过时为 {@code true}
     */
    private static boolean recordInvariantSnapshot(
            AccountLedger ledger, ChaosReport report) {
        return invariantSnapshotState(ledger, report) == 3;
    }

    /**
     * 按条带升序获取全部锁，读取两个独立不变量结果后逆序释放。
     *
     * @param ledger 待检查的多资产账本
     * @param report 并发安全的混沌验收报告
     * @return 位零表示逐资产守恒，位一表示所有余额非负
     * @note 全条带锁仅用于有界批次快照，不进入订单或成交热路径。
     */
    private static int invariantSnapshotState(
            AccountLedger ledger, ChaosReport report) {
        StripedLockManager locks = ledger.lockManager();
        int acquired = 0;
        try {
            for (; acquired < locks.stripeCount(); acquired++) {
                locks.lockForStripe(acquired).lock();
            }
            boolean invariant = ledger.allAssetInvariantsHold();
            boolean nonNegative = ledger.allBalancesNonNegative();
            int state = (invariant ? 1 : 0) | (nonNegative ? 2 : 0);
            report.invariantSnapshot(state == 3);
            return state;
        } finally {
            for (int index = acquired - 1; index >= 0; index--) {
                ReentrantLock lock = locks.lockForStripe(index);
                lock.unlock();
            }
        }
    }

    /**
     * 计算统一并发截止时间剩余纳秒数。
     *
     * @param deadlineNanos 单调时钟截止纳秒
     * @return 至少为一的剩余纳秒数
     */
    private static long remainingNanos(long deadlineNanos) {
        return Math.max(1L, deadlineNanos - System.nanoTime());
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

    /** 聚合多引擎、多线程混沌场景的原始计数，避免为每次投递分配报告对象。 */
    private static final class ChaosReport {
        /** 调用强类型成交入口的总次数。 */
        private final LongAdder processedExecutions = new LongAdder();
        /** 所有场景成功完成双边结算的成交数。 */
        private final LongAdder settledTrades = new LongAdder();
        /** 所有场景精确重复投递的成交数。 */
        private final LongAdder duplicateTrades = new LongAdder();
        /** 所有场景确定拒绝且消费双序号的成交数。 */
        private final LongAdder rejectedTrades = new LongAdder();
        /** 所有场景至少一侧仍有剩余数量的成交数。 */
        private final LongAdder partialFills = new LongAdder();
        /** 所有场景首次进入等待撤单确认的订单数。 */
        private final LongAdder pendingCancels = new LongAdder();
        /** 所有场景首次观察到的权威序号空洞数。 */
        private final LongAdder sequenceGaps = new LongAdder();
        /** 已完成的全条带一致快照数。 */
        private final LongAdder invariantSnapshots = new LongAdder();
        /** 全条带快照中资产守恒或非负检查失败数。 */
        private final LongAdder invariantFailures = new LongAdder();
        /** 已执行完成的最终验收场景数量。 */
        private final AtomicInteger coveredScenarios = new AtomicInteger();

        /** 记录一次强类型成交入口调用。 */
        private void processedExecution() {
            processedExecutions.increment();
        }

        /**
         * 汇总一个已完成场景引擎的九项领域指标。
         *
         * @param engine 已完成当前场景的强类型订单引擎
         */
        private void addEngineMetrics(OrderEngine engine) {
            settledTrades.add(engine.metrics().settledTradeCount());
            duplicateTrades.add(engine.metrics().duplicateTradeCount());
            rejectedTrades.add(engine.metrics().tradeRejectedCount());
            partialFills.add(engine.metrics().partialFillCount());
            pendingCancels.add(engine.metrics().pendingCancelCount());
            sequenceGaps.add(engine.metrics().sequenceGapCount());
        }

        /**
         * 记录一次全条带一致快照结果。
         *
         * @param valid 逐资产守恒且所有余额非负时为 {@code true}
         */
        private void invariantSnapshot(boolean valid) {
            invariantSnapshots.increment();
            if (!valid) {
                invariantFailures.increment();
            }
        }

        /** 记录一个八场景矩阵成员已执行完成。 */
        private void scenarioCovered() {
            coveredScenarios.incrementAndGet();
        }

        /** @return 强类型成交入口调用总次数 */
        private long processedExecutions() { return processedExecutions.sum(); }

        /** @return 成功双边结算成交总数 */
        private long settledTrades() { return settledTrades.sum(); }

        /** @return 精确重复成交总数 */
        private long duplicateTrades() { return duplicateTrades.sum(); }

        /** @return 确定拒绝成交总数 */
        private long rejectedTrades() { return rejectedTrades.sum(); }

        /** @return 部分成交总数 */
        private long partialFills() { return partialFills.sum(); }

        /** @return 首次等待撤单确认订单总数 */
        private long pendingCancels() { return pendingCancels.sum(); }

        /** @return 权威序号空洞总数 */
        private long sequenceGaps() { return sequenceGaps.sum(); }

        /** @return 已完成的全条带一致快照总数 */
        private long invariantSnapshots() { return invariantSnapshots.sum(); }

        /** @return 失败的全条带一致快照总数 */
        private long invariantFailures() { return invariantFailures.sum(); }

        /** @return 已执行完成的最终验收场景数量 */
        private int coveredScenarios() { return coveredScenarios.get(); }
    }

    /** 最终验收必须逐项覆盖的八类双边成交混沌场景。 */
    private enum AcceptanceScenario {
        /** 成交早于买卖双方订单创建。 */
        TRADE_BEFORE_BOTH_SUBMISSIONS,
        /** 两次部分成交后由第三笔完成全量成交。 */
        TWO_PARTIALS_THEN_FULL,
        /** 部分成交后等待撤单并消费权威确认。 */
        PARTIAL_THEN_CANCEL_CONFIRMATION,
        /** 同一成交标识在结算后重复投递一百次。 */
        HUNDRED_DUPLICATES,
        /** 买卖订单序号以相反次序形成交叉空洞。 */
        CROSSED_SEQUENCE_GAPS,
        /** 风控暂挂期间缓存成交并在审批通过后结算。 */
        APPROVAL_HOLD_THEN_PASS,
        /** 审批拒绝后先消费早期成交再确认撤单。 */
        APPROVAL_REJECT_THEN_EARLIER_TRADE,
        /** 相同用户以相反买卖身份竞争条带锁。 */
        REVERSED_USER_LOCK_CONTENTION
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
