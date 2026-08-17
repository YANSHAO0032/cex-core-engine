package com.cex.core.trade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.OrderContext;
import com.cex.core.order.OrderEngineMetrics;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStateMachine;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * 验证成交协调器的固定锁序、同条带去重和并发资金不变量。
 *
 * <p>核心能力：对反向买卖双方施加并发压力，并限定所有等待时间以检测死锁。</p>
 * <p>线程安全：每个测试使用独立协调器；工作线程只通过生产入口访问共享订单和账本。</p>
 * <p>使用限制：守护线程仅用于失败时避免故障锁序阻止测试进程退出，不放宽任何完成断言。</p>
 */
class TradeSettlementConcurrencyTest {

    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 每个并发工作线程提交的独立成交数量。 */
    private static final int TRADES_PER_WORKER = 16;
    /** 固定并发工作线程数量。 */
    private static final int WORKER_COUNT = 16;

    /** 场景：反向参数顺序的成交等待低条带时不得提前持有高条带。 */
    @Test
    void reverseSubmissionWaitsForLowerStripeBeforeAcquiringHigherStripe() throws Exception {
        StripedLockManager locks = new StripedLockManager(8);
        assertTrue(locks.stripeIndexForUser(1L) < locks.stripeIndexForUser(2L));
        ConcurrentFixture fixture = singleTradeFixture(locks, 2L, 1L, false);
        ReentrantLock lowerLock = locks.lockForUser(1L);
        ReentrantLock higherLock = locks.lockForUser(2L);
        AtomicReference<Thread> workerThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = Thread.ofPlatform()
                    .daemon(true)
                    .name("settlement-lock-order-probe")
                    .unstarted(runnable);
            workerThread.set(thread);
            return thread;
        });
        lowerLock.lock();
        try {
            Future<TradeResult> result = executor.submit(
                    () -> fixture.coordinator.accept(fixture.executions.getFirst()));
            awaitCondition(
                    () -> workerThread.get() != null
                            && lowerLock.hasQueuedThread(workerThread.get()),
                    Duration.ofSeconds(2));

            assertNotNull(fixture.store.record(1L),
                    "trade registration must finish before waiting for user locks");
            assertFalse(higherLock.isLocked(),
                    "higher stripe must not be held while waiting for the lower stripe");

            lowerLock.unlock();
            assertEquals(TradeResult.SETTLED, result.get(5L, TimeUnit.SECONDS));
        } finally {
            if (lowerLock.isHeldByCurrentThread()) {
                lowerLock.unlock();
            }
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /** 场景：两个不同用户映射至同一条带时，临界区内的锁持有次数必须恰好为一。 */
    @Test
    void collidingUsersAcquireSharedStripeOnlyOnce() {
        StripedLockManager locks = new StripedLockManager(1);
        ConcurrentFixture fixture = singleTradeFixture(locks, 1L, 2L, true);

        assertEquals(TradeResult.SETTLED,
                fixture.coordinator.accept(fixture.executions.getFirst()));

        assertEquals(1, fixture.maximumObservedHoldCount.get());
        assertEquals(1L, fixture.metrics.settledTradeCount());
    }

    /** 场景：同用户自成交即使其条带被其他线程持有，也必须在获取用户锁之前完成拒绝。 */
    @Test
    void sameUserSelfTradeRejectsBeforeWaitingForUserStripe() throws Exception {
        StripedLockManager locks = new StripedLockManager(8);
        ConcurrentFixture fixture = singleTradeFixture(locks, 1L, 1L, false);
        ReentrantLock userLock = locks.lockForUser(1L);
        ExecutorService executor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().daemon(true).name("self-trade-probe").factory());
        userLock.lock();
        try {
            Future<TradeResult> result = executor.submit(
                    () -> fixture.coordinator.accept(fixture.executions.getFirst()));

            assertEquals(TradeResult.REJECTED, result.get(2L, TimeUnit.SECONDS));
            assertEquals(TradeExecutionState.REJECTED, fixture.store.record(1L).state());
            assertEquals(1L, fixture.metrics.tradeRejectedCount());
        } finally {
            userLock.unlock();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /** 场景：十六个线程并发提交 A/B 与 B/A 成交时应全部在限定时间内完成且资金守恒。 */
    @Test
    void reversedCounterpartyWorkloadsFinishWithoutDeadlockAndSettleExactlyOnce() throws Exception {
        ConcurrentFixture fixture = workloadFixture();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(
                WORKER_COUNT,
                Thread.ofPlatform().daemon(true).name("settlement-worker-", 0L).factory());
        List<Future<List<TradeResult>>> futures = new ArrayList<>(WORKER_COUNT);
        try {
            for (int worker = 0; worker < WORKER_COUNT; worker++) {
                int fromIndex = worker * TRADES_PER_WORKER;
                futures.add(executor.submit(() -> {
                    await(start);
                    List<TradeResult> results = new ArrayList<>(TRADES_PER_WORKER);
                    for (int offset = 0; offset < TRADES_PER_WORKER; offset++) {
                        results.add(fixture.coordinator.accept(
                                fixture.executions.get(fromIndex + offset)));
                    }
                    return results;
                }));
            }
            start.countDown();

            int settledResults = 0;
            for (Future<List<TradeResult>> future : futures) {
                settledResults += future.get(10L, TimeUnit.SECONDS).stream()
                        .filter(result -> result == TradeResult.SETTLED)
                        .count();
            }

            int expectedTrades = WORKER_COUNT * TRADES_PER_WORKER;
            assertEquals(expectedTrades, settledResults);
            assertEquals(expectedTrades, fixture.metrics.settledTradeCount());
            assertEquals(0L, fixture.metrics.tradeRejectedCount());
            assertEquals(0, fixture.store.pendingCount());
            assertTrue(fixture.store.pendingTradeIds(fixture.executions.getFirst().buyOrderId()).isEmpty());
            assertNonNegative(fixture.ledger.balance(1L, BTC));
            assertNonNegative(fixture.ledger.balance(1L, USDT));
            assertNonNegative(fixture.ledger.balance(2L, BTC));
            assertNonNegative(fixture.ledger.balance(2L, USDT));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 创建一笔可用于锁顺序、条带碰撞或自成交探测的夹具。
     *
     * @param locks 条带锁管理器
     * @param buyerUserId 买方用户标识
     * @param sellerUserId 卖方用户标识
     * @param observeHoldCount 是否记录订单查询时当前条带锁持有次数
     * @return 包含一笔权威成交的夹具
     */
    private static ConcurrentFixture singleTradeFixture(
            StripedLockManager locks,
            long buyerUserId,
            long sellerUserId,
            boolean observeHoldCount) {
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(buyerUserId, BTC, 0L, sellerUserId == buyerUserId ? 1L : 0L);
        ledger.createBalance(buyerUserId, USDT, 0L, 10L);
        if (sellerUserId != buyerUserId) {
            ledger.createBalance(sellerUserId, BTC, 0L, 1L);
            ledger.createBalance(sellerUserId, USDT, 0L);
        }

        OrderContext buyer = OrderContext.fromSubmission(new OrderSubmission(
                11L, buyerUserId, OrderSide.BUY, BTC_USDT,
                1L, 10L, 10L, 1L, 0L));
        OrderContext seller = OrderContext.fromSubmission(new OrderSubmission(
                22L, sellerUserId, OrderSide.SELL, BTC_USDT,
                1L, 1L, 10L, 1L, 0L));
        java.util.concurrent.ConcurrentMap<Long, OrderContext> orders =
                new java.util.concurrent.ConcurrentHashMap<>();
        orders.put(buyer.orderId(), buyer);
        orders.put(seller.orderId(), seller);
        AtomicInteger maximumHoldCount = new AtomicInteger();
        OrderEngineMetrics metrics = new OrderEngineMetrics();
        TradeExecutionStore store = new TradeExecutionStore(8, 16);
        TradeSettlementCoordinator coordinator = new TradeSettlementCoordinator(
                ledger,
                new OrderStateMachine(8),
                store,
                locks,
                metrics,
                orderId -> {
                    if (observeHoldCount) {
                        maximumHoldCount.accumulateAndGet(
                                locks.lockForUser(buyerUserId).getHoldCount(), Math::max);
                    }
                    return orders.get(orderId);
                });
        TradeExecution execution = new TradeExecution(
                1L, buyer.orderId(), seller.orderId(), BTC_USDT,
                1L, 10L, 2L, 2L, 1L);
        return new ConcurrentFixture(
                ledger,
                metrics,
                store,
                coordinator,
                List.of(execution),
                maximumHoldCount);
    }

    /**
     * 创建两用户双向成交压力夹具，每笔成交拥有独立买卖订单。
     *
     * @return 含固定数量 A/B 和 B/A 成交的夹具
     */
    private static ConcurrentFixture workloadFixture() {
        int tradeCount = WORKER_COUNT * TRADES_PER_WORKER;
        int tradesPerDirection = tradeCount / 2;
        StripedLockManager locks = new StripedLockManager(8);
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, BTC, 1_000L, tradesPerDirection);
        ledger.createBalance(1L, USDT, 10_000L, tradesPerDirection * 10L);
        ledger.createBalance(2L, BTC, 1_000L, tradesPerDirection);
        ledger.createBalance(2L, USDT, 10_000L, tradesPerDirection * 10L);

        java.util.concurrent.ConcurrentMap<Long, OrderContext> orders =
                new java.util.concurrent.ConcurrentHashMap<>();
        List<TradeExecution> executions = new ArrayList<>(tradeCount);
        for (int index = 0; index < tradeCount; index++) {
            long tradeId = index + 1L;
            long buyerOrderId = 1_000L + index * 2L;
            long sellerOrderId = buyerOrderId + 1L;
            long buyerUserId = index % 2 == 0 ? 1L : 2L;
            long sellerUserId = index % 2 == 0 ? 2L : 1L;
            OrderContext buyer = OrderContext.fromSubmission(new OrderSubmission(
                    buyerOrderId, buyerUserId, OrderSide.BUY, BTC_USDT,
                    1L, 10L, 10L, 1L, 0L));
            OrderContext seller = OrderContext.fromSubmission(new OrderSubmission(
                    sellerOrderId, sellerUserId, OrderSide.SELL, BTC_USDT,
                    1L, 1L, 10L, 1L, 0L));
            orders.put(buyerOrderId, buyer);
            orders.put(sellerOrderId, seller);
            executions.add(new TradeExecution(
                    tradeId, buyerOrderId, sellerOrderId, BTC_USDT,
                    1L, 10L, 2L, 2L, tradeId));
        }

        OrderEngineMetrics metrics = new OrderEngineMetrics();
        TradeExecutionStore store = new TradeExecutionStore(tradeCount, tradeCount);
        TradeSettlementCoordinator coordinator = new TradeSettlementCoordinator(
                ledger,
                new OrderStateMachine(8),
                store,
                locks,
                metrics,
                orders::get);
        return new ConcurrentFixture(
                ledger,
                metrics,
                store,
                coordinator,
                List.copyOf(executions),
                new AtomicInteger());
    }

    /**
     * 在限定时间内等待并发条件成立。
     *
     * @param condition 待满足条件
     * @param timeout 最大等待时间
     * @throws InterruptedException 当前线程等待时被中断
     */
    private static void awaitCondition(BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true within " + timeout);
    }

    /**
     * 以限定时间等待启动信号。
     *
     * @param latch 工作线程启动门闩
     */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5L, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for concurrent start");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for concurrent start", interrupted);
        }
    }

    /**
     * 断言余额快照的可用和冻结字段均保持非负。
     *
     * @param balance 待检查余额快照
     */
    private static void assertNonNegative(BalanceSnapshot balance) {
        assertTrue(balance.available() >= 0L);
        assertTrue(balance.frozen() >= 0L);
    }

    /**
     * 保存并发测试共享组件。
     *
     * <p>线程安全：各组件遵循其生产锁和并发映射约束。</p>
     * <p>使用限制：仅供本测试类使用。</p>
     *
     * @param ledger 多资产账本
     * @param metrics 结算指标
     * @param store 成交存储
     * @param coordinator 成交协调器
     * @param executions 待提交成交
     * @param maximumObservedHoldCount 查询订单时观察到的最大锁持有次数
     */
    private record ConcurrentFixture(
            AccountLedger ledger,
            OrderEngineMetrics metrics,
            TradeExecutionStore store,
            TradeSettlementCoordinator coordinator,
            List<TradeExecution> executions,
            AtomicInteger maximumObservedHoldCount) { }
}
