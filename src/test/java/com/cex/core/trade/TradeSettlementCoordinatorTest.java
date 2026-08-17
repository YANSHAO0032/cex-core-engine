package com.cex.core.trade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.OrderContext;
import com.cex.core.order.OrderEngineMetrics;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStateMachine;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 验证成交协调器对双方订单、四个余额桶和成交终态的原子处理。
 *
 * <p>核心能力：覆盖成功、重复、确定拒绝、价格改善释放和订单缺失后的重试。</p>
 * <p>线程安全：每个场景使用独立内存组件；锁顺序压力由并发测试类单独覆盖。</p>
 * <p>使用限制：测试直接装配协调器，不经过后续任务提供的订单引擎门面。</p>
 */
class TradeSettlementCoordinatorTest {

    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);

    /** 场景：首次成交同时修改四个余额桶和两个订单，精确重复不得再次修改。 */
    @Test
    void oneExecutionUpdatesFourBalancesAndTwoOrdersOnce() {
        Fixture fixture = readyBuyerAndSeller(10L, 200L, 10L, 2L);
        TradeExecution execution = fixture.execution(1L, 2L, 200L, 2L, 2L);

        assertEquals(TradeResult.SETTLED, fixture.coordinator.accept(execution));
        assertEquals(TradeResult.DUPLICATE, fixture.coordinator.accept(execution));

        assertEquals(new BalanceSnapshot(2L, 0L), fixture.ledger.balance(fixture.buyerId, BTC));
        assertEquals(new BalanceSnapshot(800L, 0L), fixture.ledger.balance(fixture.buyerId, USDT));
        assertEquals(new BalanceSnapshot(8L, 0L), fixture.ledger.balance(fixture.sellerId, BTC));
        assertEquals(new BalanceSnapshot(200L, 0L), fixture.ledger.balance(fixture.sellerId, USDT));
        assertEquals(OrderStatus.PARTIALLY_FILLED, fixture.buyer.status());
        assertEquals(OrderStatus.PARTIALLY_FILLED, fixture.seller.status());
        assertEquals(1L, fixture.metrics.settledTradeCount());
        assertEquals(0, fixture.store.pendingCount());
    }

    /** 场景：订单数量校验失败时余额和双方累计量保持不变，成交进入确定拒绝终态。 */
    @Test
    void anyValidationFailureLeavesBalancesAndOrderFillsUncommitted() {
        Fixture fixture = readyBuyerAndSeller(10L, 200L, 10L, 2L);
        TradeExecution oversized = fixture.execution(1L, 20L, 200L, 2L, 2L);
        Map<AssetId, Long> totalsBefore = fixture.ledger.currentTotalAssets();
        BalanceSnapshot buyerQuoteBefore = fixture.ledger.balance(fixture.buyerId, USDT);
        BalanceSnapshot sellerBaseBefore = fixture.ledger.balance(fixture.sellerId, BTC);

        assertEquals(TradeResult.REJECTED, fixture.coordinator.accept(oversized));

        assertEquals(totalsBefore, fixture.ledger.currentTotalAssets());
        assertEquals(buyerQuoteBefore, fixture.ledger.balance(fixture.buyerId, USDT));
        assertEquals(sellerBaseBefore, fixture.ledger.balance(fixture.sellerId, BTC));
        assertEquals(0L, fixture.buyer.cumulativeBaseFilled());
        assertEquals(0L, fixture.seller.cumulativeBaseFilled());
        assertEquals(TradeExecutionState.REJECTED, fixture.store.record(1L).state());
        assertEquals(1L, fixture.metrics.tradeRejectedCount());
    }

    /** 场景：卖方准备在买方准备之后失败，也不得只推进买方订单。 */
    @Test
    void sellerValidationFailureAfterBuyerPreparationLeavesBothOrdersUnchanged() {
        Fixture fixture = readyBuyerAndSeller(10L, 200L, 1L, 1L);
        TradeExecution execution = fixture.execution(1L, 2L, 200L, 2L, 2L);

        assertEquals(TradeResult.REJECTED, fixture.coordinator.accept(execution));

        assertEquals(0L, fixture.buyer.cumulativeBaseFilled());
        assertEquals(0L, fixture.seller.cumulativeBaseFilled());
        assertEquals(OrderStatus.NEW, fixture.buyer.status());
        assertEquals(OrderStatus.NEW, fixture.seller.status());
        assertTrue(fixture.ledger.allAssetInvariantsHold());
    }

    /** 场景：双订单准备成功但账本冻结额不足时，双方订单和所有余额仍不得修改。 */
    @Test
    void ledgerValidationFailureAfterBothOrderPreparationsLeavesEverythingUnchanged() {
        Fixture fixture = readyBuyerAndSeller(10L, 200L, 10L, 2L);
        withUserLock(fixture.locks, fixture.sellerId,
                () -> fixture.ledger.unfreezeLocked(fixture.sellerId, BTC, 1L));
        BalanceSnapshot buyerQuoteBefore = fixture.ledger.balance(fixture.buyerId, USDT);
        BalanceSnapshot sellerBaseBefore = fixture.ledger.balance(fixture.sellerId, BTC);

        assertEquals(TradeResult.REJECTED,
                fixture.coordinator.accept(fixture.execution(1L, 2L, 200L, 2L, 2L)));

        assertEquals(buyerQuoteBefore, fixture.ledger.balance(fixture.buyerId, USDT));
        assertEquals(sellerBaseBefore, fixture.ledger.balance(fixture.sellerId, BTC));
        assertEquals(0L, fixture.buyer.cumulativeBaseFilled());
        assertEquals(0L, fixture.seller.cumulativeBaseFilled());
        assertTrue(fixture.ledger.allAssetInvariantsHold());
    }

    /** 场景：最终买方成交必须在同一账本变更内支付成交额并释放未花费报价预留。 */
    @Test
    void finalFillAtomicallyReleasesBuyerPriceImprovement() {
        Fixture fixture = readyBuyerAndSeller(10L, 1_000L, 10L, 10L);
        TradeExecution execution = fixture.execution(1L, 10L, 950L, 2L, 2L);

        assertEquals(TradeResult.SETTLED, fixture.coordinator.accept(execution));

        assertEquals(new BalanceSnapshot(50L, 0L),
                fixture.ledger.balance(fixture.buyerId, USDT));
        assertEquals(OrderStatus.FILLED, fixture.buyer.status());
        assertEquals(OrderStatus.FILLED, fixture.seller.status());
        assertEquals(0L, fixture.buyer.remainingReservedAmount());
        assertTrue(fixture.ledger.allAssetInvariantsHold());
    }

    /** 场景：确定拒绝应同时消费双方下一序号，使后一权威成交可以继续结算。 */
    @Test
    void deterministicRejectionConsumesBothSequenceReferencesExactlyOnce() {
        Fixture fixture = readyBuyerAndSeller(10L, 400L, 10L, 4L);
        TradeExecution rejected = fixture.execution(1L, 20L, 200L, 2L, 2L);

        assertEquals(TradeResult.REJECTED, fixture.coordinator.accept(rejected));
        assertEquals(TradeResult.DUPLICATE, fixture.coordinator.accept(rejected));
        assertEquals(2L, fixture.buyer.lastAppliedSequence());
        assertEquals(2L, fixture.seller.lastAppliedSequence());
        assertEquals(1L, fixture.metrics.tradeRejectedCount());

        TradeExecution next = fixture.execution(2L, 2L, 200L, 3L, 3L);
        assertEquals(TradeResult.SETTLED, fixture.coordinator.accept(next));
        assertEquals(2L, fixture.buyer.cumulativeBaseFilled());
        assertEquals(2L, fixture.seller.cumulativeBaseFilled());
        assertEquals(1L, fixture.metrics.settledTradeCount());
    }

    /** 场景：成交先于订单可见时保持挂起，双方上下文就绪后可通过订单索引重试结算。 */
    @Test
    void retryPendingForOrderSettlesExecutionAfterBothContextsBecomeVisible() {
        Fixture fixture = readyBuyerAndSeller(10L, 200L, 10L, 2L);
        fixture.orders.clear();
        TradeExecution execution = fixture.execution(1L, 2L, 200L, 2L, 2L);

        assertEquals(TradeResult.PENDING, fixture.coordinator.accept(execution));
        assertEquals(1, fixture.store.pendingCount());

        fixture.orders.put(fixture.buyer.orderId(), fixture.buyer);
        fixture.coordinator.retryPendingForOrder(fixture.buyer.orderId());
        assertEquals(TradeExecutionState.PENDING, fixture.store.record(1L).state());

        fixture.orders.put(fixture.seller.orderId(), fixture.seller);
        fixture.coordinator.retryPendingForOrder(fixture.seller.orderId());
        assertEquals(TradeExecutionState.SETTLED, fixture.store.record(1L).state());
        assertEquals(1L, fixture.metrics.settledTradeCount());
    }

    /**
     * 创建双方订单、余额和冻结额均已准备完毕的协调器夹具。
     *
     * @param buyerBaseQuantity 买单原始基础数量
     * @param buyerQuoteReserve 买单报价资产预留
     * @param sellerBaseQuantity 卖单原始基础数量
     * @param sellerBaseReserve 卖单基础资产预留
     * @return 可直接接受成交的独立夹具
     */
    private static Fixture readyBuyerAndSeller(
            long buyerBaseQuantity,
            long buyerQuoteReserve,
            long sellerBaseQuantity,
            long sellerBaseReserve) {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = new AccountLedger(locks);
        long buyerId = 1L;
        long sellerId = 2L;
        ledger.createBalance(buyerId, BTC, 0L);
        ledger.createBalance(buyerId, USDT, 1_000L);
        ledger.createBalance(sellerId, BTC, 10L);
        ledger.createBalance(sellerId, USDT, 0L);
        withUserLock(locks, buyerId,
                () -> ledger.freezeLocked(buyerId, USDT, buyerQuoteReserve));
        withUserLock(locks, sellerId,
                () -> ledger.freezeLocked(sellerId, BTC, sellerBaseReserve));

        OrderContext buyer = OrderContext.fromSubmission(new OrderSubmission(
                11L, buyerId, OrderSide.BUY, BTC_USDT,
                buyerBaseQuantity, buyerQuoteReserve, buyerQuoteReserve, 1L, 0L));
        OrderContext seller = OrderContext.fromSubmission(new OrderSubmission(
                22L, sellerId, OrderSide.SELL, BTC_USDT,
                sellerBaseQuantity, sellerBaseReserve, 1_000L, 1L, 0L));
        ConcurrentMap<Long, OrderContext> orders = new ConcurrentHashMap<>();
        orders.put(buyer.orderId(), buyer);
        orders.put(seller.orderId(), seller);
        OrderEngineMetrics metrics = new OrderEngineMetrics();
        TradeExecutionStore store = new TradeExecutionStore(32, 64);
        TradeSettlementCoordinator coordinator = new TradeSettlementCoordinator(
                ledger,
                new OrderStateMachine(32),
                store,
                locks,
                metrics,
                orders::get);
        return new Fixture(
                buyerId, sellerId, locks, ledger, buyer, seller, orders, metrics, store, coordinator);
    }

    /**
     * 在一个用户的条带锁内执行测试准备动作。
     *
     * @param locks 条带锁管理器
     * @param userId 用户标识
     * @param action 准备动作
     */
    private static void withUserLock(StripedLockManager locks, long userId, Runnable action) {
        ReentrantLock lock = locks.lockForUser(userId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 保存一个测试所需的全部独立组件。
     *
     * <p>线程安全：订单索引可并发更新；其余可变组件遵循生产锁约束。</p>
     * <p>使用限制：仅供本测试类使用。</p>
     *
     * @param buyerId 买方用户标识
     * @param sellerId 卖方用户标识
     * @param locks 条带锁管理器
     * @param ledger 多资产账本
     * @param buyer 买单上下文
     * @param seller 卖单上下文
     * @param orders 订单索引
     * @param metrics 指标
     * @param store 成交存储
     * @param coordinator 成交协调器
     */
    private record Fixture(
            long buyerId,
            long sellerId,
            StripedLockManager locks,
            AccountLedger ledger,
            OrderContext buyer,
            OrderContext seller,
            ConcurrentMap<Long, OrderContext> orders,
            OrderEngineMetrics metrics,
            TradeExecutionStore store,
            TradeSettlementCoordinator coordinator) {

        /**
         * 创建指向夹具双方订单的权威成交。
         *
         * @param tradeId 成交标识
         * @param baseQuantity 基础资产成交数量
         * @param quoteQuantity 报价资产成交数量
         * @param buySequence 买单权威序号
         * @param sellSequence 卖单权威序号
         * @return 不可变测试成交
         */
        private TradeExecution execution(
                long tradeId,
                long baseQuantity,
                long quoteQuantity,
                long buySequence,
                long sellSequence) {
            return new TradeExecution(
                    tradeId,
                    buyer.orderId(),
                    seller.orderId(),
                    BTC_USDT,
                    baseQuantity,
                    quoteQuantity,
                    buySequence,
                    sellSequence,
                    tradeId);
        }
    }
}
