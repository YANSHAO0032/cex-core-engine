package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 验证双边成交在创建缺失、双方序号空洞和撤单终态下按权威顺序收敛。
 *
 * <p>测试只通过 {@link OrderEngine} 强类型门面驱动真实协调器，不建立第二套撮合或结算逻辑。</p>
 */
class CounterpartyOutOfOrderTest {
    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试使用的基础/报价交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 买方用户标识。 */
    private static final long BUYER_ID = 101L;
    /** 卖方用户标识。 */
    private static final long SELLER_ID = 202L;
    /** 买单标识。 */
    private static final long BUY_ORDER_ID = 1_001L;
    /** 卖单标识。 */
    private static final long SELL_ORDER_ID = 2_002L;

    /** 测试结束时需要关闭的引擎资源。 */
    private final List<OrderEngine> engines = new ArrayList<>();

    /** 关闭场景内引擎拥有的异步审批资源。 */
    @AfterEach
    void closeEngines() {
        engines.forEach(OrderEngine::close);
    }

    /** 场景：成交早于双方创建时只驻留成交存储，双方就绪后自动结算。 */
    @Test
    void tradeBeforeBothSubmissionsSettlesAfterBothOrdersBecomeReady() {
        Fixture fixture = fixture();

        assertEquals(TradeResult.PENDING,
                fixture.engine.onTrade(fixture.execution(1L, 2L, 200L, 2L, 2L)));
        fixture.engine.submit(fixture.buySubmission());
        assertEquals(TradeExecutionState.PENDING, fixture.engine.trade(1L).state());
        assertEquals(0L, fixture.buyOrder().cumulativeBaseFilled());

        fixture.engine.submit(fixture.sellSubmission());

        assertEquals(TradeExecutionState.SETTLED, fixture.engine.trade(1L).state());
        assertEquals(2L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(2L, fixture.sellOrder().cumulativeBaseFilled());
        assertEquals(0, fixture.engine.pendingTradeCount());
    }

    /** 场景：较高序号先到时不越过空洞，低序号终结后持续重试双方新暴露的头部。 */
    @Test
    void futureSequenceWaitsForGapOnBothOrders() {
        Fixture fixture = readyFixture();

        assertEquals(TradeResult.PENDING,
                fixture.engine.onTrade(fixture.execution(2L, 1L, 100L, 3L, 3L)));
        assertEquals(0L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(TradeResult.SETTLED,
                fixture.engine.onTrade(fixture.execution(1L, 1L, 100L, 2L, 2L)));

        assertEquals(2L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(2L, fixture.sellOrder().cumulativeBaseFilled());
        assertEquals(TradeExecutionState.SETTLED, fixture.engine.trade(1L).state());
        assertEquals(TradeExecutionState.SETTLED, fixture.engine.trade(2L).state());
        assertEquals(0, fixture.engine.pendingTradeCount());
        assertEquals(2L, fixture.engine.metrics().partialFillCount());
        assertEquals(0, fixture.engine.metrics().pendingTradeCount());
    }

    /** 场景：买卖序号形成不同空洞时，任一单边下一事件都不得被独立提交。 */
    @Test
    void crossedCounterpartyGapsNeverCommitOneSide() {
        Fixture fixture = readyFixture();

        assertEquals(TradeResult.PENDING,
                fixture.engine.onTrade(fixture.execution(1L, 1L, 100L, 2L, 3L)));
        assertEquals(TradeResult.PENDING,
                fixture.engine.onTrade(fixture.execution(2L, 1L, 100L, 3L, 2L)));

        assertEquals(0L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(0L, fixture.sellOrder().cumulativeBaseFilled());
        assertEquals(1L, fixture.buyOrder().lastAppliedSequence());
        assertEquals(1L, fixture.sellOrder().lastAppliedSequence());
        assertEquals(2, fixture.engine.pendingTradeCount());
        assertEquals(new BalanceSnapshot(0L, 1_000L),
                fixture.ledger.balance(BUYER_ID, USDT));
        assertEquals(new BalanceSnapshot(0L, 10L),
                fixture.ledger.balance(SELLER_ID, BTC));
    }

    /** 场景：撤单确认提交后，更高成交只能双边消费序号并拒绝，不能改写任何资产。 */
    @Test
    void higherTradeAfterCancelIsRejectedWithoutAssetChange() {
        Fixture fixture = readyFixture();
        fixture.engine.requestCancel(new CancelRequest(90L, BUY_ORDER_ID, 10L));
        fixture.engine.onCancelConfirmed(
                new CancelConfirmation(90L, BUY_ORDER_ID, 2L, 20L));

        assertEquals(TradeResult.REJECTED,
                fixture.engine.onTrade(fixture.execution(1L, 1L, 100L, 3L, 2L)));

        assertEquals(OrderStatus.CANCELED, fixture.buyOrder().status());
        assertEquals(0L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(0L, fixture.sellOrder().cumulativeBaseFilled());
        assertEquals(TradeExecutionState.REJECTED, fixture.engine.trade(1L).state());
        assertEquals(new BalanceSnapshot(1_000L, 0L),
                fixture.ledger.balance(BUYER_ID, USDT));
        assertEquals(new BalanceSnapshot(0L, 10L),
                fixture.ledger.balance(SELLER_ID, BTC));
        assertEquals(0, fixture.engine.pendingTradeCount());
        assertTrue(fixture.ledger.allAssetInvariantsHold());
    }

    private Fixture readyFixture() {
        Fixture fixture = fixture();
        fixture.engine.submit(fixture.buySubmission());
        fixture.engine.submit(fixture.sellSubmission());
        return fixture;
    }

    private Fixture fixture() {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(BUYER_ID, BTC, 0L);
        ledger.createBalance(BUYER_ID, USDT, 1_000L);
        ledger.createBalance(SELLER_ID, BTC, 10L);
        ledger.createBalance(SELLER_ID, USDT, 0L);
        OrderEngine engine = new OrderEngine(ledger);
        engines.add(engine);
        return new Fixture(ledger, engine);
    }

    /** 保存双边乱序场景中的真实依赖和权威输入工厂。 */
    private static final class Fixture {
        /** 场景使用的多资产账本。 */
        private final AccountLedger ledger;
        /** 场景使用的强类型订单引擎。 */
        private final OrderEngine engine;

        private Fixture(AccountLedger ledger, OrderEngine engine) {
            this.ledger = ledger;
            this.engine = engine;
        }

        private OrderSubmission buySubmission() {
            return new OrderSubmission(
                    BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                    10L, 1_000L, 1_000L, 1L, 10L);
        }

        private OrderSubmission sellSubmission() {
            return new OrderSubmission(
                    SELL_ORDER_ID, SELLER_ID, OrderSide.SELL, BTC_USDT,
                    10L, 10L, 1_000L, 1L, 10L);
        }

        private TradeExecution execution(
                long tradeId,
                long baseQuantity,
                long quoteQuantity,
                long buySequence,
                long sellSequence) {
            return new TradeExecution(
                    tradeId, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                    baseQuantity, quoteQuantity,
                    buySequence, sellSequence, 30L + tradeId);
        }

        private OrderContext buyOrder() {
            return engine.order(BUY_ORDER_ID);
        }

        private OrderContext sellOrder() {
            return engine.order(SELL_ORDER_ID);
        }
    }
}
