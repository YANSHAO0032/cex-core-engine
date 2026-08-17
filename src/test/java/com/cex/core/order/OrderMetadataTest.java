package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.trade.TradeMetadataMismatchException;
import com.cex.core.trade.TradeResult;
import org.junit.jupiter.api.Test;

/**
 * 验证强类型提交与成交引用的不可变元数据冲突拦截。
 *
 * <p>核心能力：覆盖重复提交的交易对/方向冲突，以及成交的交易对和买卖订单引用冲突。</p>
 * <p>线程安全：每个用例使用独立引擎，不共享可变状态。</p>
 * <p>使用限制：仅验证入口元数据，不覆盖外部序列化。</p>
 */
class OrderMetadataTest {
    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试替代基础资产。 */
    private static final AssetId ETH = new AssetId("ETH");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** BTC/USDT 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** ETH/USDT 冲突交易对。 */
    private static final TradingPair ETH_USDT = new TradingPair(ETH, USDT);
    /** 买方用户标识。 */
    private static final long BUYER_ID = 202L;
    /** 卖方用户标识。 */
    private static final long SELLER_ID = 303L;
    /** 买单标识。 */
    private static final long BUY_ORDER_ID = 101L;
    /** 卖单标识。 */
    private static final long SELL_ORDER_ID = 102L;

    /** 场景：相同订单标识绑定不同交易对或方向时必须拒绝。 */
    @Test
    void duplicateSubmissionWithDifferentPairOrSideFailsValidation() {
        try (Fixture fixture = new Fixture()) {
            fixture.engine.submit(fixture.buySubmission());

            OrderSubmission mismatchedPair = new OrderSubmission(
                    BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, ETH_USDT,
                    10L, 1_000L, 1_000L, 1L, 1L);
            OrderSubmission mismatchedSide = new OrderSubmission(
                    BUY_ORDER_ID, BUYER_ID, OrderSide.SELL, BTC_USDT,
                    10L, 1_000L, 1_000L, 1L, 1L);

            assertThrows(OrderMetadataMismatchException.class,
                    () -> fixture.engine.submit(mismatchedPair));
            assertThrows(OrderMetadataMismatchException.class,
                    () -> fixture.engine.submit(mismatchedSide));
            assertEquals(OrderStatus.NEW,
                    fixture.engine.order(BUY_ORDER_ID).status());
        }
    }

    /** 场景：成交交易对与订单交易对不一致时必须整笔拒绝且资产不变。 */
    @Test
    void mismatchedTradePairIsRejectedWithoutAssetChange() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution mismatch = new TradeExecution(
                    1L, BUY_ORDER_ID, SELL_ORDER_ID, ETH_USDT,
                    10L, 1_000L, 2L, 2L, 2L);

            assertEquals(TradeResult.REJECTED, fixture.engine.onTrade(mismatch));
            assertEquals(new BalanceSnapshot(0L, 1_000L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertEquals(new BalanceSnapshot(0L, 10L),
                    fixture.ledger.balance(SELLER_ID, BTC));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：成交买卖订单引用对调时必须整笔拒绝且不得推进任一订单。 */
    @Test
    void mismatchedOrderSideReferencesAreRejected() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution mismatch = new TradeExecution(
                    2L, SELL_ORDER_ID, BUY_ORDER_ID, BTC_USDT,
                    1L, 100L, 2L, 2L, 2L);

            assertEquals(TradeResult.REJECTED, fixture.engine.onTrade(mismatch));
            assertEquals(0L, fixture.engine.order(BUY_ORDER_ID).cumulativeBaseFilled());
            assertEquals(0L, fixture.engine.order(SELL_ORDER_ID).cumulativeBaseFilled());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：相同成交标识绑定不同载荷时应抛出协议冲突并累计专用指标。 */
    @Test
    void conflictingTradeIdPayloadIncrementsMetadataConflictMetric() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution accepted = new TradeExecution(
                    3L, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                    1L, 100L, 3L, 3L, 3L);
            TradeExecution conflict = new TradeExecution(
                    3L, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                    1L, 101L, 3L, 3L, 3L);

            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(accepted));
            assertThrows(TradeMetadataMismatchException.class,
                    () -> fixture.engine.onTrade(conflict));

            assertEquals(1L,
                    fixture.engine.metrics().tradeMetadataConflictCount());
            assertEquals(1, fixture.engine.metrics().pendingTradeCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 保存元数据场景的真实账本与强类型引擎。 */
    private static final class Fixture implements AutoCloseable {
        /** 多资产账户账本。 */
        private final AccountLedger ledger;
        /** 被测强类型订单引擎。 */
        private final OrderEngine engine;

        /** 创建买卖双方余额和独立引擎。 */
        private Fixture() {
            ledger = new AccountLedger(new StripedLockManager(16));
            ledger.createBalance(BUYER_ID, BTC, 0L);
            ledger.createBalance(BUYER_ID, ETH, 0L);
            ledger.createBalance(BUYER_ID, USDT, 1_000L);
            ledger.createBalance(SELLER_ID, BTC, 10L);
            ledger.createBalance(SELLER_ID, ETH, 10L);
            ledger.createBalance(SELLER_ID, USDT, 0L);
            engine = new OrderEngine(ledger);
        }

        /** 提交买卖双方强类型订单。 */
        private void submitBoth() {
            engine.submit(buySubmission());
            engine.submit(sellSubmission());
        }

        /** @return 固定买单提交 */
        private OrderSubmission buySubmission() {
            return new OrderSubmission(
                    BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                    10L, 1_000L, 1_000L, 1L, 1L);
        }

        /** @return 固定卖单提交 */
        private OrderSubmission sellSubmission() {
            return new OrderSubmission(
                    SELL_ORDER_ID, SELLER_ID, OrderSide.SELL, BTC_USDT,
                    10L, 10L, 1_000L, 1L, 1L);
        }

        /** 关闭引擎持有的审批线程资源。 */
        @Override
        public void close() {
            engine.close();
        }
    }
}
