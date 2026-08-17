package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeResult;
import org.junit.jupiter.api.Test;

/**
 * 验证撤单确认与成交按权威序号裁决终态，且资金不被迟到事件反向修改。
 *
 * <p>核心能力：覆盖撤单确认后的高序号成交拒绝，以及成交完成后的撤单请求幂等终止。</p>
 * <p>线程安全：每个场景使用独立引擎，真实双用户锁路径完成结算。</p>
 * <p>使用限制：不模拟订单簿，仅消费外部权威成交和撤单确认。</p>
 */
class TerminalConflictTest {
    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 买方用户标识。 */
    private static final long BUYER_ID = 1L;
    /** 卖方用户标识。 */
    private static final long SELLER_ID = 2L;
    /** 买单标识。 */
    private static final long BUY_ORDER_ID = 11L;
    /** 卖单标识。 */
    private static final long SELL_ORDER_ID = 22L;

    /** 场景：完全成交后迟到撤单请求不得解冻或翻转已结算终态。 */
    @Test
    void cancelRequestAfterFillDoesNotUnfreeze() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            assertEquals(TradeResult.SETTLED,
                    fixture.engine.onTrade(fixture.execution(1L, 2L, 2L)));

            assertEquals(CancelRequestResult.ALREADY_TERMINAL,
                    fixture.engine.requestCancel(
                            new CancelRequest(90L, BUY_ORDER_ID, 3L)));
            assertEquals(OrderStatus.FILLED, fixture.buyOrder().status());
            assertEquals(new BalanceSnapshot(0L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：撤单确认后较高序号成交必须拒绝且不得改变双方资产。 */
    @Test
    void fillAfterCancelSequenceIsRejectedWithoutAssetChange() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            fixture.engine.requestCancel(
                    new CancelRequest(90L, BUY_ORDER_ID, 2L));
            fixture.engine.onCancelConfirmed(
                    new CancelConfirmation(90L, BUY_ORDER_ID, 2L, 3L));
            fixture.engine.onCancelConfirmed(
                    new CancelConfirmation(90L, BUY_ORDER_ID, 2L, 3L));

            TradeResult result = fixture.engine.onTrade(
                    fixture.execution(2L, 3L, 2L));

            assertEquals(TradeResult.REJECTED, result);
            assertEquals(TradeExecutionState.REJECTED,
                    fixture.engine.trade(2L).state());
            assertEquals(OrderStatus.CANCELED, fixture.buyOrder().status());
            assertEquals(new BalanceSnapshot(1_000L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertEquals(new BalanceSnapshot(0L, 10L),
                    fixture.ledger.balance(SELLER_ID, BTC));
            assertEquals(1L, fixture.engine.metrics().pendingCancelCount());
            assertEquals(1L,
                    fixture.engine.metrics().staleCancelConfirmationCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 保存终态裁决场景的账本与引擎。 */
    private static final class Fixture implements AutoCloseable {
        /** 多资产账户账本。 */
        private final AccountLedger ledger =
                new AccountLedger(new StripedLockManager(16));
        /** 被测强类型订单引擎。 */
        private final OrderEngine engine;

        /** 创建并初始化买卖双方资产。 */
        private Fixture() {
            ledger.createBalance(BUYER_ID, BTC, 0L);
            ledger.createBalance(BUYER_ID, USDT, 1_000L);
            ledger.createBalance(SELLER_ID, BTC, 10L);
            ledger.createBalance(SELLER_ID, USDT, 0L);
            engine = new OrderEngine(ledger);
        }

        /** 提交固定买卖双方订单。 */
        private void submitBoth() {
            engine.submit(new OrderSubmission(
                    BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                    10L, 1_000L, 1_000L, 1L, 1L));
            engine.submit(new OrderSubmission(
                    SELL_ORDER_ID, SELLER_ID, OrderSide.SELL, BTC_USDT,
                    10L, 10L, 1_000L, 1L, 1L));
        }

        /**
         * 构造固定全量成交。
         *
         * @param tradeId 成交标识
         * @param buySequence 买单权威序号
         * @param sellSequence 卖单权威序号
         * @return 全量成交输入
         */
        private TradeExecution execution(
                long tradeId, long buySequence, long sellSequence) {
            return new TradeExecution(
                    tradeId, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                    10L, 1_000L, buySequence, sellSequence, 4L);
        }

        /** @return 当前买单上下文 */
        private OrderContext buyOrder() {
            return engine.order(BUY_ORDER_ID);
        }

        /** 关闭引擎持有的审批线程资源。 */
        @Override
        public void close() {
            engine.close();
        }
    }
}
