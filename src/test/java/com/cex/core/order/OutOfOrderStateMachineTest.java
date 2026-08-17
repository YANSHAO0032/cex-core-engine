package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * 验证强类型订单状态机在创建前事件、重放、并发和序号冲突下的收敛行为。
 *
 * <p>核心能力：覆盖成交存储重试、创建前撤单确认转移、成交幂等和未来序号冲突拦截。</p>
 * <p>线程安全：并发用例使用同步门闩协调重放，真实资金变更由固定顺序双用户锁保护。</p>
 * <p>使用限制：仅覆盖内存引擎的代表性乱序组合，不模拟持久化恢复。</p>
 */
class OutOfOrderStateMachineTest {
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
    private static final long BUY_ORDER_ID = 9L;
    /** 卖单标识。 */
    private static final long SELL_ORDER_ID = 10L;

    /** 场景：成交早于双方创建到达时，应在双方发布后自动结算。 */
    @Test
    void tradeBeforeCreateConvergesToFilled() {
        try (Fixture fixture = new Fixture()) {
            TradeExecution execution = fixture.fullExecution(1L, 2L, 2L);

            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(execution));
            fixture.engine.submit(fixture.buySubmission());
            assertEquals(TradeExecutionState.PENDING, fixture.engine.trade(1L).state());
            fixture.engine.submit(fixture.sellSubmission());

            assertEquals(OrderStatus.FILLED, fixture.buyOrder().status());
            assertEquals(OrderStatus.FILLED, fixture.sellOrder().status());
            assertEquals(new BalanceSnapshot(0L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertEquals(1L, fixture.engine.metrics().settledTradeCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：撤单确认先于创建到达时，应在同请求登记后解冻并取消。 */
    @Test
    void cancelConfirmationBeforeCreateConvergesToCanceled() {
        try (Fixture fixture = new Fixture()) {
            fixture.engine.onCancelConfirmed(
                    new CancelConfirmation(90L, BUY_ORDER_ID, 2L, 1L));
            fixture.engine.submit(fixture.buySubmission());

            assertEquals(CancelRequestResult.SUBMITTED,
                    fixture.engine.requestCancel(
                            new CancelRequest(90L, BUY_ORDER_ID, 2L)));

            assertEquals(OrderStatus.CANCELED, fixture.buyOrder().status());
            assertEquals(new BalanceSnapshot(1_000L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：大量重复成交和重复创建不得重复结算或冻结。 */
    @Test
    void duplicateInputsStillReconcileExactlyOnce() {
        try (Fixture fixture = new Fixture()) {
            TradeExecution execution = fixture.fullExecution(1L, 2L, 2L);
            for (int i = 0; i < 20; i++) {
                fixture.engine.onTrade(execution);
            }
            for (int i = 0; i < 10; i++) {
                fixture.engine.submit(fixture.buySubmission());
                fixture.engine.submit(fixture.sellSubmission());
            }

            assertEquals(OrderStatus.FILLED, fixture.buyOrder().status());
            assertEquals(1L, fixture.engine.metrics().settledTradeCount());
            assertEquals(0, fixture.engine.pendingTradeCount());
            assertTrue(fixture.engine.metrics().duplicateTradeCount() >= 19L);
            assertEquals(0, fixture.engine.metrics().pendingTradeCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 场景：32 个线程首次并发提交同一成交时，业务结果与重复指标都必须精确收敛为 1+31。 */
    @Test
    void concurrentFirstTradeCountsEveryDuplicateExactlyOnce() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution execution = fixture.fullExecution(1L, 2L, 2L);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(32);
            try {
                List<Future<TradeResult>> futures = new ArrayList<>();
                for (int index = 0; index < 32; index++) {
                    futures.add(executor.submit(() -> {
                        start.await();
                        return fixture.engine.onTrade(execution);
                    }));
                }

                start.countDown();
                int settled = 0;
                int duplicates = 0;
                for (Future<TradeResult> future : futures) {
                    TradeResult result = future.get(5L, TimeUnit.SECONDS);
                    if (result == TradeResult.SETTLED) {
                        settled++;
                    } else if (result == TradeResult.DUPLICATE) {
                        duplicates++;
                    }
                }

                assertEquals(1, settled);
                assertEquals(31, duplicates);
                assertEquals(1L, fixture.engine.metrics().settledTradeCount());
                assertEquals(31L, fixture.engine.metrics().duplicateTradeCount());
                assertTrue(fixture.ledger.allAssetInvariantsHold());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
            }
        }
    }

    /** 场景：同一未来序号成交的顺序重放不得重复累计序号空洞。 */
    @Test
    void sequentialDuplicateGapCountsSequenceGapOnce() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution execution = fixture.fullExecution(1L, 3L, 3L);

            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(execution));
            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(execution));

            assertEquals(1L, fixture.engine.metrics().sequenceGapCount());
            assertEquals(1L, fixture.engine.metrics().duplicateTradeCount());
            assertEquals(1, fixture.engine.metrics().pendingTradeCount());
        }
    }

    /** 场景：同一未来序号成交的并发首次投递只累计一个空洞和 31 个重复。 */
    @Test
    void concurrentDuplicateGapCountsSequenceGapOnce() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            TradeExecution execution = fixture.fullExecution(1L, 3L, 3L);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(32);
            try {
                List<Future<TradeResult>> futures = new ArrayList<>();
                for (int index = 0; index < 32; index++) {
                    futures.add(executor.submit(() -> {
                        start.await();
                        return fixture.engine.onTrade(execution);
                    }));
                }

                start.countDown();
                for (Future<TradeResult> future : futures) {
                    assertEquals(TradeResult.PENDING,
                            future.get(5L, TimeUnit.SECONDS));
                }
                assertEquals(1L, fixture.engine.metrics().sequenceGapCount());
                assertEquals(31L, fixture.engine.metrics().duplicateTradeCount());
                assertEquals(1, fixture.engine.metrics().pendingTradeCount());
            } finally {
                start.countDown();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * 场景：相同成交的并发重放与调用线程中断不应妨碍最终收敛。
     *
     * @throws Exception 并发线程等待失败时抛出
     */
    @Test
    void concurrentDuplicatesAndInterruptComplete() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            Thread.currentThread().interrupt();
            TradeExecution execution = fixture.fullExecution(1L, 2L, 2L);
            fixture.engine.onTrade(execution);
            assertTrue(Thread.interrupted());

            CountDownLatch start = new CountDownLatch(1);
            Thread[] threads = new Thread[32];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        start.await();
                        fixture.engine.onTrade(execution);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        fail(interrupted);
                    }
                });
                threads[i].start();
            }
            start.countDown();
            for (Thread thread : threads) {
                thread.join(5_000L);
                assertFalse(thread.isAlive());
            }

            assertEquals(OrderStatus.FILLED, fixture.buyOrder().status());
            assertEquals(1L, fixture.engine.metrics().settledTradeCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        } finally {
            Thread.interrupted();
        }
    }

    /** 场景：相同订单未来序号绑定不同权威载荷时必须拒绝。 */
    @Test
    void sequencePayloadConflictIsRejected() {
        try (Fixture fixture = new Fixture()) {
            fixture.submitBoth();
            assertEquals(TradeResult.PENDING,
                    fixture.engine.onTrade(fixture.fullExecution(1L, 3L, 3L)));

            assertThrows(TradeSequenceConflictException.class,
                    () -> fixture.engine.onCancelConfirmed(
                            new CancelConfirmation(90L, BUY_ORDER_ID, 3L, 3L)));
            assertEquals(0L, fixture.buyOrder().cumulativeBaseFilled());
            assertTrue(fixture.engine.metrics().sequenceGapCount() > 0L);
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /** 保存乱序状态机测试使用的真实账本与引擎。 */
    private static final class Fixture implements AutoCloseable {
        /** 多资产账户账本。 */
        private final AccountLedger ledger =
                new AccountLedger(new StripedLockManager(16));
        /** 被测强类型订单引擎。 */
        private final OrderEngine engine;

        /** 创建买卖双方资产与引擎。 */
        private Fixture() {
            ledger.createBalance(BUYER_ID, BTC, 0L);
            ledger.createBalance(BUYER_ID, USDT, 1_000L);
            ledger.createBalance(SELLER_ID, BTC, 10L);
            ledger.createBalance(SELLER_ID, USDT, 0L);
            engine = new OrderEngine(ledger);
        }

        /** 提交固定买卖双方订单。 */
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

        /**
         * 构造固定全量成交。
         *
         * @param tradeId 成交标识
         * @param buySequence 买单权威序号
         * @param sellSequence 卖单权威序号
         * @return 全量成交输入
         */
        private TradeExecution fullExecution(
                long tradeId, long buySequence, long sellSequence) {
            return new TradeExecution(
                    tradeId, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                    10L, 1_000L, buySequence, sellSequence, 2L);
        }

        /** @return 当前买单上下文 */
        private OrderContext buyOrder() {
            return engine.order(BUY_ORDER_ID);
        }

        /** @return 当前卖单上下文 */
        private OrderContext sellOrder() {
            return engine.order(SELL_ORDER_ID);
        }

        /** 关闭引擎持有的审批线程资源。 */
        @Override
        public void close() {
            engine.close();
        }
    }
}
