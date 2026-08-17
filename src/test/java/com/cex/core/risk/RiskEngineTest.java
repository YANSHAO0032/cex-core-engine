package com.cex.core.risk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import com.cex.core.trade.TradeResult;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 风控阈值、规则快照替换与成交时间窗口的单元测试。
 *
 * <p>核心能力：验证 10 秒窗口过期、copy-on-write 规则更新及短路行为。</p>
 * <p>线程安全：使用闭锁协调异步审批。</p>
 * <p>使用限制：仅验证内存实现的确定性行为。</p>
 */
class RiskEngineTest {
    /** 风控上下文测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 风控上下文测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 风控成交使用的交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 风控测试买方用户标识。 */
    private static final long BUYER_ID = 1L;
    /** 风控测试卖方用户标识。 */
    private static final long SELLER_ID = 2L;

    /**
     * 场景：已结算报价金额超过阈值触发审批，并可通过推进时钟跨越 10 秒窗口而过期。
     *
     * @throws Exception 审批线程同步、结果等待或引擎关闭失败时抛出
     */
    @Test
    void thresholdUsesSettledQuoteAndExpiresWithoutSleeping() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createBalance(BUYER_ID, BTC, 0L);
        ledger.createBalance(BUYER_ID, USDT, 1_000L);
        ledger.createBalance(SELLER_ID, BTC, 10L);
        ledger.createBalance(SELLER_ID, USDT, 0L);
        ManualClock clock = new ManualClock(100L);
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(new SlidingWindowAmountRule(120L)),
                clock,
                approvals,
                submission -> {
                    approvalEntered.countDown();
                    try {
                        releaseApproval.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return ApprovalDecision.REJECT;
                    }
                    return ApprovalDecision.PASS;
                });
        try {
            engine.submit(buySubmission(1L, 120L));
            engine.submit(sellSubmission(11L));
            assertEquals(TradeResult.SETTLED, engine.onTrade(new TradeExecution(
                    1L, 1L, 11L, BTC_USDT,
                    1L, 120L, 2L, 2L, 100L)));

            engine.submit(buySubmission(2L, 50L));
            assertEquals(OrderStatus.RISK_HOLD, engine.order(2L).status());
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
            releaseApproval.countDown();
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(OrderStatus.NEW, engine.order(2L).status());

            clock.advanceMillis(10_001L);
            engine.submit(buySubmission(3L, 50L));
            assertEquals(OrderStatus.NEW, engine.order(3L).status());
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    /** 场景：规则管道在 HOLD 时短路，且替换与移除 copy-on-write 快照后立即生效。 */
    @Test
    void pipelineShortCircuitsAndSupportsCopyOnWriteReplacement() {
        AtomicInteger second = new AtomicInteger();
        RiskRule hold = context -> RiskDecision.HOLD;
        RiskRule countingPass = context -> {
            second.incrementAndGet();
            return RiskDecision.PASS;
        };
        RiskPipeline pipeline = new RiskPipeline(hold, countingPass);
        RiskContext context = new RiskContext(1L, 1L, USDT, 1L, 1L, 1L);
        assertEquals(RiskDecision.HOLD, pipeline.evaluate(context));
        assertEquals(0, second.get());
        pipeline.replaceRules(countingPass);
        assertEquals(RiskDecision.PASS, pipeline.evaluate(context));
        assertEquals(1, second.get());
        assertTrue(pipeline.removeRule(countingPass));
        assertEquals(RiskDecision.PASS, pipeline.evaluate(context));
    }

    /** 场景：成交窗口仅保留 10 秒范围内的记录并准确淘汰过期金额。 */
    @Test
    void tradeWindowEvictsExpiredEntries() {
        TradeWindow window = new TradeWindow(10_000L);
        window.record(100L, 40L);
        window.record(1_000L, 50L);
        assertEquals(90L, window.currentSum(10_000L));
        assertEquals(50L, window.currentSum(10_101L));
    }

    /** 场景：窗口准备阶段无副作用，提交后才发布新金额。 */
    @Test
    void tradeWindowPreparationDoesNotMutateUntilCommit() {
        TradeWindow window = new TradeWindow(10_000L);
        TradeWindowMutation mutation = window.prepareRecord(100L, 40L);

        assertEquals(0L, window.currentSum(100L));
        window.commitRecord(mutation);
        assertEquals(40L, window.currentSum(100L));
    }

    /**
     * 创建测试买单提交。
     *
     * @param orderId 买单标识
     * @param reserve 报价资产冻结量与风险名义金额
     * @return 强类型买单提交
     */
    private static OrderSubmission buySubmission(long orderId, long reserve) {
        return new OrderSubmission(
                orderId, BUYER_ID, OrderSide.BUY, BTC_USDT,
                1L, reserve, reserve, 1L, 100L);
    }

    /**
     * 创建测试卖单提交。
     *
     * @param orderId 卖单标识
     * @return 强类型卖单提交
     */
    private static OrderSubmission sellSubmission(long orderId) {
        return new OrderSubmission(
                orderId, SELLER_ID, OrderSide.SELL, BTC_USDT,
                1L, 1L, 120L, 1L, 100L);
    }
}
