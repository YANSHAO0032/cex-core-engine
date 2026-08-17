package com.cex.core.risk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.CancelConfirmation;
import com.cex.core.order.CancelRequest;
import com.cex.core.order.OrderContext;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import com.cex.core.trade.TradeResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * 验证双边成交窗口、强类型审批和权威撤单确认的风险集成。
 *
 * <p>核心能力：覆盖报价资产风险名义金额、暂挂审批和拒绝后的冻结资金顺序。</p>
 * <p>线程安全：异步审批场景使用闭锁提供确定的可见性与执行顺序。</p>
 * <p>使用限制：测试只覆盖任务 7 风控边界，不替代后续混沌和性能验收。</p>
 */
class CounterpartyRiskTest {

    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 用于验证报价资产窗口隔离的第二报价资产。 */
    private static final AssetId USDC = new AssetId("USDC");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);
    /** 第二报价资产测试交易对。 */
    private static final TradingPair BTC_USDC = new TradingPair(BTC, USDC);

    /**
     * 场景：SELL 创建风控必须使用上游报价资产名义金额，而不是基础资产冻结量。
     *
     * @throws Exception 当审批闭锁等待或服务关闭失败时抛出
     */
    @Test
    void sellSubmissionUsesUpstreamQuoteRiskAmount() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(new SlidingWindowAmountRule(500L)),
                new ManualClock(100L),
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
            engine.submit(buySubmission(11L, 101L, 10L, 1_000L, 10L));
            engine.submit(sellSubmission(22L, 202L, 10L, 10L, 10L));
            assertEquals(TradeResult.SETTLED, engine.onTrade(new TradeExecution(
                    1L, 11L, 22L, BTC_USDT, 1L, 450L, 2L, 2L, 100L)));

            OrderContext held = engine.submit(
                    sellSubmission(23L, 202L, 1L, 1L, 100L));

            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
            assertEquals(OrderStatus.RISK_HOLD, held.status());
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    /**
     * 场景：审批拒绝只发送稳定撤单请求，确认前不得解冻剩余资金。
     *
     * @throws Exception 当审批结果等待或服务关闭失败时抛出
     */
    @Test
    void approvalRejectionRequestsCancelButWaitsForConfirmation() throws Exception {
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        List<CancelRequest> requests = new ArrayList<>();
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> RiskDecision.HOLD),
                new ManualClock(100L),
                approvals,
                submission -> ApprovalDecision.REJECT,
                requests::add);
        try {
            OrderContext order = engine.submit(
                    buySubmission(11L, 101L, 10L, 1_000L, 1_000L));
            engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.PENDING_CANCEL, order.status());
            assertEquals(1, requests.size());
            assertTrue(ledger.balance(101L, USDT).frozen() > 0L);

            CancelRequest request = requests.getFirst();
            engine.onCancelConfirmed(new CancelConfirmation(
                    request.cancelRequestId(), order.orderId(), 2L, 200L));

            assertEquals(OrderStatus.CANCELED, order.status());
            assertEquals(0L, ledger.balance(101L, USDT).frozen());
        } finally {
            engine.close();
        }
    }

    /** 场景：同一用户在 USDT 的历史成交不得导致 USDC 新单被错误暂挂。 */
    @Test
    void settledAmountsFromDifferentQuoteAssetsRemainIsolated() {
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(new SlidingWindowAmountRule(500L)),
                new ManualClock(100L),
                approvals,
                submission -> ApprovalDecision.PASS);
        try {
            engine.submit(buySubmission(11L, 101L, 10L, 1_000L, 10L));
            engine.submit(sellSubmission(22L, 202L, 10L, 10L, 10L));
            assertEquals(TradeResult.SETTLED, engine.onTrade(new TradeExecution(
                    1L, 11L, 22L, BTC_USDT, 1L, 450L, 2L, 2L, 100L)));

            OrderContext usdcOrder = engine.submit(new OrderSubmission(
                    23L, 202L, OrderSide.SELL, BTC_USDC,
                    1L, 1L, 100L, 1L, 100L));

            assertEquals(OrderStatus.NEW, usdcOrder.status());
            assertEquals(450L, engine.tradeWindow(202L, USDT).currentSum(100L));
            assertEquals(0L, engine.tradeWindow(202L, USDC).currentSum(100L));
        } finally {
            engine.close();
        }
    }

    /**
     * 场景：暂挂买单审批通过后必须排空已缓存的双边成交。
     *
     * @throws Exception 当审批闭锁等待或服务关闭失败时抛出
     */
    @Test
    void approvalPassDrainsCachedCounterpartyTrade() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> context.userId() == 101L
                        ? RiskDecision.HOLD : RiskDecision.PASS),
                new ManualClock(100L),
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
            OrderContext buyer = engine.submit(
                    buySubmission(11L, 101L, 10L, 1_000L, 1_000L));
            OrderContext seller = engine.submit(
                    sellSubmission(22L, 202L, 10L, 10L, 1_000L));
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));

            assertEquals(TradeResult.PENDING, engine.onTrade(new TradeExecution(
                    1L, 11L, 22L, BTC_USDT, 2L, 200L, 2L, 2L, 100L)));
            assertEquals(0L, buyer.cumulativeBaseFilled());

            releaseApproval.countDown();
            engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.PARTIALLY_FILLED, buyer.status());
            assertEquals(OrderStatus.PARTIALLY_FILLED, seller.status());
            assertEquals(2L, buyer.cumulativeBaseFilled());
            assertEquals(200L, engine.tradeWindow(101L, USDT).currentSum(100L));
            assertEquals(200L, engine.tradeWindow(202L, USDT).currentSum(100L));
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    /**
     * 场景：审批拒绝后的确认 N 必须先结算序号小于 N 的双边成交，再取消并释放余款。
     *
     * @throws Exception 当审批结果等待或服务关闭失败时抛出
     */
    @Test
    void rejectionConfirmationSettlesEarlierTradeBeforeCancelingRemainder() throws Exception {
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        List<CancelRequest> requests = new ArrayList<>();
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> context.userId() == 101L
                        ? RiskDecision.HOLD : RiskDecision.PASS),
                new ManualClock(100L),
                approvals,
                submission -> ApprovalDecision.REJECT,
                requests::add);
        try {
            OrderContext buyer = engine.submit(
                    buySubmission(11L, 101L, 10L, 1_000L, 1_000L));
            engine.submit(sellSubmission(22L, 202L, 10L, 10L, 1_000L));
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            CancelRequest request = requests.getFirst();

            engine.onCancelConfirmed(new CancelConfirmation(
                    request.cancelRequestId(), buyer.orderId(), 3L, 200L));
            assertEquals(TradeResult.SETTLED, engine.onTrade(new TradeExecution(
                    1L, 11L, 22L, BTC_USDT, 2L, 200L, 2L, 2L, 100L)));

            assertEquals(OrderStatus.CANCELED, buyer.status());
            assertEquals(2L, buyer.cumulativeBaseFilled());
            assertEquals(3L, buyer.lastAppliedSequence());
            assertEquals(0L, ledger.balance(101L, USDT).frozen());
            assertEquals(9_800L, ledger.balance(101L, USDT).available());
        } finally {
            engine.close();
        }
    }

    /**
     * 场景：风险撤单首次发送失败后，重复拒绝回调必须复用同一请求载荷完成单次重试。
     *
     * @throws Exception 当审批闭锁等待或服务关闭失败时抛出
     */
    @Test
    void rejectedApprovalRetriesTheSameStableCancelRequest() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = ledger();
        ApprovalService approvals = new ApprovalService(1, 8);
        List<CancelRequest> attempts = new ArrayList<>();
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> RiskDecision.HOLD),
                new ManualClock(100L),
                approvals,
                submission -> {
                    approvalEntered.countDown();
                    try {
                        releaseApproval.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return ApprovalDecision.REJECT;
                },
                request -> {
                    attempts.add(request);
                    if (attempts.size() == 1) {
                        throw new IllegalStateException("injected risk cancel delivery failure");
                    }
                });
        try {
            OrderContext order = engine.submit(
                    buySubmission(11L, 101L, 10L, 1_000L, 1_000L));
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));

            assertThrows(IllegalStateException.class,
                    () -> engine.onApproval(new ApprovalResult(
                            order.orderId(), ApprovalDecision.REJECT, 200L)));
            assertEquals(OrderStatus.PENDING_CANCEL, order.status());

            engine.onApproval(new ApprovalResult(
                    order.orderId(), ApprovalDecision.REJECT, 300L));
            engine.onApproval(new ApprovalResult(
                    order.orderId(), ApprovalDecision.REJECT, 400L));

            assertEquals(2, attempts.size());
            assertEquals(attempts.get(0), attempts.get(1));
            assertEquals(attempts.get(0).cancelRequestId(), order.cancelRequestId());
            assertTrue(ledger.balance(101L, USDT).frozen() > 0L);

            releaseApproval.countDown();
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(2, attempts.size());
        } finally {
            releaseApproval.countDown();
            engine.close();
        }
    }

    /**
     * 创建包含买卖双方多资产余额的测试账本。
     *
     * @return 初始化完成的独立多资产账本
     */
    private static AccountLedger ledger() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager(16));
        ledger.createBalance(101L, BTC, 0L);
        ledger.createBalance(101L, USDT, 10_000L);
        ledger.createBalance(202L, BTC, 20L);
        ledger.createBalance(202L, USDT, 0L);
        ledger.createBalance(202L, USDC, 0L);
        return ledger;
    }

    /**
     * 创建测试买单提交。
     *
     * @param orderId 买单标识
     * @param userId 买方用户标识
     * @param base 基础资产数量
     * @param reserve 报价资产冻结数量
     * @param riskQuote 上游报价资产风险名义金额
     * @return 不可变买单提交
     */
    private static OrderSubmission buySubmission(
            long orderId, long userId, long base, long reserve, long riskQuote) {
        return new OrderSubmission(
                orderId, userId, OrderSide.BUY, BTC_USDT,
                base, reserve, riskQuote, 1L, 100L);
    }

    /**
     * 创建测试卖单提交。
     *
     * @param orderId 卖单标识
     * @param userId 卖方用户标识
     * @param base 基础资产数量
     * @param reserve 基础资产冻结数量
     * @param riskQuote 上游报价资产风险名义金额
     * @return 不可变卖单提交
     */
    private static OrderSubmission sellSubmission(
            long orderId, long userId, long base, long reserve, long riskQuote) {
        return new OrderSubmission(
                orderId, userId, OrderSide.SELL, BTC_USDT,
                base, reserve, riskQuote, 1L, 100L);
    }
}
