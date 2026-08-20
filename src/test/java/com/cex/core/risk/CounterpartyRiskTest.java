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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 验证双边成交窗口、强类型审批和权威撤单确认的风险集成。
 *
 * <p>核心能力：覆盖报价资产风险名义金额、暂挂审批和拒绝后的冻结资金顺序。</p>
 * <p>线程安全：异步审批场景使用闭锁提供确定的可见性与执行顺序。</p>
 * <p>使用限制：测试只覆盖任务 7 风控边界，不替代后续混沌和性能验收。</p>
 */
class CounterpartyRiskTest {
    /** 创建双边风控测试实例。 */
    CounterpartyRiskTest() {
    }


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
     * 场景：并发 PASS/REJECT 只能一个决策取得用户锁内线性化点。
     *
     * @throws Exception 当审批闭锁、反射测试锁替换或线程等待失败时抛出
     */
    @Test
    void concurrentPassCannotInterleaveInsideRejectedApprovalTransition() throws Exception {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = ledger(locks);
        ApprovalService approvals = new ApprovalService(1, 8);
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseAutomaticApproval = new CountDownLatch(1);
        List<CancelRequest> requests = new ArrayList<>();
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> RiskDecision.HOLD),
                new ManualClock(100L),
                approvals,
                submission -> {
                    approvalEntered.countDown();
                    try {
                        releaseAutomaticApproval.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return ApprovalDecision.PASS;
                },
                requests::add);
        try {
            OrderContext order = engine.submit(
                    buySubmission(11L, 101L, 10L, 1_000L, 1_000L));
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
            ApprovalHandoffLock handoffLock = new ApprovalHandoffLock("approval-reject");
            replaceUserLock(locks, 101L, handoffLock);
            AtomicReference<Throwable> rejectFailure = new AtomicReference<>();
            Thread reject = new Thread(() -> {
                try {
                    engine.onApproval(new ApprovalResult(
                            order.orderId(), ApprovalDecision.REJECT, 200L));
                } catch (Throwable failure) {
                    rejectFailure.set(failure);
                }
            }, "approval-reject");

            reject.start();
            assertTrue(handoffLock.awaitFirstRelease());
            engine.onApproval(new ApprovalResult(
                    order.orderId(), ApprovalDecision.PASS, 201L));
            handoffLock.continueReject();
            reject.join(2_000L);

            assertTrue(!reject.isAlive());
            assertEquals(null, rejectFailure.get());
            assertEquals(OrderStatus.PENDING_CANCEL, order.status());
            assertEquals(1, requests.size());

            releaseAutomaticApproval.countDown();
            engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(OrderStatus.PENDING_CANCEL, order.status());
            assertEquals(1, requests.size());
        } finally {
            releaseAutomaticApproval.countDown();
            engine.close();
        }
    }

    /**
     * 创建包含买卖双方多资产余额的测试账本。
     *
     * @return 初始化完成的独立多资产账本
     */
    private static AccountLedger ledger() {
        return ledger(new StripedLockManager(16));
    }

    /**
     * 使用指定条带锁创建多资产测试账本。
     *
     * @param locks 账本与引擎共享的条带锁管理器
     * @return 初始化完成的独立多资产账本
     */
    private static AccountLedger ledger(StripedLockManager locks) {
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(101L, BTC, 0L);
        ledger.createBalance(101L, USDT, 10_000L);
        ledger.createBalance(202L, BTC, 20L);
        ledger.createBalance(202L, USDT, 0L);
        ledger.createBalance(202L, USDC, 0L);
        return ledger;
    }

    /**
     * 将指定用户条带替换为可控交接锁，仅用于确定性复现审批线性化竞态。
     *
     * @param locks 待修改的测试锁管理器
     * @param userId 需要拦截的用户标识
     * @param replacement 替换后的可控锁
     * @throws ReflectiveOperationException 当无法访问测试锁数组时抛出
     */
    private static void replaceUserLock(
            StripedLockManager locks,
            long userId,
            ReentrantLock replacement) throws ReflectiveOperationException {
        Field stripesField = StripedLockManager.class.getDeclaredField("stripes");
        stripesField.setAccessible(true);
        ReentrantLock[] stripes = (ReentrantLock[]) stripesField.get(locks);
        stripes[locks.stripeIndexForUser(userId)] = replacement;
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

    /**
     * 在指定线程首次释放审批锁后暂停该线程的测试锁。
     *
     * <p>核心能力：把 REJECT 的第一次 unlock 与后续逻辑分开，以确定性验证中间是否允许 PASS 插入。</p>
     * <p>线程安全：原子标识保证只拦截一次，闭锁协调测试线程与审批线程。</p>
     * <p>使用限制：只用于本测试类，不能进入生产代码。</p>
     */
    private static final class ApprovalHandoffLock extends ReentrantLock {
        /** 需要在首次 unlock 后暂停的线程名称。 */
        private final String interceptedThreadName;
        /** 已完成目标 unlock 的通知闭锁。 */
        private final CountDownLatch firstRelease = new CountDownLatch(1);
        /** 允许目标线程继续执行的闭锁。 */
        private final CountDownLatch continueReject = new CountDownLatch(1);
        /** 是否已经执行过一次确定性交接。 */
        private final AtomicBoolean intercepted = new AtomicBoolean();

        /**
         * 创建审批交接锁。
         *
         * @param interceptedThreadName 要拦截的审批线程名称
         */
        private ApprovalHandoffLock(String interceptedThreadName) {
            this.interceptedThreadName = interceptedThreadName;
        }

        /**
         * 释放锁，并在目标线程首次释放后等待测试线程继续信号。
         *
         * @note 先实际释放底层锁再等待，使竞争审批能够取得同一用户锁。
         */
        @Override
        public void unlock() {
            super.unlock();
            if (Thread.currentThread().getName().equals(interceptedThreadName)
                    && intercepted.compareAndSet(false, true)) {
                firstRelease.countDown();
                try {
                    if (!continueReject.await(2L, TimeUnit.SECONDS)) {
                        throw new AssertionError("approval handoff timed out");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("approval handoff interrupted", interrupted);
                }
            }
        }

        /**
         * 等待被测审批线程第一次释放用户锁。
         *
         * @return 两秒内观察到第一次释放时为 {@code true}
         * @throws InterruptedException 当测试线程等待期间被中断时抛出
         */
        private boolean awaitFirstRelease() throws InterruptedException {
            return firstRelease.await(2L, TimeUnit.SECONDS);
        }

        /** 允许被暂停的 REJECT 审批线程继续执行。 */
        private void continueReject() {
            continueReject.countDown();
        }
    }
}
