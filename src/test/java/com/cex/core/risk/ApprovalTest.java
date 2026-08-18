package com.cex.core.risk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
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
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeResult;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * 审批结果回流、风险暂挂成交门禁与权威撤单确认的集成测试。
 *
 * <p>核心能力：验证审批服务只发布强类型结果，PASS 排空成交，REJECT 等待撤单确认。</p>
 * <p>线程安全：通过闭锁协调异步审批，撤单收集使用并发列表。</p>
 * <p>使用限制：依赖测试专用内存账本，不覆盖外部撮合实现。</p>
 */
class ApprovalTest {
    /** 创建审批集成测试实例。 */
    ApprovalTest() {
    }

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

    /**
     * 场景：审批服务只发布强类型拒绝结果，不直接修改订单或账本。
     *
     * @throws Exception 审批任务等待或服务关闭失败时抛出
     */
    @Test
    void approvalServiceEmitsStrongResultWithoutChangingOrderState() throws Exception {
        ApprovalService service = new ApprovalService(1, 1);
        try {
            OrderSubmission submission = buySubmission();
            AtomicReference<ApprovalResult> received = new AtomicReference<>();

            service.submit(submission,
                    source -> ApprovalDecision.REJECT, received::set);
            service.awaitQuiescence(2L, TimeUnit.SECONDS);

            assertNotNull(received.get());
            assertEquals(BUY_ORDER_ID, received.get().orderId());
            assertEquals(ApprovalDecision.REJECT, received.get().decision());
            assertTrue(received.get().decidedAtMillis() >= 0L);
            assertEquals(1L, service.submittedCount());
        } finally {
            service.close();
        }
    }

    /**
     * 场景：提交已越过接收边界后并发关闭，已接收任务仍须完成，关闭后的新提交必须拒绝。
     *
     * @throws Exception 并发提交、关闭或审批等待失败时抛出
     */
    @Test
    void acceptedSubmissionCompletesWhenCloseRacesAfterAcceptance() throws Exception {
        CountDownLatch accepted = new CountDownLatch(1);
        CountDownLatch releaseSubmission = new CountDownLatch(1);
        ApprovalService service = new ApprovalService(1, 1, () -> {
            accepted.countDown();
            awaitLatch(releaseSubmission);
        });
        ExecutorService callers = Executors.newFixedThreadPool(2);
        AtomicReference<ApprovalResult> received = new AtomicReference<>();
        try {
            Future<?> submission = callers.submit(() -> service.submit(
                    buySubmission(), source -> ApprovalDecision.PASS, received::set));
            assertTrue(accepted.await(2L, TimeUnit.SECONDS));

            Future<?> closing = callers.submit(service::close);
            closing.get(2L, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, () -> service.submit(
                    buySubmission(), source -> ApprovalDecision.PASS, result -> { }));

            releaseSubmission.countDown();
            submission.get(2L, TimeUnit.SECONDS);
            service.awaitQuiescence(2L, TimeUnit.SECONDS);

            assertNotNull(received.get());
            assertEquals(1L, service.submittedCount());
        } finally {
            releaseSubmission.countDown();
            service.close();
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(2L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：已越过接收边界但尚未进入执行器的任务必须参与静止等待，关闭不得令等待提前成功。
     *
     * @throws Exception 并发提交、关闭或审批等待失败时抛出
     */
    @Test
    void quiescenceIncludesAcceptedSubmissionBeforeExecutorEntry() throws Exception {
        CountDownLatch accepted = new CountDownLatch(1);
        CountDownLatch releaseSubmission = new CountDownLatch(1);
        ApprovalService service = new ApprovalService(1, 1, () -> {
            accepted.countDown();
            awaitLatch(releaseSubmission);
        });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AtomicReference<ApprovalResult> received = new AtomicReference<>();
        try {
            Future<?> submission = caller.submit(() -> service.submit(
                    buySubmission(), source -> ApprovalDecision.PASS, received::set));
            assertTrue(accepted.await(2L, TimeUnit.SECONDS));
            service.close();

            assertThrows(IllegalStateException.class,
                    () -> service.awaitQuiescence(20L, TimeUnit.MILLISECONDS));

            releaseSubmission.countDown();
            submission.get(2L, TimeUnit.SECONDS);
            service.awaitQuiescence(2L, TimeUnit.SECONDS);
            assertNotNull(received.get());
        } finally {
            releaseSubmission.countDown();
            service.close();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(2L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：队列满触发提交线程执行时，结果回流内关闭服务不得形成生命周期锁升级死锁。
     *
     * @throws Exception 并发任务或审批等待失败时抛出
     */
    @Test
    void callerRunsSinkMayCloseServiceWithoutDeadlock() throws Exception {
        ApprovalService service = new ApprovalService(1, 1);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            service.submit(buySubmission(), submission -> {
                workerEntered.countDown();
                awaitLatch(releaseWorker);
                return ApprovalDecision.PASS;
            }, result -> { });
            assertTrue(workerEntered.await(2L, TimeUnit.SECONDS));
            service.submit(buySubmission(), submission -> ApprovalDecision.PASS, result -> { });

            Future<?> callerRuns = caller.submit(() -> service.submit(
                    buySubmission(), submission -> ApprovalDecision.PASS,
                    result -> service.close()));
            callerRuns.get(2L, TimeUnit.SECONDS);
            releaseWorker.countDown();
            service.awaitQuiescence(2L, TimeUnit.SECONDS);

            assertEquals(3L, service.submittedCount());
        } finally {
            releaseWorker.countDown();
            service.close();
            caller.shutdownNow();
            assertTrue(caller.awaitTermination(2L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：提交线程执行的审批策略失败仍属于已接收且已完成任务，不得回滚接收计数。
     *
     * @throws Exception 并发任务或审批等待失败时抛出
     */
    @Test
    void callerRunsTaskFailureStillBalancesAcceptedCount() throws Exception {
        ApprovalService service = new ApprovalService(1, 1);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try {
            service.submit(buySubmission(), submission -> {
                workerEntered.countDown();
                awaitLatch(releaseWorker);
                return ApprovalDecision.PASS;
            }, result -> { });
            assertTrue(workerEntered.await(2L, TimeUnit.SECONDS));
            service.submit(buySubmission(), submission -> ApprovalDecision.PASS, result -> { });

            assertThrows(IllegalStateException.class, () -> service.submit(
                    buySubmission(), submission -> {
                        throw new IllegalStateException("injected approval failure");
                    }, result -> { }));
            releaseWorker.countDown();
            service.awaitQuiescence(2L, TimeUnit.SECONDS);

            assertEquals(3L, service.submittedCount());
        } finally {
            releaseWorker.countDown();
            service.close();
        }
    }

    /**
     * 场景：审批拒绝只能进入等待撤单，确认前保持冻结，确认后仅解冻一次。
     *
     * @throws Exception 审批结果等待失败时抛出
     */
    @Test
    void rejectWaitsForConfirmationAndUnfreezesExactlyOnce() throws Exception {
        List<CancelRequest> requests = new CopyOnWriteArrayList<>();
        try (Fixture fixture = new Fixture(
                ApprovalDecision.REJECT, requests, false)) {
            OrderContext buyer = fixture.engine.submit(buySubmission());
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.PENDING_CANCEL, buyer.status());
            assertEquals(new BalanceSnapshot(0L, 1_000L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertEquals(1, requests.size());

            CancelRequest request = requests.getFirst();
            CancelConfirmation confirmation = new CancelConfirmation(
                    request.cancelRequestId(), BUY_ORDER_ID, 2L, 3L);
            fixture.engine.onCancelConfirmed(confirmation);
            fixture.engine.onCancelConfirmed(confirmation);

            assertEquals(OrderStatus.CANCELED, buyer.status());
            assertEquals(new BalanceSnapshot(1_000L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /**
     * 场景：审批通过后只结算一次暂挂期间重复到达的双边成交。
     *
     * @throws Exception 审批线程同步或结果等待失败时抛出
     */
    @Test
    void approvedRiskHoldAppliesCachedTradeExactlyOnce() throws Exception {
        try (Fixture fixture = new Fixture(
                ApprovalDecision.PASS, new CopyOnWriteArrayList<>(), true)) {
            OrderContext buyer = fixture.engine.submit(buySubmission());
            fixture.engine.submit(sellSubmission());
            fixture.awaitApprovalEntry();
            TradeExecution execution = execution(1L, 10L, 1_000L, 2L, 2L);

            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(execution));
            assertEquals(TradeResult.PENDING, fixture.engine.onTrade(execution));
            assertEquals(OrderStatus.RISK_HOLD, buyer.status());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.FILLED, buyer.status());
            assertEquals(TradeExecutionState.SETTLED,
                    fixture.engine.trade(1L).state());
            assertEquals(1L, fixture.engine.metrics().settledTradeCount());
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /**
     * 场景：拒绝后的确认 N 先结算序号小于 N 的成交，再取消并释放剩余冻结额。
     *
     * @throws Exception 审批结果等待失败时抛出
     */
    @Test
    void rejectedApprovalSettlesEarlierTradeBeforeCancelingRemainder()
            throws Exception {
        List<CancelRequest> requests = new CopyOnWriteArrayList<>();
        try (Fixture fixture = new Fixture(
                ApprovalDecision.REJECT, requests, false)) {
            OrderContext buyer = fixture.engine.submit(buySubmission());
            fixture.engine.submit(sellSubmission());
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);
            CancelRequest request = requests.getFirst();

            fixture.engine.onCancelConfirmed(new CancelConfirmation(
                    request.cancelRequestId(), BUY_ORDER_ID, 3L, 4L));
            assertEquals(TradeResult.SETTLED,
                    fixture.engine.onTrade(execution(
                            1L, 2L, 200L, 2L, 2L)));

            assertEquals(OrderStatus.CANCELED, buyer.status());
            assertEquals(2L, buyer.cumulativeBaseFilled());
            assertEquals(3L, buyer.lastAppliedSequence());
            assertEquals(new BalanceSnapshot(800L, 0L),
                    fixture.ledger.balance(BUYER_ID, USDT));
            assertTrue(fixture.ledger.allAssetInvariantsHold());
        }
    }

    /**
     * 创建固定买单提交。
     *
     * @return 固定买单提交
     */
    private static OrderSubmission buySubmission() {
        return new OrderSubmission(
                BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                10L, 1_000L, 1_000L, 1L, 1L);
    }

    /**
     * 创建固定卖单提交。
     *
     * @return 固定卖单提交
     */
    private static OrderSubmission sellSubmission() {
        return new OrderSubmission(
                SELL_ORDER_ID, SELLER_ID, OrderSide.SELL, BTC_USDT,
                10L, 10L, 1_000L, 1L, 1L);
    }

    /**
     * 构造固定双边成交。
     *
     * @param tradeId 成交标识
     * @param baseQuantity 基础资产成交量
     * @param quoteQuantity 报价资产成交量
     * @param buySequence 买单权威序号
     * @param sellSequence 卖单权威序号
     * @return 强类型双边成交
     */
    private static TradeExecution execution(
            long tradeId, long baseQuantity, long quoteQuantity,
            long buySequence, long sellSequence) {
        return new TradeExecution(
                tradeId, BUY_ORDER_ID, SELL_ORDER_ID, BTC_USDT,
                baseQuantity, quoteQuantity,
                buySequence, sellSequence, 2L);
    }

    /**
     * 在限定时间内等待测试同步信号。
     *
     * @param latch 待等待的同步信号
     */
    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(2L, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test synchronization");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for test synchronization", interrupted);
        }
    }

    /** 通过闭锁控制审批返回的强类型风险夹具。 */
    private static final class Fixture implements AutoCloseable {
        /** 多资产账户账本。 */
        private final AccountLedger ledger =
                new AccountLedger(new StripedLockManager(16));
        /** 审批策略开始执行的通知闭锁。 */
        private final CountDownLatch approvalEntered = new CountDownLatch(1);
        /** 允许审批策略返回的控制闭锁。 */
        private final CountDownLatch releaseApproval = new CountDownLatch(1);
        /** 被测强类型订单引擎。 */
        private final OrderEngine engine;

        /**
         * 创建固定审批结论的测试夹具。
         *
         * @param decision 审批策略最终返回值
         * @param requests 外部撤单请求收集器
         * @param blockApproval 是否在返回审批结果前等待测试释放
         */
        private Fixture(
                ApprovalDecision decision,
                List<CancelRequest> requests,
                boolean blockApproval) {
            ledger.createBalance(BUYER_ID, BTC, 0L);
            ledger.createBalance(BUYER_ID, USDT, 1_000L);
            ledger.createBalance(SELLER_ID, BTC, 10L);
            ledger.createBalance(SELLER_ID, USDT, 0L);
            ApprovalService approvals = new ApprovalService(1, 8);
            engine = new OrderEngine(
                    ledger,
                    new RiskPipeline(context -> context.userId() == BUYER_ID
                            ? RiskDecision.HOLD : RiskDecision.PASS),
                    new ManualClock(1L),
                    approvals,
                    submission -> {
                        approvalEntered.countDown();
                        if (blockApproval) {
                            try {
                                releaseApproval.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return ApprovalDecision.REJECT;
                            }
                        }
                        return decision;
                    },
                    requests::add);
        }

        /**
         * 等待审批策略进入。
         *
         * @throws InterruptedException 当等待线程被中断时抛出
         */
        private void awaitApprovalEntry() throws InterruptedException {
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
        }

        /** 释放阻塞审批并关闭引擎。 */
        @Override
        public void close() {
            releaseApproval.countDown();
            engine.close();
        }
    }
}
