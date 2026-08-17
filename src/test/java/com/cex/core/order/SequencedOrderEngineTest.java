package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceSnapshot;
import com.cex.core.account.InsufficientBalanceException;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalResult;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.ManualClock;
import com.cex.core.risk.RiskDecision;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.trade.TradeExecutionState;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 验证强类型订单入口的发布、冻结、审批和撤单确认顺序。
 *
 * <p>每个场景使用真实多资产账本与订单引擎，防止测试替身掩盖资金或锁边界副作用。</p>
 */
class SequencedOrderEngineTest {
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

    /** 关闭每个场景创建的审批执行器。 */
    @AfterEach
    void closeEngines() {
        engines.forEach(OrderEngine::close);
    }

    /** 场景：冻结失败不得发布一个可被成交协调器读取的半创建订单。 */
    @Test
    void freezeFailureDoesNotPublishOrder() {
        Fixture fixture = fixture(999L);

        assertThrows(InsufficientBalanceException.class,
                () -> fixture.engine.submit(fixture.buySubmission()));

        assertNull(fixture.engine.order(BUY_ORDER_ID));
        assertEquals(new BalanceSnapshot(999L, 0L),
                fixture.ledger.balance(BUYER_ID, USDT));
    }

    /** 场景：相同创建载荷重复提交只返回原上下文，不得重复冻结。 */
    @Test
    void exactDuplicateSubmissionDoesNotFreezeTwice() {
        Fixture fixture = fixture(1_000L);
        OrderSubmission submission = fixture.buySubmission();

        OrderContext first = fixture.engine.submit(submission);
        OrderContext duplicate = fixture.engine.submit(submission);

        assertSame(first, duplicate);
        assertEquals(new BalanceSnapshot(0L, 1_000L),
                fixture.ledger.balance(BUYER_ID, USDT));
    }

    /** 场景：创建前撤单确认转移到订单序列，待相同请求登记后原子解冻。 */
    @Test
    void cancelConfirmationBeforeSubmissionTransfersWithoutAssetLoss() {
        Fixture fixture = fixture(1_000L);
        CancelConfirmation confirmation =
                new CancelConfirmation(90L, BUY_ORDER_ID, 2L, 20L);

        fixture.engine.onCancelConfirmed(confirmation);
        OrderContext order = fixture.engine.submit(fixture.buySubmission());
        assertEquals(OrderStatus.NEW, order.status());
        assertEquals(new BalanceSnapshot(0L, 1_000L),
                fixture.ledger.balance(BUYER_ID, USDT));

        assertEquals(CancelRequestResult.SUBMITTED,
                fixture.engine.requestCancel(new CancelRequest(90L, BUY_ORDER_ID, 10L)));
        assertEquals(OrderStatus.CANCELED, order.status());
        assertEquals(2L, order.lastAppliedSequence());
        assertEquals(new BalanceSnapshot(1_000L, 0L),
                fixture.ledger.balance(BUYER_ID, USDT));
    }

    /** 场景：序号 N 的撤单确认必须等待 N 之前的成交，再释放真实剩余冻结额。 */
    @Test
    void cancelConfirmationDrainsEarlierTradeThenReleasesRemainder() {
        Fixture fixture = readyFixture();

        assertEquals(CancelRequestResult.SUBMITTED,
                fixture.engine.requestCancel(new CancelRequest(90L, BUY_ORDER_ID, 10L)));
        fixture.engine.onCancelConfirmed(
                new CancelConfirmation(90L, BUY_ORDER_ID, 3L, 20L));
        fixture.engine.onTrade(fixture.execution(1L, 2L, 200L, 2L, 2L));

        assertEquals(OrderStatus.CANCELED, fixture.buyOrder().status());
        assertEquals(2L, fixture.buyOrder().cumulativeBaseFilled());
        assertEquals(3L, fixture.buyOrder().lastAppliedSequence());
        assertEquals(new BalanceSnapshot(800L, 0L),
                fixture.ledger.balance(BUYER_ID, USDT));
        assertEquals(TradeExecutionState.SETTLED, fixture.engine.trade(1L).state());
    }

    /** 场景：撤单发送失败保留等待态，同 ID 可重发一次，成功后精确重复不再调用外部边界。 */
    @Test
    void failedCancelDeliveryRetriesSameIdAndSuccessfulDeliveryIsIdempotent() {
        AtomicInteger attempts = new AtomicInteger();
        CancelRequestSink sink = request -> {
            assertEquals(90L, request.cancelRequestId());
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("injected first delivery failure");
            }
        };
        Fixture fixture = fixture(1_000L, sink);
        OrderContext order = fixture.engine.submit(fixture.buySubmission());
        CancelRequest request = new CancelRequest(90L, BUY_ORDER_ID, 10L);

        assertThrows(IllegalStateException.class,
                () -> fixture.engine.requestCancel(request));
        assertEquals(1, attempts.get());
        assertEquals(OrderStatus.PENDING_CANCEL, order.status());

        assertEquals(CancelRequestResult.DUPLICATE,
                fixture.engine.requestCancel(request));
        assertEquals(2, attempts.get());
        assertEquals(OrderStatus.PENDING_CANCEL, order.status());

        assertEquals(CancelRequestResult.DUPLICATE,
                fixture.engine.requestCancel(request));
        assertEquals(2, attempts.get());
    }

    /** 场景：同 ID 并发重试只能有一个线程占用发送权，其他调用不得并行触发 sink。 */
    @Test
    void concurrentSameIdRetryDoesNotStartParallelDelivery() throws Exception {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = ledger(locks, 1_000L);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch retryEnteredSink = new CountDownLatch(1);
        CountDownLatch releaseRetry = new CountDownLatch(1);
        CancelRequestSink sink = request -> {
            assertFalse(locks.lockForUser(BUYER_ID).isHeldByCurrentThread());
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                throw new IllegalStateException("injected first delivery failure");
            }
            if (attempt > 2) {
                throw new AssertionError("parallel duplicate delivery");
            }
            retryEnteredSink.countDown();
            try {
                if (!releaseRetry.await(2L, TimeUnit.SECONDS)) {
                    throw new AssertionError("retry release timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("retry delivery interrupted", interrupted);
            }
        };
        ApprovalService approvals = new ApprovalService(1, 16);
        OrderEngine engine = new OrderEngine(
                ledger, new RiskPipeline(), new ManualClock(1L), approvals,
                event -> ApprovalDecision.PASS, sink);
        engines.add(engine);
        engine.submit(new OrderSubmission(
                BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                10L, 1_000L, 1_000L, 1L, 10L));
        CancelRequest request = new CancelRequest(90L, BUY_ORDER_ID, 10L);
        assertThrows(IllegalStateException.class, () -> engine.requestCancel(request));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CancelRequestResult> retry =
                    executor.submit(() -> engine.requestCancel(request));
            assertTrue(retryEnteredSink.await(2L, TimeUnit.SECONDS));

            assertEquals(CancelRequestResult.DUPLICATE, engine.requestCancel(request));
            assertEquals(2, attempts.get());

            releaseRetry.countDown();
            assertEquals(CancelRequestResult.DUPLICATE,
                    retry.get(2L, TimeUnit.SECONDS));
            assertEquals(CancelRequestResult.DUPLICATE, engine.requestCancel(request));
            assertEquals(2, attempts.get());
        } finally {
            releaseRetry.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(2L, TimeUnit.SECONDS));
        }
    }

    /** 场景：最小风险分类可暂挂强类型订单，审批通过后恢复为可成交状态。 */
    @Test
    void approvalPassReleasesRiskHeldTypedOrder() {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = ledger(locks, 1_000L);
        ApprovalService approvals = new ApprovalService(1, 16);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(context -> RiskDecision.HOLD),
                new ManualClock(1L),
                approvals,
                event -> ApprovalDecision.PASS);
        engines.add(engine);

        OrderContext order = engine.submit(new OrderSubmission(
                BUY_ORDER_ID, BUYER_ID, OrderSide.BUY, BTC_USDT,
                10L, 1_000L, 1_000L, 1L, 10L));
        assertEquals(OrderStatus.RISK_HOLD, order.status());

        engine.onApproval(new ApprovalResult(BUY_ORDER_ID, ApprovalDecision.PASS, 20L));

        assertEquals(OrderStatus.NEW, order.status());
        assertEquals(new BalanceSnapshot(0L, 1_000L), ledger.balance(BUYER_ID, USDT));
    }

    /** 场景：创建前缓存精确重复幂等，相同序号冲突优先于容量错误。 */
    @Test
    void preCreationBufferIsBoundedIdempotentAndConflictDetecting() {
        PreCreationEventBuffer buffer = new PreCreationEventBuffer(1);
        CancelConfirmation accepted =
                new CancelConfirmation(90L, BUY_ORDER_ID, 3L, 20L);

        buffer.register(accepted);
        buffer.register(accepted);
        assertThrows(TradeSequenceConflictException.class,
                () -> buffer.register(
                        new CancelConfirmation(91L, BUY_ORDER_ID, 3L, 21L)));
        assertThrows(IllegalStateException.class,
                () -> buffer.register(
                        new CancelConfirmation(92L, BUY_ORDER_ID, 4L, 22L)));

        assertEquals(List.of(accepted), buffer.removeAll(BUY_ORDER_ID));
        assertEquals(List.of(), buffer.removeAll(BUY_ORDER_ID));
    }

    private Fixture readyFixture() {
        Fixture fixture = fixture(1_000L);
        fixture.engine.submit(fixture.buySubmission());
        fixture.engine.submit(fixture.sellSubmission());
        return fixture;
    }

    private Fixture fixture(long buyerQuoteAvailable) {
        return fixture(buyerQuoteAvailable, request -> { });
    }

    private Fixture fixture(
            long buyerQuoteAvailable, CancelRequestSink cancelRequestSink) {
        StripedLockManager locks = new StripedLockManager(16);
        AccountLedger ledger = ledger(locks, buyerQuoteAvailable);
        ApprovalService approvals = new ApprovalService(1, 16);
        OrderEngine engine = new OrderEngine(
                ledger, new RiskPipeline(), new ManualClock(1L), approvals,
                event -> ApprovalDecision.PASS, cancelRequestSink);
        engines.add(engine);
        return new Fixture(ledger, engine);
    }

    private static AccountLedger ledger(
            StripedLockManager locks, long buyerQuoteAvailable) {
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(BUYER_ID, BTC, 0L);
        ledger.createBalance(BUYER_ID, USDT, buyerQuoteAvailable);
        ledger.createBalance(SELLER_ID, BTC, 10L);
        ledger.createBalance(SELLER_ID, USDT, 0L);
        return ledger;
    }

    /** 保存单个测试场景的真实账本、引擎和不可变输入工厂。 */
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
    }
}
