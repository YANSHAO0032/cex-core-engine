package com.cex.core.risk;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderSubmission;
import com.cex.core.order.TradingPair;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 审批结果回流、订单乱序成交门禁与冻结资金处理的集成场景测试。
 * 核心能力：验证审批只发布事件并经统一订单入口生效；线程安全：通过闭锁协调异步审批；使用限制：依赖测试专用内存账务组件。
 */
class ApprovalTest {
    /**
     * 场景：审批服务只发布拒绝事件，不直接修改订单状态或账务。
     *
     * @throws Exception 审批任务等待或服务关闭失败时抛出
     */
    @Test
    void approvalServiceEmitsStrongResultWithoutChangingOrderState() throws Exception {
        ApprovalService service = new ApprovalService(1, 1);
        try {
            AssetId btc = new AssetId("BTC");
            AssetId usdt = new AssetId("USDT");
            OrderSubmission submission = new OrderSubmission(
                    1L, 1L, OrderSide.BUY, new TradingPair(btc, usdt),
                    1L, 10L, 10L, 1L, 1L);
            AtomicReference<ApprovalResult> received = new AtomicReference<>();

            service.submit(submission,
                    source -> ApprovalDecision.REJECT, received::set);
            service.awaitQuiescence(2, TimeUnit.SECONDS);

            assertNotNull(received.get());
            assertEquals(1L, received.get().orderId());
            assertEquals(ApprovalDecision.REJECT, received.get().decision());
            assertTrue(received.get().decidedAtMillis() >= 0L);
            assertEquals(1L, service.submittedCount());
        } finally { service.close(); }
    }

    /**
     * 场景：审批拒绝经统一入口仅解冻一次，即使原始创建事件重复到达。
     *
     * @throws Exception 审批任务等待或引擎关闭失败时抛出
     */
    @Test
    void rejectUnfreezesThroughUnifiedOrderEntryExactlyOnce() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createAccount(1L, 1000L);
        ApprovalService approvals = new ApprovalService(1, 4);
        OrderEngine engine = new OrderEngine(ledger,
                new RiskPipeline(new SlidingWindowAmountRule(100L)), new ManualClock(1L), approvals,
            event -> ApprovalDecision.REJECT);
        try {
            engine.process(new OrderEvent(1L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.process(new OrderEvent(1L, 1L, 100L, 1L, OrderEventType.MATCH_FILLED));
            engine.process(new OrderEvent(2L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.process(new OrderEvent(2L, 1L, 100L, 1L, OrderEventType.ORDER_CREATED));
            engine.awaitApprovals(2, TimeUnit.SECONDS);
            assertEquals(OrderStatus.CANCELED, engine.order(2L).status());
            assertEquals(900L, ledger.getRequiredAccount(1L).available());
            assertEquals(0L, ledger.getRequiredAccount(1L).frozen());
            assertEquals(1L, engine.metrics().approvalScheduledCount());
            assertEquals(1L, engine.metrics().unfreezeCount());
        } finally { engine.close(); }
    }

    /**
     * 场景：风险挂起期间乱序或重复成交被门禁拦截，拒绝后取消且不再结算。
     *
     * @throws Exception 审批线程同步或测试夹具关闭失败时抛出
     */
    @Test
    void fillDuringRiskHoldWaitsAndRejectedApprovalCancelsWithoutSettlement() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();
            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());

            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.MATCH_FILLED);

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(900L, fixture.ledger.getRequiredAccount(1L).available());
            assertEquals(0L, fixture.ledger.getRequiredAccount(1L).frozen());
            assertEquals(1L, fixture.engine.metrics().settleCount());
            assertEquals(1L, fixture.engine.metrics().unfreezeCount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    /**
     * 场景：审批通过后仅回放一次挂起期间缓存的成交。
     *
     * @throws Exception 审批线程同步或测试夹具关闭失败时抛出
     */
    @Test
    void approvedRiskHoldAppliesCachedFillExactlyOnce() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.PASS)) {
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();
            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.MATCH_FILLED);

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

            assertEquals(OrderStatus.FILLED, fixture.engine.order(2L).status());
            assertEquals(200L, fixture.ledger.systemSettledAmount());
            assertEquals(2L, fixture.engine.metrics().settleCount());
            assertEquals(0L, fixture.engine.metrics().unfreezeCount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    /**
     * 场景：成交先于创建到达时仍先进入风险挂起，审批拒绝后不发生结算。
     *
     * @throws Exception 审批线程同步或测试夹具关闭失败时抛出
     */
    @Test
    void fillBeforeCreateStillEntersRiskHoldBeforeSettlement() throws Exception {
        try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
            fixture.process(2L, OrderEventType.MATCH_FILLED);
            fixture.process(2L, OrderEventType.ORDER_CREATED);
            fixture.awaitApprovalEntry();

            assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

            fixture.releaseApproval.countDown();
            fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);
            assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertTrue(fixture.ledger.invariantHolds());
        }
    }

    /**
     * 通过闭锁阻塞审批工作线程的集成测试夹具。
     * 核心能力：稳定复现审批未返回时的订单状态；线程安全：闭锁提供跨线程可见性；使用限制：仅供本测试类使用。
     */
    private static final class BlockingApprovalFixture implements AutoCloseable {
        /** 测试账务账本，用于校验资金冻结与结算。 */
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        /** 审批策略已开始执行的测试同步信号。 */
        private final CountDownLatch approvalEntered = new CountDownLatch(1);
        /** 允许被阻塞审批策略返回决定的测试同步信号。 */
        private final CountDownLatch releaseApproval = new CountDownLatch(1);
        /** 容量为 8 的测试审批服务，承载回流审批任务。 */
        private final ApprovalService approvals = new ApprovalService(1, 8);
        /** 接收原始与审批回流事件的订单引擎。 */
        private final OrderEngine engine;

        /**
         * 建立已完成首笔成交、第二笔将触发风险审批的夹具。
         *
         * @param decision 解除阻塞后审批策略返回的结果
         */
        private BlockingApprovalFixture(ApprovalDecision decision) {
            ledger.createAccount(1L, 1_000L);
            engine = new OrderEngine(
                    ledger,
                    new RiskPipeline(new SlidingWindowAmountRule(100L)),
                    new ManualClock(1L),
                    approvals,
                    event -> {
                        approvalEntered.countDown();
                        try {
                            releaseApproval.await();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return ApprovalDecision.REJECT;
                        }
                        return decision;
                    });
            process(1L, OrderEventType.ORDER_CREATED);
            process(1L, OrderEventType.MATCH_FILLED);
        }

        /** 向订单引擎投递测试事件。
         * @param orderId 订单标识
         * @param type 订单事件类型
         */
        private void process(long orderId, OrderEventType type) {
            engine.process(new OrderEvent(orderId, 1L, 100L, 1L, type));
        }

        /** 等待审批策略进入阻塞点。
         * @throws InterruptedException 当等待线程被中断时抛出
         */
        private void awaitApprovalEntry() throws InterruptedException {
            assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
        }

        /** 释放审批并关闭订单引擎及其审批服务。 */
        @Override
        public void close() {
            releaseApproval.countDown();
            engine.close();
        }
    }
}
