package com.cex.core.risk;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 风控阈值、规则快照替换与成交时间窗口的单元测试。
 * 核心能力：验证 10 秒窗口过期、copy-on-write 规则更新及短路行为；线程安全：使用闭锁协调异步审批；使用限制：仅验证内存实现的确定性行为。
 */
class RiskEngineTest {
    /**
     * 场景：已结算金额超过阈值触发审批，并可通过推进时钟跨越 10 秒窗口而过期。
     *
     * @throws Exception 审批线程同步、结果等待或引擎关闭失败时抛出
     */
    @Test
    void thresholdUsesSettledAmountAndExpiresWithoutSleeping() throws Exception {
        CountDownLatch approvalEntered = new CountDownLatch(1);
        CountDownLatch releaseApproval = new CountDownLatch(1);
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createAccount(1L, 1000L);
        ManualClock clock = new ManualClock(100L);
        ApprovalService approvals = new ApprovalService(1, 8);
        OrderEngine engine = new OrderEngine(ledger,
                new RiskPipeline(new SlidingWindowAmountRule(100L)), clock, approvals, event -> {
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
            engine.process(event(1L, 120L, OrderEventType.ORDER_CREATED));
            engine.process(event(1L, 120L, OrderEventType.MATCH_FILLED));
            engine.process(event(2L, 50L, OrderEventType.ORDER_CREATED));
            assertEquals(OrderStatus.RISK_HOLD, engine.order(2L).status());
            assertTrue(approvalEntered.await(2, java.util.concurrent.TimeUnit.SECONDS));
            releaseApproval.countDown();
            engine.awaitApprovals(2, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(OrderStatus.NEW, engine.order(2L).status());

            clock.advanceMillis(10_001L);
            engine.process(event(3L, 50L, OrderEventType.ORDER_CREATED));
            assertEquals(OrderStatus.NEW, engine.order(3L).status());
            assertEquals(1L, engine.metrics().riskHoldCount());
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
        RiskRule countingPass = context -> { second.incrementAndGet(); return RiskDecision.PASS; };
        RiskPipeline pipeline = new RiskPipeline(hold, countingPass);
        RiskContext context = new RiskContext(1L, 1L, 1L, 1L, 1L);
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
        window.record(1000L, 50L);
        assertEquals(90L, window.currentSum(10_000L));
        assertEquals(50L, window.currentSum(10_101L));
    }

    /**
     * 创建测试用订单事件。
     *
     * @param orderId 订单标识
     * @param amount 订单金额
     * @param type 订单事件类型
     * @return 固定用户和时间的测试订单事件
     */
    private static OrderEvent event(long orderId, long amount, OrderEventType type) {
        return new OrderEvent(orderId, 1L, amount, 100L, type);
    }
}
