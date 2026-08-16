package com.cex.core.order;

import com.cex.core.account.Account;
import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.RiskPipeline;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证订单状态机在乱序、重放、并发和元数据冲突下的收敛行为。
 *
 * <p>核心能力：覆盖滞后事件登记、创建后置补偿、重复事件幂等和元数据冲突拦截。</p>
 * <p>线程安全：并发用例使用同步门闩协调重放，验证 CAS 事实登记与用户分片锁下的资金操作收敛。</p>
 * <p>使用限制：仅覆盖内存引擎的代表性乱序组合，不模拟跨节点网络分区或持久化恢复。</p>
 */
class OutOfOrderStateMachineTest {
    /** 场景：成交先于创建到达时，创建事实到达后应冻结并结算为成交态。 */
    @Test
    void filledBeforeCreateConvergesToFilled() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(9L, 1L, 100L, OrderEventType.MATCH_FILLED));
            fixture.engine.process(event(9L, 1L, 100L, OrderEventType.ORDER_CREATED));

            assertEquals(OrderStatus.FILLED, fixture.engine.order(9L).status());
            assertEquals(900L, fixture.account().available());
            assertEquals(0L, fixture.account().frozen());
            assertEquals(100L, fixture.ledger.systemSettledAmount());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().settleCount());
        } finally {
            fixture.close();
        }
    }

    /** 场景：取消先于创建到达时，创建事实到达后应冻结、解冻并收敛为取消态。 */
    @Test
    void cancelledBeforeCreateConvergesToCanceled() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(10L, 1L, 100L, OrderEventType.ORDER_CANCELLED));
            fixture.engine.process(event(10L, 1L, 100L, OrderEventType.ORDER_CREATED));

            assertEquals(OrderStatus.CANCELED, fixture.engine.order(10L).status());
            assertEquals(1000L, fixture.account().available());
            assertEquals(0L, fixture.account().frozen());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().unfreezeCount());
        } finally {
            fixture.close();
        }
    }

    /** 场景：大量重复的创建和成交事件不得重复执行资金副作用。 */
    @Test
    void duplicateEventsStillReconcileExactlyOnce() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            IntStream.range(0, 20).forEach(i -> fixture.engine.process(
                    event(11L, 1L, 100L, OrderEventType.MATCH_FILLED)));
            IntStream.range(0, 10).forEach(i -> fixture.engine.process(
                    event(11L, 1L, 100L, OrderEventType.ORDER_CREATED)));

            assertEquals(OrderStatus.FILLED, fixture.engine.order(11L).status());
            assertEquals(1L, fixture.engine.metrics().freezeCount());
            assertEquals(1L, fixture.engine.metrics().settleCount());
            assertTrue(fixture.engine.metrics().duplicateEvents() >= 28L);
        } finally {
            fixture.close();
        }
    }

    /**
     * 场景：同订单的并发重放与线程中断不应妨碍最终收敛。
     *
     * @throws Exception 并发线程等待或测试资源关闭失败时抛出
     */
    @Test
    void sameOrderConcurrentDuplicatesAndInterruptComplete() throws Exception {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            Thread.currentThread().interrupt();
            fixture.engine.process(event(12L, 1L, 100L, OrderEventType.MATCH_FILLED));
            fixture.engine.process(event(12L, 1L, 100L, OrderEventType.ORDER_CREATED));
            assertTrue(Thread.interrupted());
            CountDownLatch start = new CountDownLatch(1);
            Thread[] threads = new Thread[32];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        start.await();
                        fixture.engine.process(event(12L, 1L, 100L, OrderEventType.MATCH_FILLED));
                        fixture.engine.process(event(12L, 1L, 100L, OrderEventType.ORDER_CREATED));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        fail(e);
                    }
                });
                threads[i].start();
            }
            start.countDown();
            for (Thread thread : threads) {
                thread.join(5000L);
                assertFalse(thread.isAlive());
            }
            assertEquals(OrderStatus.FILLED, fixture.engine.order(12L).status());
            assertEquals(1L, fixture.engine.metrics().settleCount());
        } finally {
            Thread.interrupted();
            fixture.close();
        }
    }

    /** 场景：相同订单 ID 的不一致用户元数据必须被拒绝并计数。 */
    @Test
    void metadataMismatchIsRejected() {
        Fixture fixture = new Fixture(event -> ApprovalDecision.PASS);
        try {
            fixture.engine.process(event(13L, 1L, 100L, OrderEventType.ORDER_CREATED));
            assertThrows(OrderMetadataMismatchException.class,
                    () -> fixture.engine.process(event(13L, 2L, 100L, OrderEventType.MATCH_FILLED)));
            assertEquals(1L, fixture.engine.metrics().metadataConflictEvents());
        } finally {
            fixture.close();
        }
    }

    /**
     * 构造指定元数据的测试事件。
     *
     * @param orderId 订单 ID
     * @param userId 用户 ID
     * @param amount 订单金额
     * @param type 事件类型
     * @return 测试订单事件
     */
    private static OrderEvent event(long orderId, long userId, long amount, OrderEventType type) {
        return new OrderEvent(orderId, userId, amount, 1L, type);
    }

    /** 乱序状态机测试的账户、审批服务和订单引擎夹具。 */
    private static final class Fixture implements AutoCloseable {
        /** 使用的账户账本。 */
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        /** 使用的异步审批服务。 */
        private final ApprovalService approvals = new ApprovalService(1, 16);
        /** 被测订单引擎。 */
        private final OrderEngine engine;

        /**
         * 创建并初始化测试账户和引擎。
         *
         * @param policy 测试审批策略
         */
        private Fixture(com.cex.core.risk.ApprovalPolicy policy) {
            ledger.createAccount(1L, 1000L);
            engine = new OrderEngine(ledger, new RiskPipeline(), new com.cex.core.risk.ManualClock(1L), approvals, policy);
        }

        /**
         * 返回测试用户账户。
         *
         * @return 用户 1 的账户
         */
        private Account account() { return ledger.getRequiredAccount(1L); }
        /** 关闭夹具创建的引擎资源。 */
        @Override public void close() { engine.close(); }
    }
}
