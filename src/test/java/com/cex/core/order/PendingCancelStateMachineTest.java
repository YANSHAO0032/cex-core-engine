package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 验证撤单请求、等待确认及确认到达后的单订单状态转换。
 *
 * <p>核心能力：覆盖等待撤单期间继续成交、剩余冻结额释放和完全成交优先规则。</p>
 * <p>线程安全：测试调用均代表已处于同一用户锁临界区，订单上下文不跨线程共享。</p>
 * <p>使用限制：不发送外部撤单请求，也不直接修改账户资金。</p>
 */
class PendingCancelStateMachineTest {
    /** 创建等待撤单状态机测试实例。 */
    PendingCancelStateMachineTest() {
    }


    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);

    /** 场景：等待撤单期间发生部分成交时应继续保持等待状态。 */
    @Test
    void partialFillDuringPendingCancelKeepsPendingState() {
        OrderContext order = partialBuyOrder();
        OrderStateMachine machine = new OrderStateMachine(1_024);

        assertTrue(machine.requestCancelLocked(
                order, new CancelRequest(90L, order.orderId(), 10L)));
        assertFalse(machine.requestCancelLocked(
                order, new CancelRequest(90L, order.orderId(), 11L)));
        assertEquals(OrderStatus.PENDING_CANCEL, order.status());

        machine.applyFillLocked(order, buyExecution(2L, 2L, 200L, 3L));

        assertEquals(OrderStatus.PENDING_CANCEL, order.status());
        assertEquals(6L, order.cumulativeBaseFilled());
        assertEquals(400L, order.remainingReservedAmount());
    }

    /** 场景：撤单确认只能释放成交后仍被冻结的资产。 */
    @Test
    void cancelConfirmationReleasesOnlyRemainingReserve() {
        OrderContext order = partialBuyOrder();
        OrderStateMachine machine = new OrderStateMachine(1_024);
        machine.requestCancelLocked(order, new CancelRequest(90L, order.orderId(), 10L));

        OrderCancelMutation mutation = machine.prepareCancelLocked(
                order, new CancelConfirmation(90L, order.orderId(), 3L, 20L));

        assertEquals(600L, mutation.releaseAmount());
        assertEquals(OrderStatus.PENDING_CANCEL, order.status());
        assertEquals(600L, order.remainingReservedAmount());
        machine.commitCancelLocked(order, mutation);
        assertEquals(OrderStatus.CANCELED, order.status());
        assertEquals(0L, order.remainingReservedAmount());
        assertEquals(3L, order.lastAppliedSequence());
    }

    /** 场景：等待撤单期间的完整成交优先形成成交终态，确认不得反向改为取消。 */
    @Test
    void completeFillDuringPendingCancelWinsOverConfirmation() {
        OrderContext order = partialBuyOrder();
        OrderStateMachine machine = new OrderStateMachine(1_024);
        machine.requestCancelLocked(order, new CancelRequest(90L, order.orderId(), 10L));
        machine.applyFillLocked(order, buyExecution(2L, 6L, 550L, 3L));

        assertEquals(OrderStatus.FILLED, order.status());
        assertEquals(0L, order.remainingReservedAmount());

        OrderCancelMutation staleConfirmation = machine.prepareCancelLocked(
                order, new CancelConfirmation(90L, order.orderId(), 4L, 20L));
        assertEquals(0L, staleConfirmation.releaseAmount());
        machine.commitCancelLocked(order, staleConfirmation);

        assertEquals(OrderStatus.FILLED, order.status());
        assertEquals(0L, order.remainingReservedAmount());
    }

    /** 场景：撤单确认必须匹配已提交请求并占用订单的下一权威序号。 */
    @Test
    void cancelConfirmationRequiresRequestIdentityAndNextSequence() {
        OrderContext order = partialBuyOrder();
        OrderStateMachine machine = new OrderStateMachine(1_024);
        machine.requestCancelLocked(order, new CancelRequest(90L, order.orderId(), 10L));

        assertThrows(TradeSequenceConflictException.class,
                () -> machine.prepareCancelLocked(order,
                        new CancelConfirmation(90L, order.orderId(), 4L, 20L)));
        assertThrows(IllegalArgumentException.class,
                () -> machine.prepareCancelLocked(order,
                        new CancelConfirmation(91L, order.orderId(), 3L, 20L)));

        assertEquals(OrderStatus.PENDING_CANCEL, order.status());
        assertEquals(2L, order.lastAppliedSequence());
        assertEquals(600L, order.remainingReservedAmount());
    }

    /**
     * 创建已部分成交的买单。
     *
     * @return 已消费序号 2 的部分成交买单
     */
    private static OrderContext partialBuyOrder() {
        OrderContext order = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, BTC_USDT,
                10L, 1_000L, 1_000L, 1L, 0L));
        new OrderStateMachine(1_024).applyFillLocked(
                order, buyExecution(1L, 4L, 400L, 2L));
        return order;
    }

    /**
     * 构造指向固定买单的成交。
     *
     * @param tradeId 成交标识
     * @param baseQuantity 基础资产成交数量
     * @param quoteQuantity 报价资产成交数量
     * @param buySequence 买单权威序号
     * @return 测试成交
     */
    private static TradeExecution buyExecution(
            long tradeId, long baseQuantity, long quoteQuantity, long buySequence) {
        return new TradeExecution(
                tradeId, 11L, 22L, BTC_USDT,
                baseQuantity, quoteQuantity, buySequence, 2L, 1L);
    }
}
