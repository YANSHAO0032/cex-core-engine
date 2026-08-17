package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * 验证单订单部分成交的累计量、剩余冻结额与终态转换。
 *
 * <p>核心能力：覆盖买卖方向的两阶段成交计算、最终买方余款释放和确定性拒绝。</p>
 * <p>线程安全：所有场景均模拟已持有用户锁的串行临界区，不共享订单上下文。</p>
 * <p>使用限制：不执行账户资金变更，也不验证双边协调器的原子提交。</p>
 */
class PartialFillStateMachineTest {

    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");
    /** 测试交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(BTC, USDT);

    /** 场景：两笔连续成交应将新单依次推进至部分成交和完全成交。 */
    @Test
    void twoExecutionsMoveNewToPartialThenFilled() {
        OrderContext order = OrderContext.fromSubmission(buySubmission(10L, 1_000L));
        OrderStateMachine machine = new OrderStateMachine(1_024);

        machine.applyFillLocked(order, buyExecution(1L, 4L, 400L, 2L));
        assertEquals(OrderStatus.PARTIALLY_FILLED, order.status());
        assertEquals(4L, order.cumulativeBaseFilled());
        assertEquals(400L, order.cumulativeQuoteFilled());
        assertEquals(6L, order.remainingBaseQuantity());
        assertEquals(600L, order.remainingReservedAmount());
        assertEquals(2L, order.lastAppliedSequence());

        OrderFillMutation finalFill = machine.prepareFillLocked(
                order, buyExecution(2L, 6L, 550L, 3L));
        assertEquals(50L, finalFill.buyerQuoteReleaseAmount());
        assertEquals(OrderStatus.PARTIALLY_FILLED, order.status());
        assertEquals(4L, order.cumulativeBaseFilled());

        machine.commitFillLocked(order, finalFill);
        assertEquals(OrderStatus.FILLED, order.status());
        assertEquals(10L, order.cumulativeBaseFilled());
        assertEquals(950L, order.cumulativeQuoteFilled());
        assertEquals(0L, order.remainingBaseQuantity());
        assertEquals(0L, order.remainingReservedAmount());
        assertEquals(3L, order.lastAppliedSequence());
    }

    /** 场景：成交不得超过订单的剩余基础数量或剩余冻结资产。 */
    @Test
    void executionCannotOverfillBaseOrReserve() {
        OrderContext order = OrderContext.fromSubmission(buySubmission(10L, 1_000L));
        OrderStateMachine machine = new OrderStateMachine(1_024);

        assertThrows(InvalidTradeExecutionException.class,
                () -> machine.prepareFillLocked(
                        order, buyExecution(1L, 11L, 900L, 2L)));
        assertThrows(InvalidTradeExecutionException.class,
                () -> machine.prepareFillLocked(
                        order, buyExecution(2L, 1L, 1_001L, 2L)));

        assertEquals(0L, order.cumulativeBaseFilled());
        assertEquals(0L, order.cumulativeQuoteFilled());
        assertEquals(10L, order.remainingBaseQuantity());
        assertEquals(1_000L, order.remainingReservedAmount());
        assertEquals(1L, order.lastAppliedSequence());
    }

    /** 场景：卖单冻结额应按基础资产成交量递减而非按报价资产递减。 */
    @Test
    void sellReserveDecreasesByBaseQuantity() {
        OrderContext order = OrderContext.fromSubmission(new OrderSubmission(
                22L, 32L, OrderSide.SELL, BTC_USDT,
                10L, 10L, 1_000L, 1L, 0L));
        OrderStateMachine machine = new OrderStateMachine(1_024);

        machine.applyFillLocked(order, new TradeExecution(
                1L, 11L, 22L, BTC_USDT,
                4L, 400L, 2L, 2L, 1L));

        assertEquals(OrderStatus.PARTIALLY_FILLED, order.status());
        assertEquals(6L, order.remainingBaseQuantity());
        assertEquals(6L, order.remainingReservedAmount());
        assertEquals(400L, order.cumulativeQuoteFilled());
    }

    /** 场景：终态订单和非下一序号成交必须被确定性拒绝。 */
    @Test
    void terminalAndNonNextExecutionsAreRejected() {
        OrderContext order = OrderContext.fromSubmission(buySubmission(10L, 1_000L));
        OrderStateMachine machine = new OrderStateMachine(1_024);

        assertThrows(TradeSequenceConflictException.class,
                () -> machine.prepareFillLocked(
                        order, buyExecution(1L, 1L, 100L, 3L)));

        machine.applyFillLocked(order, buyExecution(2L, 10L, 950L, 2L));
        assertThrows(OrderTerminalStateException.class,
                () -> machine.prepareFillLocked(
                        order, buyExecution(3L, 1L, 50L, 3L)));
    }

    /**
     * 构造买单提交。
     *
     * @param baseQuantity 原始基础资产数量
     * @param reservedAmount 原始报价资产冻结额
     * @return 测试买单提交
     */
    private static OrderSubmission buySubmission(long baseQuantity, long reservedAmount) {
        return new OrderSubmission(
                11L, 21L, OrderSide.BUY, BTC_USDT,
                baseQuantity, reservedAmount, reservedAmount, 1L, 0L);
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
