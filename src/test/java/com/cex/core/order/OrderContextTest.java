package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * 验证强类型订单上下文的可见性、序号登记和提交边界。
 *
 * <p>核心能力：覆盖不可变元数据、未来事件登记、序号空洞和未持锁读可见性。</p>
 * <p>线程安全：状态修改由调用方持有用户锁，测试直接验证锁内状态机契约。</p>
 * <p>使用限制：仅验证单 JVM 内订单上下文的并发语义，不覆盖跨进程或持久化恢复。</p>
 */
class OrderContextTest {
    /** 创建订单上下文测试实例。 */
    OrderContextTest() {
    }


    /**
     * 场景：未持锁读取状态时，状态字段必须具备 volatile 可见性。
     *
     * @throws Exception 反射读取状态字段失败时抛出
     */
    @Test
    void statusFieldIsVolatileForUnlockedReaders() throws Exception {
        int modifiers = OrderContext.class.getDeclaredField("status").getModifiers();
        assertTrue(Modifier.isVolatile(modifiers));
    }

    /** 场景：强类型提交应初始化订单元数据、剩余量和起始权威序号。 */
    @Test
    void submissionInitializesTypedOrderState() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));

        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 7L, 100L));

        assertEquals(11L, context.orderId());
        assertEquals(21L, context.userId());
        assertEquals(OrderSide.BUY, context.side());
        assertEquals(pair, context.pair());
        assertEquals(10L, context.originalBaseQuantity());
        assertEquals(1_000L, context.originalReservedAmount());
        assertEquals(900L, context.riskQuoteAmount());
        assertEquals(0L, context.cumulativeBaseFilled());
        assertEquals(0L, context.cumulativeQuoteFilled());
        assertEquals(10L, context.remainingBaseQuantity());
        assertEquals(1_000L, context.remainingReservedAmount());
        assertEquals(7L, context.lastAppliedSequence());
        assertEquals(OrderStatus.NEW, context.status());
    }

    /** 场景：未来事件按序缓存，相同载荷幂等，不同载荷形成确定性协议冲突。 */
    @Test
    void sequencedEventRegistrationBuffersDuplicatesAndRejectsConflicts() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(2);
        TradeOrderReference future = new TradeOrderReference(30L, 11L, 3L);
        TradeOrderReference next = new TradeOrderReference(20L, 11L, 2L);

        assertEquals(SequenceRegistrationResult.BUFFERED,
                machine.registerEventLocked(context, future));
        assertEquals(SequenceRegistrationResult.DUPLICATE,
                machine.registerEventLocked(context, future));
        assertEquals(SequenceRegistrationResult.READY,
                machine.registerEventLocked(context, next));
        assertEquals(next, machine.nextEventLocked(context));
        assertThrows(TradeSequenceConflictException.class,
                () -> machine.registerEventLocked(context,
                        new TradeOrderReference(21L, 11L, 2L)));
    }

    /** 场景：已消费序号属于过期事件，缓存达到边界后不得接受新的未来事件。 */
    @Test
    void sequenceRegistrationIsStaleAndBoundedDeterministically() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(1);

        assertEquals(SequenceRegistrationResult.STALE,
                machine.registerEventLocked(context,
                        new TradeOrderReference(10L, 11L, 1L)));
        assertEquals(SequenceRegistrationResult.BUFFERED,
                machine.registerEventLocked(context,
                        new TradeOrderReference(30L, 11L, 3L)));
        assertThrows(IllegalStateException.class,
                () -> machine.registerEventLocked(context,
                        new TradeOrderReference(40L, 11L, 4L)));
    }

    /** 场景：容量只限制未来事件，已就绪的下一事件不得占用未来缓存配额。 */
    @Test
    void readyEventDoesNotConsumeFutureEventCapacity() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(1);

        assertEquals(SequenceRegistrationResult.READY,
                machine.registerEventLocked(context,
                        new TradeOrderReference(20L, 11L, 2L)));
        assertEquals(SequenceRegistrationResult.BUFFERED,
                machine.registerEventLocked(context,
                        new TradeOrderReference(30L, 11L, 3L)));
        assertThrows(IllegalStateException.class,
                () -> machine.registerEventLocked(context,
                        new TradeOrderReference(40L, 11L, 4L)));
    }

    /** 场景：准备事件登记只校验并绑定变更，提交前不得修改订单待处理映射。 */
    @Test
    void preparedEventRegistrationMutatesOnlyWhenCommitted() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(1);
        TradeOrderReference future = new TradeOrderReference(30L, 11L, 3L);

        OrderEventRegistrationMutation mutation =
                machine.prepareEventRegistrationLocked(context, future);

        assertEquals(0, machine.pendingEventCountLocked(context));
        assertEquals(1L, context.lastAppliedSequence());
        machine.commitEventRegistrationLocked(mutation);
        assertEquals(1, machine.pendingEventCountLocked(context));
        assertEquals(SequenceRegistrationResult.DUPLICATE,
                machine.registerEventLocked(context, future));
    }

    /** 场景：冲突或容量不足的登记准备不得修改既有事件、序号或订单累计字段。 */
    @Test
    void failedEventRegistrationPreparationLeavesOrderUnchanged() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(1);
        TradeOrderReference existing = new TradeOrderReference(30L, 11L, 3L);
        machine.registerEventLocked(context, existing);

        assertThrows(TradeSequenceConflictException.class,
                () -> machine.prepareEventRegistrationLocked(context,
                        new TradeOrderReference(31L, 11L, 3L)));
        assertThrows(IllegalStateException.class,
                () -> machine.prepareEventRegistrationLocked(context,
                        new TradeOrderReference(40L, 11L, 4L)));

        assertEquals(1, machine.pendingEventCountLocked(context));
        assertEquals(1L, context.lastAppliedSequence());
        assertEquals(0L, context.cumulativeBaseFilled());
        assertEquals(OrderStatus.NEW, context.status());
    }

    /** 场景：消费序号不得绕过尚未到达的较低权威序号。 */
    @Test
    void sequenceConsumptionCannotSkipGap() {
        TradingPair pair = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));
        OrderContext context = OrderContext.fromSubmission(new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair,
                10L, 1_000L, 900L, 1L, 100L));
        OrderStateMachine machine = new OrderStateMachine(4);
        TradeOrderReference sequenceFour = new TradeOrderReference(40L, 11L, 4L);
        machine.registerEventLocked(context, sequenceFour);

        assertThrows(TradeSequenceConflictException.class,
                () -> machine.prepareSequenceLocked(context, sequenceFour));

        assertEquals(1L, context.lastAppliedSequence());
        assertEquals(1, machine.pendingEventCountLocked(context));

        TradeOrderReference sequenceTwo = new TradeOrderReference(20L, 11L, 2L);
        machine.registerEventLocked(context, sequenceTwo);
        OrderSequenceMutation mutation = machine.prepareSequenceLocked(context, sequenceTwo);
        machine.commitSequenceLocked(mutation);

        assertEquals(2L, context.lastAppliedSequence());
        assertEquals(1, machine.pendingEventCountLocked(context));
        assertNull(machine.nextEventLocked(context));
    }

}
