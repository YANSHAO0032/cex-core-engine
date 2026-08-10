package com.cex.core.engine.order;

import com.cex.core.engine.event.OrderEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 订单状态机乱序和幂等测试工具。
 *
 * <p>覆盖滞后成交缓存、CREATE 后 replay、重复 eventId 和终态迟到事件。</p>
 */
class OrderStateMachineTest {

    /** 验证订单创建前到达的成交事件会被缓存并在创建后补偿执行。 */
    @Test
    void replaysMatchReceivedBeforeCreate() {
        OrderStateMachine machine = new OrderStateMachine();

        EventApplyResult buffered = machine.apply(
                OrderEvent.matchFilled(100L, 10L, 4L));
        assertEquals(EventApplyStatus.BUFFERED, buffered.getStatus());
        assertNull(machine.get(10L));

        EventApplyResult created = machine.apply(
                OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));
        assertEquals(EventApplyStatus.APPLIED, created.getStatus());
        assertEquals(OrderState.PARTIAL_FILLED, created.getOrder().getState());
        assertEquals(4L, created.getOrder().getFilledQuantity());
        assertEquals(0, machine.pendingEventCount(10L));
    }

    /** 验证同一成交 eventId 重复提交十次只累计一次成交数量。 */
    @Test
    void duplicateEventIdIsAppliedOnlyOnce() {
        OrderStateMachine machine = new OrderStateMachine();
        machine.apply(OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));

        machine.apply(OrderEvent.matchFilled(100L, 10L, 4L));
        for (int i = 0; i < 10; i++) {
            EventApplyResult duplicate = machine.apply(
                    OrderEvent.matchFilled(100L, 10L, 4L));
            assertEquals(EventApplyStatus.DUPLICATE, duplicate.getStatus());
        }

        assertEquals(4L, machine.get(10L).getFilledQuantity());
        assertEquals(OrderState.PARTIAL_FILLED, machine.get(10L).getState());
    }

    /** 验证未知订单的重复事件只会进入 pending 队列一次。 */
    @Test
    void duplicateUnknownEventIsBufferedOnlyOnce() {
        OrderStateMachine machine = new OrderStateMachine();

        assertEquals(EventApplyStatus.BUFFERED,
                machine.apply(OrderEvent.matchFilled(100L, 10L, 4L)).getStatus());
        assertEquals(EventApplyStatus.DUPLICATE,
                machine.apply(OrderEvent.matchFilled(100L, 10L, 4L)).getStatus());
        assertEquals(1, machine.pendingEventCount(10L));
    }

    /** 验证撤单后到达的成交事件不会改变已撤订单。 */
    @Test
    void cancelPreventsLaterFillFromChangingTheOrder() {
        OrderStateMachine machine = new OrderStateMachine();
        machine.apply(OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));
        machine.apply(OrderEvent.cancelled(2L, 10L));

        EventApplyResult ignored = machine.apply(OrderEvent.matchFilled(3L, 10L, 10L));
        assertEquals(EventApplyStatus.IGNORED, ignored.getStatus());
        assertEquals(OrderState.CANCELLED, machine.get(10L).getState());
    }
}
