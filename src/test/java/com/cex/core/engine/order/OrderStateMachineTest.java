package com.cex.core.engine.order;

import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.ledger.LedgerBalance;
import com.cex.core.engine.ledger.LedgerService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    /** 验证审批通过事件只释放 RISK_HOLD 订单，并恢复挂起前状态。 */
    @Test
    void riskReleaseRestoresTheStateBeforeHold() {
        OrderStateMachine machine = new OrderStateMachine();
        machine.apply(OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));
        machine.apply(OrderEvent.matchFilled(2L, 10L, 4L));
        machine.apply(OrderEvent.riskHold(3L, 10L));

        EventApplyResult released = machine.apply(OrderEvent.riskReleased(4L, 10L));

        assertEquals(EventApplyStatus.APPLIED, released.getStatus());
        assertEquals(OrderState.PARTIAL_FILLED, machine.get(10L).getState());
        assertEquals(4L, machine.get(10L).getFilledQuantity());
    }

    /** 验证非挂起订单收到放行事件时不会改变业务状态。 */
    @Test
    void riskReleaseIsIgnoredWhenOrderIsNotHeld() {
        OrderStateMachine machine = new OrderStateMachine();
        machine.apply(OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));

        EventApplyResult released = machine.apply(OrderEvent.riskReleased(4L, 10L));

        assertEquals(EventApplyStatus.IGNORED, released.getStatus());
        assertEquals(OrderState.CREATED, machine.get(10L).getState());
    }

    @Test
    void matchedOrderSettlesBuyerFrozenFundsToSeller() {
        LedgerService ledger = new LedgerService(4);
        ledger.openAccount(7L, 1_000L);
        ledger.openAccount(8L, 500L);
        assertTrue(ledger.freeze(7L, 100L));

        OrderStateMachine machine = new OrderStateMachine(ledger);
        machine.apply(OrderEvent.created(1L, 10L, 7L,
                "BTC-USDT", 50_000L, 10L));

        EventApplyResult result = machine.apply(OrderEvent.matchFilled(
                2L, 10L, 10L, 100L, 7L, 8L, 100L));

        assertEquals(EventApplyStatus.APPLIED, result.getStatus());
        assertEquals(OrderState.FILLED, machine.get(10L).getState());
        LedgerBalance buyer = ledger.snapshot(7L);
        LedgerBalance seller = ledger.snapshot(8L);
        assertEquals(900L, buyer.getAvailable());
        assertEquals(0L, buyer.getFrozen());
        assertEquals(600L, seller.getAvailable());
        assertTrue(buyer.isConserved());
        assertTrue(seller.isConserved());

        assertEquals(EventApplyStatus.DUPLICATE,
                machine.apply(OrderEvent.matchFilled(
                        2L, 10L, 10L, 100L, 7L, 8L, 100L)).getStatus());
        assertEquals(600L, ledger.snapshot(8L).getAvailable());
    }

    @Test
    void matchIsNotAppliedWhenSettlementFundsAreMissing() {
        LedgerService ledger = new LedgerService(4);
        ledger.openAccount(7L, 1_000L);
        ledger.openAccount(8L, 500L);
        OrderStateMachine machine = new OrderStateMachine(ledger);
        machine.apply(OrderEvent.created(1L, 10L, 7L,
                "BTC-USDT", 50_000L, 10L));

        EventApplyResult result = machine.apply(OrderEvent.matchFilled(
                2L, 10L, 10L, 100L, 7L, 8L, 100L));

        assertEquals(EventApplyStatus.SETTLEMENT_REJECTED, result.getStatus());
        assertEquals(OrderState.CREATED, machine.get(10L).getState());
        assertEquals(0L, machine.get(10L).getFilledQuantity());
        assertEquals(1_000L, ledger.snapshot(7L).getAvailable());
        assertEquals(500L, ledger.snapshot(8L).getAvailable());
    }

    @Test
    void bufferedMatchCanRetrySettlementAfterFundsBecomeAvailable() {
        LedgerService ledger = new LedgerService(4);
        ledger.openAccount(7L, 1_000L);
        ledger.openAccount(8L, 500L);
        OrderStateMachine machine = new OrderStateMachine(ledger);
        OrderEvent match = OrderEvent.matchFilled(
                2L, 10L, 10L, 100L, 7L, 8L, 100L);

        assertEquals(EventApplyStatus.BUFFERED, machine.apply(match).getStatus());
        assertEquals(EventApplyStatus.SETTLEMENT_REJECTED,
                machine.apply(OrderEvent.created(1L, 10L, 7L,
                        "BTC-USDT", 50_000L, 10L)).getStatus());
        assertEquals(1, machine.pendingEventCount(10L));

        assertTrue(ledger.freeze(7L, 100L));
        assertEquals(EventApplyStatus.APPLIED, machine.apply(match).getStatus());
        assertEquals(0, machine.pendingEventCount(10L));
        assertEquals(OrderState.FILLED, machine.get(10L).getState());
        assertEquals(600L, ledger.snapshot(8L).getAvailable());
    }
}
