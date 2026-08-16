package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * 验证订单不可变元数据的初始化与冲突拦截。
 *
 * <p>核心能力：覆盖首事件固化订单、用户和金额元数据，以及后续冲突事件拒绝。</p>
 * <p>线程安全：用例验证不可变元数据的只读校验，不在测试中共享可变状态。</p>
 * <p>使用限制：仅覆盖单订单元数据一致性，不验证外部序列化或跨进程传输。</p>
 */
class OrderMetadataTest {

    /** 场景：首个事件无论类型如何都应固定订单、用户和金额元数据。 */
    @Test
    void firstEventSeedsImmutableMetadata() {
        OrderEvent firstEvent = new OrderEvent(101L, 202L, 303L, 1_000L, OrderEventType.MATCH_FILLED);

        OrderContext context = OrderContext.fromFirstEvent(firstEvent);

        assertEquals(101L, context.orderId());
        assertEquals(202L, context.userId());
        assertEquals(303L, context.amount());
        assertEquals(OrderStatus.INIT, context.status());
        assertDoesNotThrow(() -> context.validateMetadata(firstEvent));
    }

    /** 场景：同订单 ID 的后续事件改变用户时必须拒绝。 */
    @Test
    void mismatchedUserIdFailsValidation() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(101L, 202L, 303L, 1_000L, OrderEventType.ORDER_CREATED));

        OrderEvent mismatched = new OrderEvent(101L, 999L, 303L, 1_001L, OrderEventType.MATCH_FILLED);

        assertThrows(OrderMetadataMismatchException.class, () -> context.validateMetadata(mismatched));
    }

    /** 场景：同订单 ID 的后续事件改变金额时必须拒绝。 */
    @Test
    void mismatchedAmountFailsValidation() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(101L, 202L, 303L, 1_000L, OrderEventType.ORDER_CREATED));

        OrderEvent mismatched = new OrderEvent(101L, 202L, 999L, 1_001L, OrderEventType.ORDER_CANCELLED);

        assertThrows(OrderMetadataMismatchException.class, () -> context.validateMetadata(mismatched));
    }
}
