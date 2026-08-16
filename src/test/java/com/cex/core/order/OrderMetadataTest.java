package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class OrderMetadataTest {

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

    @Test
    void mismatchedUserIdFailsValidation() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(101L, 202L, 303L, 1_000L, OrderEventType.ORDER_CREATED));

        OrderEvent mismatched = new OrderEvent(101L, 999L, 303L, 1_001L, OrderEventType.MATCH_FILLED);

        assertThrows(OrderMetadataMismatchException.class, () -> context.validateMetadata(mismatched));
    }

    @Test
    void mismatchedAmountFailsValidation() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(101L, 202L, 303L, 1_000L, OrderEventType.ORDER_CREATED));

        OrderEvent mismatched = new OrderEvent(101L, 202L, 999L, 1_001L, OrderEventType.ORDER_CANCELLED);

        assertThrows(OrderMetadataMismatchException.class, () -> context.validateMetadata(mismatched));
    }
}
