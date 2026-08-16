package com.cex.core.order;

public enum OrderFact {
    CREATED_SEEN(1 << 0),
    FILLED_SEEN(1 << 1),
    CANCELLED_SEEN(1 << 2),
    APPROVED_SEEN(1 << 3),
    REJECTED_SEEN(1 << 4);

    private final int mask;

    OrderFact(int mask) {
        this.mask = mask;
    }

    public int mask() {
        return mask;
    }

    public static OrderFact fromEventType(OrderEventType eventType) {
        return switch (eventType) {
            case ORDER_CREATED -> CREATED_SEEN;
            case MATCH_FILLED -> FILLED_SEEN;
            case ORDER_CANCELLED -> CANCELLED_SEEN;
            case APPROVAL_PASSED -> APPROVED_SEEN;
            case APPROVAL_REJECTED -> REJECTED_SEEN;
        };
    }
}
