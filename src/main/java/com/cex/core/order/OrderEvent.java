package com.cex.core.order;

import com.cex.core.util.MoneyMath;
import java.util.Objects;

public final class OrderEvent {

    private final long orderId;
    private final long userId;
    private final long amount;
    private final long eventTimeMillis;
    private final OrderEventType type;

    public OrderEvent(long orderId, long userId, long amount, long eventTimeMillis, OrderEventType type) {
        this.orderId = requirePositive(orderId, "orderId");
        this.userId = requirePositive(userId, "userId");
        this.amount = MoneyMath.requirePositive(amount);
        this.eventTimeMillis = MoneyMath.requireNonNegative(eventTimeMillis);
        this.type = Objects.requireNonNull(type, "type");
    }

    public long orderId() {
        return orderId;
    }

    public long userId() {
        return userId;
    }

    public long amount() {
        return amount;
    }

    public long eventTimeMillis() {
        return eventTimeMillis;
    }

    public OrderEventType type() {
        return type;
    }

    private static long requirePositive(long value, String fieldName) {
        if (value <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }
}
