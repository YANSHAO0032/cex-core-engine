package com.cex.core.order;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public final class OrderContext {

    private final long orderId;
    private final long userId;
    private final long amount;
    private final AtomicInteger factBits = new AtomicInteger();

    private int effectBits;
    private volatile OrderStatus status = OrderStatus.INIT;
    private boolean terminalConflictRecorded;
    private boolean approvalConflictRecorded;

    private OrderContext(long orderId, long userId, long amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    public static OrderContext fromFirstEvent(OrderEvent firstEvent) {
        Objects.requireNonNull(firstEvent, "firstEvent");
        return new OrderContext(firstEvent.orderId(), firstEvent.userId(), firstEvent.amount());
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

    public OrderStatus status() {
        return status;
    }

    public void setStatusLocked(OrderStatus status) {
        this.status = Objects.requireNonNull(status, "status");
    }

    public void validateMetadata(OrderEvent event) {
        Objects.requireNonNull(event, "event");
        if (orderId != event.orderId() || userId != event.userId() || amount != event.amount()) {
            throw new OrderMetadataMismatchException(
                    "order metadata mismatch for orderId=" + orderId);
        }
    }

    public FactRegistrationResult registerFact(OrderEventType eventType) {
        int mask = OrderFact.fromEventType(Objects.requireNonNull(eventType, "eventType")).mask();
        while (true) {
            int current = factBits.get();
            if ((current & mask) != 0) {
                return FactRegistrationResult.DUPLICATE;
            }
            int updated = current | mask;
            if (factBits.compareAndSet(current, updated)) {
                return FactRegistrationResult.NEW;
            }
        }
    }

    public boolean hasFact(OrderFact fact) {
        return (factBits.get() & fact.mask()) != 0;
    }

    public boolean hasEffect(OrderEffect effect) {
        return (effectBits & effect.mask()) != 0;
    }

    public boolean applyEffectLocked(OrderEffect effect, LockedEffectOperation operation) {
        Objects.requireNonNull(effect, "effect");
        Objects.requireNonNull(operation, "operation");
        if (hasEffect(effect)) {
            return false;
        }
        operation.run();
        effectBits |= effect.mask();
        return true;
    }

    public boolean markTerminalConflictLocked() {
        if (terminalConflictRecorded) {
            return false;
        }
        terminalConflictRecorded = true;
        return true;
    }

    public boolean markApprovalConflictLocked() {
        if (approvalConflictRecorded) {
            return false;
        }
        approvalConflictRecorded = true;
        return true;
    }

    @FunctionalInterface
    public interface LockedEffectOperation {
        void run();
    }
}
