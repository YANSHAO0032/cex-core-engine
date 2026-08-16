package com.cex.core.risk;

public final class RiskContext {
    private final long orderId;
    private final long userId;
    private final long amount;
    private final long nowMillis;
    private final long recentSettledAmount;

    public RiskContext(long orderId, long userId, long amount, long nowMillis, long recentSettledAmount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.nowMillis = nowMillis;
        this.recentSettledAmount = recentSettledAmount;
    }

    public long orderId() { return orderId; }
    public long userId() { return userId; }
    public long amount() { return amount; }
    public long nowMillis() { return nowMillis; }
    public long recentSettledAmount() { return recentSettledAmount; }
}
