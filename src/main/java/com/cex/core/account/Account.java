package com.cex.core.account;

public final class Account {

    private final long userId;
    private long available;
    private long frozen;

    Account(long userId, long available, long frozen) {
        this.userId = userId;
        this.available = available;
        this.frozen = frozen;
    }

    public long userId() {
        return userId;
    }

    public long available() {
        return available;
    }

    public long frozen() {
        return frozen;
    }

    void setAvailable(long available) {
        this.available = available;
    }

    void setFrozen(long frozen) {
        this.frozen = frozen;
    }
}
