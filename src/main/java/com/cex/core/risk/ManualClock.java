package com.cex.core.risk;

import java.util.concurrent.atomic.AtomicLong;

public final class ManualClock implements Clock {
    private final AtomicLong now;

    public ManualClock(long initialMillis) {
        if (initialMillis < 0L) {
            throw new IllegalArgumentException("initialMillis must be non-negative");
        }
        now = new AtomicLong(initialMillis);
    }

    @Override
    public long currentTimeMillis() {
        return now.get();
    }

    public long advanceMillis(long millis) {
        if (millis < 0L) {
            throw new IllegalArgumentException("millis must be non-negative");
        }
        return now.addAndGet(millis);
    }
}
