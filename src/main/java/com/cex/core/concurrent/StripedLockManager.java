package com.cex.core.concurrent;

import java.util.concurrent.locks.ReentrantLock;

public final class StripedLockManager {

    public static final int DEFAULT_STRIPE_COUNT = 256;

    private final ReentrantLock[] stripes;
    private final int stripeMask;

    public StripedLockManager() {
        this(DEFAULT_STRIPE_COUNT);
    }

    public StripedLockManager(int stripeCount) {
        if (stripeCount <= 0 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        this.stripes = new ReentrantLock[stripeCount];
        for (int index = 0; index < stripeCount; index++) {
            stripes[index] = new ReentrantLock();
        }
        this.stripeMask = stripeCount - 1;
    }

    public int stripeCount() {
        return stripes.length;
    }

    public int stripeIndexForUser(long userId) {
        return Long.hashCode(userId) & stripeMask;
    }

    public ReentrantLock lockForUser(long userId) {
        return stripes[stripeIndexForUser(userId)];
    }

    public ReentrantLock lockForStripe(int stripeIndex) {
        if (stripeIndex < 0 || stripeIndex >= stripes.length) {
            throw new IndexOutOfBoundsException("stripeIndex=" + stripeIndex);
        }
        return stripes[stripeIndex];
    }
}
