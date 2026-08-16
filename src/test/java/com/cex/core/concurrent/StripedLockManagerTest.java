package com.cex.core.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class StripedLockManagerTest {

    @Test
    void defaultConstructorUsesCanonicalStripeCount() {
        StripedLockManager lockManager = new StripedLockManager();

        assertEquals(256, lockManager.stripeCount());
    }

    @Test
    void stripeIndexUsesCanonicalUserHash() {
        StripedLockManager lockManager = new StripedLockManager(512);
        long userId = 987_654_321L;

        int expectedIndex = Long.hashCode(userId) & (512 - 1);

        assertEquals(expectedIndex, lockManager.stripeIndexForUser(userId));
        assertSame(lockManager.lockForUser(userId), lockManager.lockForUser(userId));
    }

    @Test
    void stripeCountMustBePowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new StripedLockManager(255));
        assertThrows(IllegalArgumentException.class, () -> new StripedLockManager(0));
    }
}
