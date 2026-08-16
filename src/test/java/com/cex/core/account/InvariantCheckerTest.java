package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InvariantCheckerTest {
    @Test
    void snapshotLocksAllStripesAndPreservesAssetInvariant() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager(8));
        ledger.createAccount(1L, 100L);
        ledger.createAccount(2L, 200L);
        InvariantChecker checker = new InvariantChecker(ledger);

        assertTrue(checker.check());
        assertEquals(1L, checker.snapshotCount());
        assertEquals(0L, checker.failureCount());
    }
}
