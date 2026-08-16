package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

public final class InvariantChecker {
    private final AccountLedger ledger;
    private final StripedLockManager locks;
    private final LongAdder snapshots = new LongAdder();
    private final LongAdder failures = new LongAdder();

    public InvariantChecker(AccountLedger ledger) {
        this.ledger = java.util.Objects.requireNonNull(ledger, "ledger");
        this.locks = ledger.lockManager();
    }

    public boolean check() {
        int acquired = 0;
        try {
            for (; acquired < locks.stripeCount(); acquired++) {
                locks.lockForStripe(acquired).lock();
            }
            snapshots.increment();
            boolean valid = ledger.currentTotalAsset() == ledger.initialTotalAsset();
            if (!valid) {
                failures.increment();
            }
            return valid;
        } finally {
            for (int index = acquired - 1; index >= 0; index--) {
                ReentrantLock lock = locks.lockForStripe(index);
                lock.unlock();
            }
        }
    }

    public long snapshotCount() { return snapshots.sum(); }
    public long failureCount() { return failures.sum(); }
}
