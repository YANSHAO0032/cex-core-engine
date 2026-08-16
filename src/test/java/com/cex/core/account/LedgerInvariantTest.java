package com.cex.core.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.concurrent.StripedLockManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

class LedgerInvariantTest {

    @Test
    void freezeUnfreezeAndSettlePreserveInvariant() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 1_000L);

        withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 300L));
        withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 100L));
        withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 200L));

        Account account = ledger.getRequiredAccount(1L);
        assertEquals(800L, account.available());
        assertEquals(0L, account.frozen());
        assertEquals(200L, ledger.systemSettledAmount());
        assertEquals(1_000L, ledger.currentTotalAsset());
        assertTrue(ledger.invariantHolds());
    }

    @Test
    void freezeRejectsInsufficientAvailable() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 101L)));

        assertEquals("available balance is insufficient", error.getMessage());
    }

    @Test
    void unfreezeRejectsInsufficientFrozen() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 1L)));

        assertEquals("frozen balance is insufficient", error.getMessage());
    }

    @Test
    void settleRejectsInsufficientFrozen() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 1L)));

        assertEquals("frozen balance is insufficient", error.getMessage());
    }

    @Test
    void lockedOperationsRejectZeroAndNegativeAmounts() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, -1L)));
        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 0L)));
    }

    @Test
    void checkedArithmeticRejectsOverflow() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager, Long.MAX_VALUE - 1L);

        assertThrows(ArithmeticException.class, () -> ledger.createAccount(1L, 0L, 2L));
    }

    @Test
    void createAccountOverflowLeavesNoPublishedAccountOrTotalMutation() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager, Long.MAX_VALUE - 2L);

        assertThrows(ArithmeticException.class, () -> ledger.createAccount(7L, 0L, 3L));
        assertEquals(Long.MAX_VALUE - 2L, ledger.initialTotalAsset());
        assertThrows(IllegalArgumentException.class, () -> ledger.getRequiredAccount(7L));

        ledger.createAccount(7L, 0L, 2L);

        Account account = ledger.getRequiredAccount(7L);
        assertEquals(0L, account.available());
        assertEquals(2L, account.frozen());
        assertEquals(Long.MAX_VALUE, ledger.initialTotalAsset());
    }

    @Test
    void concurrentOperationsOnDifferentUsersPreserveInvariant() throws Exception {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 10_000L);
        ledger.createAccount(2L, 10_000L);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> tasks = List.of(
                    userWorkflow(lockManager, ledger, 1L, 1_000L, 400L),
                    userWorkflow(lockManager, ledger, 2L, 1_500L, 800L));

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(executor.submit(task));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(20_000L, ledger.currentTotalAsset());
        assertTrue(ledger.invariantHolds());
    }

    @Test
    void concurrentOperationsOnSameUserRemainSerializable() throws Exception {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 1_000L);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int index = 0; index < 50; index++) {
                tasks.add(() -> {
                    withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 1L));
                    withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 1L));
                    return null;
                });
            }

            for (Future<Void> future : executor.invokeAll(tasks)) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        Account account = ledger.getRequiredAccount(1L);
        assertEquals(1_000L, account.available());
        assertEquals(0L, account.frozen());
        assertTrue(ledger.invariantHolds());
    }

    private static Callable<Void> userWorkflow(
            StripedLockManager lockManager,
            AccountLedger ledger,
            long userId,
            long freezeAmount,
            long settleAmount) {
        return () -> {
            withUserLock(lockManager, userId, () -> ledger.freezeLocked(userId, freezeAmount));
            withUserLock(lockManager, userId, () -> ledger.settleLocked(userId, settleAmount));
            withUserLock(lockManager, userId, () -> ledger.unfreezeLocked(userId, freezeAmount - settleAmount));
            return null;
        };
    }

    private static void withUserLock(StripedLockManager lockManager, long userId, ThrowingRunnable action) {
        ReentrantLock lock = lockManager.lockForUser(userId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
