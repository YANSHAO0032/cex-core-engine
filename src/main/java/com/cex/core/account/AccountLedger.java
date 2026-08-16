package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.util.MoneyMath;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class AccountLedger {

    private final StripedLockManager lockManager;
    private final Map<Long, Account> accounts = new ConcurrentHashMap<>();
    private final AtomicLong systemSettledAmount;
    private long initialTotalAsset;

    public AccountLedger(StripedLockManager lockManager) {
        this(lockManager, 0L);
    }

    public AccountLedger(StripedLockManager lockManager, long initialSystemSettledAmount) {
        this.lockManager = lockManager;
        long seededSystemSettledAmount = MoneyMath.requireNonNegative(initialSystemSettledAmount);
        this.systemSettledAmount = new AtomicLong(seededSystemSettledAmount);
        this.initialTotalAsset = seededSystemSettledAmount;
    }

    public StripedLockManager lockManager() {
        return lockManager;
    }

    public Account createAccount(long userId, long available) {
        return createAccount(userId, available, 0L);
    }

    public Account createAccount(long userId, long available, long frozen) {
        requirePositiveId(userId, "userId");
        java.util.concurrent.locks.ReentrantLock userLock = lockManager.lockForUser(userId);
        userLock.lock();
        try {
            synchronized (this) {
                return createAccountUnderLocks(userId, available, frozen);
            }
        } finally {
            userLock.unlock();
        }
    }

    private Account createAccountUnderLocks(long userId, long available, long frozen) {
        long normalizedAvailable = MoneyMath.requireNonNegative(available);
        long normalizedFrozen = MoneyMath.requireNonNegative(frozen);
        if (accounts.containsKey(userId)) {
            throw new IllegalArgumentException("account already exists for userId=" + userId);
        }
        long updatedInitialTotalAsset = MoneyMath.checkedAdd(
                initialTotalAsset,
                MoneyMath.checkedAdd(normalizedAvailable, normalizedFrozen));
        Account account = new Account(userId, normalizedAvailable, normalizedFrozen);
        Account existing = accounts.putIfAbsent(userId, account);
        if (existing != null) {
            throw new IllegalArgumentException("account already exists for userId=" + userId);
        }
        initialTotalAsset = updatedInitialTotalAsset;
        return account;
    }

    public Account getRequiredAccount(long userId) {
        Account account = accounts.get(userId);
        if (account == null) {
            throw new IllegalArgumentException("account not found for userId=" + userId);
        }
        return account;
    }

    public long systemSettledAmount() {
        return systemSettledAmount.get();
    }

    public synchronized long initialTotalAsset() {
        return initialTotalAsset;
    }

    public long currentTotalAsset() {
        long total = 0L;
        for (Account account : accounts.values()) {
            total = MoneyMath.checkedAdd(total, account.available());
            total = MoneyMath.checkedAdd(total, account.frozen());
        }
        return MoneyMath.checkedAdd(total, systemSettledAmount());
    }

    public boolean invariantHolds() {
        return currentTotalAsset() == initialTotalAsset();
    }

    public Collection<Account> accountsSnapshot() {
        return java.util.List.copyOf(accounts.values());
    }

    public void freezeLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        if (account.available() < normalizedAmount) {
            throw new IllegalArgumentException("available balance is insufficient");
        }
        account.setAvailable(MoneyMath.checkedSubtract(account.available(), normalizedAmount));
        account.setFrozen(MoneyMath.checkedAdd(account.frozen(), normalizedAmount));
    }

    public void unfreezeLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        if (account.frozen() < normalizedAmount) {
            throw new IllegalArgumentException("frozen balance is insufficient");
        }
        account.setFrozen(MoneyMath.checkedSubtract(account.frozen(), normalizedAmount));
        account.setAvailable(MoneyMath.checkedAdd(account.available(), normalizedAmount));
    }

    public void settleLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        if (account.frozen() < normalizedAmount) {
            throw new IllegalArgumentException("frozen balance is insufficient");
        }
        reserveSystemSettledAmount(normalizedAmount);
        account.setFrozen(MoneyMath.checkedSubtract(account.frozen(), normalizedAmount));
    }

    private void reserveSystemSettledAmount(long amount) {
        while (true) {
            long current = systemSettledAmount.get();
            long updated = MoneyMath.checkedAdd(current, amount);
            if (systemSettledAmount.compareAndSet(current, updated)) {
                return;
            }
        }
    }

    private static void requirePositiveId(long value, String fieldName) {
        if (value <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }
}
