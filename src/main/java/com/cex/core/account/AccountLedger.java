package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.util.MoneyMath;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 管理用户账户及系统已结算资金的内存账本。
 *
 * <p>能力：创建账户、在外部持锁条件下冻结、解冻与结算资金，并校验资产总额守恒。</p>
 * <p>线程安全：账户变更由用户条带锁串行化；账户创建额外持有账本监视器；已结算金额使用 CAS 更新。</p>
 * <p>限制：锁定操作不会自行加锁，调用方必须先持有对应用户的条带锁。</p>
 */
public final class AccountLedger {

    /** 为用户操作提供条带化互斥的锁管理器。 */
    private final StripedLockManager lockManager;
    /** 按用户标识索引的账户集合。 */
    private final Map<Long, Account> accounts = new ConcurrentHashMap<>();
    /** 系统已结算资产总额，单位为货币最小单位，与全部账户余额共同满足总资产守恒。 */
    private final AtomicLong systemSettledAmount;
    /** 账本初始总资产，单位为货币最小单位，等于全部可用余额、冻结余额及系统已结算金额之和。 */
    private long initialTotalAsset;

    /**
     * 使用零初始已结算金额创建账本。
     *
     * @param lockManager 用户条带锁管理器
     */
    public AccountLedger(StripedLockManager lockManager) {
        this(lockManager, 0L);
    }

    /**
     * 使用指定初始已结算金额创建账本。
     *
     * @param lockManager 用户条带锁管理器
     * @param initialSystemSettledAmount 初始系统已结算资产，必须非负
     * @throws IllegalArgumentException 当初始已结算金额为负数时抛出
     */
    public AccountLedger(StripedLockManager lockManager, long initialSystemSettledAmount) {
        this.lockManager = lockManager;
        long seededSystemSettledAmount = MoneyMath.requireNonNegative(initialSystemSettledAmount);
        this.systemSettledAmount = new AtomicLong(seededSystemSettledAmount);
        this.initialTotalAsset = seededSystemSettledAmount;
    }

    /**
     * 获取账本使用的条带锁管理器。
     *
     * @return 条带锁管理器
     */
    public StripedLockManager lockManager() {
        return lockManager;
    }

    /**
     * 使用零冻结余额创建用户账户。
     *
     * @param userId 用户标识，必须为正数
     * @param available 初始可用资产，必须非负
     * @return 新创建的账户
     * @throws IllegalArgumentException 当用户标识或余额无效，或账户已存在时抛出
     */
    public Account createAccount(long userId, long available) {
        return createAccount(userId, available, 0L);
    }

    /**
     * 创建同时包含可用与冻结余额的用户账户。
     *
     * @param userId 用户标识，必须为正数
     * @param available 初始可用资产，必须非负
     * @param frozen 初始冻结资产，必须非负
     * @return 新创建的账户
     * @throws IllegalArgumentException 当参数无效或账户已存在时抛出
     * @throws ArithmeticException 当初始资产总额溢出时抛出
     * @note 先获取用户条带锁再获取账本监视器，确保同一用户创建幂等地拒绝重复发布。
     */
    public Account createAccount(long userId, long available, long frozen) {
        requirePositiveId(userId, "userId");
        java.util.concurrent.locks.ReentrantLock userLock = lockManager.lockForUser(userId);
        // 先锁定用户，避免同一用户并发创建时重复发布账户。
        userLock.lock();
        try {
            synchronized (this) {
                return createAccountUnderLocks(userId, available, frozen);
            }
        } finally {
            userLock.unlock();
        }
    }

    /**
     * 在用户条带锁和账本监视器均已持有时创建账户并更新初始资产。
     *
     * @param userId 用户标识
     * @param available 初始可用资产
     * @param frozen 初始冻结资产
     * @return 已发布的账户
     * @throws IllegalArgumentException 当余额无效或账户已存在时抛出
     * @throws ArithmeticException 当资产总额计算溢出时抛出
     * @note 先完成资产总额的溢出校验，再发布账户，避免失败时破坏资产守恒或留下半成品状态。
     */
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

    /**
     * 获取指定用户的既有账户。
     *
     * @param userId 用户标识
     * @return 用户账户
     * @throws IllegalArgumentException 当账户不存在时抛出
     */
    public Account getRequiredAccount(long userId) {
        Account account = accounts.get(userId);
        if (account == null) {
            throw new IllegalArgumentException("account not found for userId=" + userId);
        }
        return account;
    }

    /**
     * 获取系统已结算资产总额。
     *
     * @return 已结算资产数量
     */
    public long systemSettledAmount() {
        return systemSettledAmount.get();
    }

    /**
     * 获取账本应保持守恒的初始资产总额。
     *
     * @return 初始资产总额
     */
    public synchronized long initialTotalAsset() {
        return initialTotalAsset;
    }

    /**
     * 汇总账户可用、冻结及系统已结算资产。
     *
     * @return 当前资产总额
     * @throws ArithmeticException 当汇总结果溢出时抛出
     */
    public long currentTotalAsset() {
        long total = 0L;
        for (Account account : accounts.values()) {
            total = MoneyMath.checkedAdd(total, account.available());
            total = MoneyMath.checkedAdd(total, account.frozen());
        }
        return MoneyMath.checkedAdd(total, systemSettledAmount());
    }

    /**
     * 判断当前资产总额是否等于应守恒的初始总额。
     *
     * @return 总额守恒时为 {@code true}
     */
    public boolean invariantHolds() {
        return currentTotalAsset() == initialTotalAsset();
    }

    /**
     * 返回当前账户引用的不可修改快照列表。
     *
     * @return 账户快照
     */
    public Collection<Account> accountsSnapshot() {
        return java.util.List.copyOf(accounts.values());
    }

    /**
     * 将用户可用资产转入冻结资产。
     *
     * @param userId 用户标识
     * @param amount 要冻结的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或可用余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 调用前必须持有用户条带锁；同额资金仅在可用与冻结余额间迁移，保持资产守恒。
     */
    public void freezeLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        // 余额校验必须在同一用户锁内完成，避免并发超额冻结。
        if (account.available() < normalizedAmount) {
            throw new IllegalArgumentException("available balance is insufficient");
        }
        account.setAvailable(MoneyMath.checkedSubtract(account.available(), normalizedAmount));
        account.setFrozen(MoneyMath.checkedAdd(account.frozen(), normalizedAmount));
    }

    /**
     * 将用户冻结资产转回可用资产。
     *
     * @param userId 用户标识
     * @param amount 要解冻的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或冻结余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 调用前必须持有用户条带锁；操作只改变资金归属，不改变总资产。
     */
    public void unfreezeLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        // 余额校验必须在同一用户锁内完成，避免并发超额解冻。
        if (account.frozen() < normalizedAmount) {
            throw new IllegalArgumentException("frozen balance is insufficient");
        }
        account.setFrozen(MoneyMath.checkedSubtract(account.frozen(), normalizedAmount));
        account.setAvailable(MoneyMath.checkedAdd(account.available(), normalizedAmount));
    }

    /**
     * 将用户冻结资产结算至系统已结算金额。
     *
     * @param userId 用户标识
     * @param amount 要结算的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或冻结余额不足时抛出
     * @throws ArithmeticException 当已结算金额累计溢出时抛出
     * @note 调用前必须持有用户条带锁；CAS 成功后再扣减冻结余额，确保总资产守恒。
     */
    public void settleLocked(long userId, long amount) {
        long normalizedAmount = MoneyMath.requirePositive(amount);
        Account account = getRequiredAccount(userId);
        // 在用户锁内确认冻结余额足额，防止重复结算同一笔资金。
        if (account.frozen() < normalizedAmount) {
            throw new IllegalArgumentException("frozen balance is insufficient");
        }
        reserveSystemSettledAmount(normalizedAmount);
        account.setFrozen(MoneyMath.checkedSubtract(account.frozen(), normalizedAmount));
    }

    /**
     * 通过 CAS 原子累加系统已结算金额。
     *
     * @param amount 要累加的结算资产数量
     * @throws ArithmeticException 当累计结果溢出时抛出
     * @note 使用 AtomicLong/CAS 无全局锁累加；CAS 失败仅表示并发更新，本次调用基于最新值重试并只成功提交一次。
     */
    private void reserveSystemSettledAmount(long amount) {
        while (true) {
            long current = systemSettledAmount.get();
            long updated = MoneyMath.checkedAdd(current, amount);
            // 仅在观察值未变化时提交，失败后基于最新值重试。
            if (systemSettledAmount.compareAndSet(current, updated)) {
                return;
            }
        }
    }

    /**
     * 校验标识值为正数。
     *
     * @param value 待校验的标识值
     * @param fieldName 用于异常信息的字段名称
     * @throws IllegalArgumentException 当标识值不是正数时抛出
     */
    private static void requirePositiveId(long value, String fieldName) {
        if (value <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }
}
