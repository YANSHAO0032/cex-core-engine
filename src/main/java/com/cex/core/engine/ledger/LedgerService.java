package com.cex.core.engine.ledger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory asset ledger.
 *
 * <p>Each operation updates all balance components while holding the lock for
 * the user's stripe. The lock is non-fair to minimize context-switch and
 * queueing overhead on the hot path.</p>
 */
public final class LedgerService {

    /** 默认账本锁分片数量，必须为 2 的幂以便快速按位路由。 */
    private static final int DEFAULT_STRIPE_COUNT = 1 << 10;

    /** 按用户标识保存内存账本账户，账户对象只在对应分片锁内修改。 */
    private final ConcurrentHashMap<Long, LedgerAccount> accounts = new ConcurrentHashMap<>();
    /** 账本锁分片数组，用于避免全局锁并降低每账户锁对象数量。 */
    private final ReentrantLock[] stripes;
    /** 锁分片掩码，用于将用户标识映射到固定分片。 */
    private final int stripeMask;

    /** 使用默认锁分片数量创建内存账本。 */
    public LedgerService() {
        this(DEFAULT_STRIPE_COUNT);
    }

    /**
     * 使用指定锁分片数量创建内存账本。
     *
     * @param stripeCount 锁分片数量，必须为正数且为 2 的幂
     * @throws IllegalArgumentException stripeCount 不满足分片约束时抛出
     */
    public LedgerService(int stripeCount) {
        if (stripeCount < 1 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        this.stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            this.stripes[i] = new ReentrantLock(false);
        }
        this.stripeMask = stripeCount - 1;
    }

    /**
     * 开户并将初始资金全部放入可用余额。
     *
     * @param userId 用户资产账户标识
     * @param initialAvailable 初始可用余额，使用资产最小资金单位
     * @throws IllegalArgumentException 初始余额为负数或账户已存在时抛出
     * @note putIfAbsent 保证并发开户只有一个账户成功；重复开户禁止覆盖原有守恒常量。
     */
    public void openAccount(long userId, long initialAvailable) {
        if (initialAvailable < 0) {
            throw new IllegalArgumentException("initialAvailable must not be negative");
        }
        LedgerAccount account = new LedgerAccount(userId, initialAvailable);
        if (accounts.putIfAbsent(userId, account) != null) {
            throw new IllegalArgumentException("account already exists: " + userId);
        }
    }

    /**
     * 将可用余额转入冻结余额。
     *
     * @param userId 用户资产账户标识
     * @param amount 冻结金额，使用资产最小资金单位且必须为正数
     * @return 可用余额充足并完成冻结时返回 true，否则返回 false
     * @note 使用用户锁分片在同一临界区更新 available 和 frozen，确保资金变更原子且守恒。
     * @note 禁止重复调用同一业务冻结请求，幂等键应由上层订单或账本流水维护。
     */
    public boolean freeze(long userId, long amount) {
        requirePositive(amount);
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            // 资金校验在锁内执行，避免并发冻结导致可用余额被重复扣减。
            if (account.available < amount) {
                return false;
            }
            long newFrozen = Math.addExact(account.frozen, amount);
            account.available -= amount;
            account.frozen = newFrozen;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将冻结余额解冻并转回可用余额。
     *
     * @param userId 用户资产账户标识
     * @param amount 解冻金额，使用资产最小资金单位且必须为正数
     * @return 冻结余额充足并完成解冻时返回 true，否则返回 false
     * @note available 与 frozen 在同一用户分片锁内互转，资产守恒常量不变。
     */
    public boolean unfreeze(long userId, long amount) {
        requirePositive(amount);
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            // 只允许从当前冻结余额释放，避免重复撤单造成可用余额虚增。
            if (account.frozen < amount) {
                return false;
            }
            long newAvailable = Math.addExact(account.available, amount);
            account.frozen -= amount;
            account.available = newAvailable;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将冻结资金借记到成交结算偏移桶。
     *
     * @param userId 用户资产账户标识
     * @param amount 成交借记金额，使用资产最小资金单位且必须为正数
     * @return 冻结余额充足并完成借记时返回 true，否则返回 false
     * @note 在同一临界区执行 frozen 减少与 traded 增加，保证 available + frozen + traded 不变。
     * @note 禁止同一成交流水重复调用，否则应由上层 eventId 幂等机制拦截。
     */
    public boolean tradeDebit(long userId, long amount) {
        requirePositive(amount);
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            // 成交借记只能消耗已冻结资金，避免无授权扣减可用余额。
            if (account.frozen < amount) {
                return false;
            }
            long newTraded = Math.addExact(account.traded, amount);
            account.frozen -= amount;
            account.traded = newTraded;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 增加可用余额并扣减对应的成交结算偏移。
     *
     * @param userId 用户资产账户标识
     * @param amount 成交贷记金额，使用资产最小资金单位且必须为正数
     * @return 成交贷记完成时返回 true
     * @note available 增加与 traded 减少在同一用户分片锁内完成，资产守恒常量保持不变。
     */
    public boolean tradeCredit(long userId, long amount) {
        requirePositive(amount);
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            long newAvailable = Math.addExact(account.available, amount);
            long newTraded = Math.subtractExact(account.traded, amount);
            account.available = newAvailable;
            account.traded = newTraded;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取用户账本的一致性快照。
     *
     * @param userId 用户资产账户标识
     * @return 在用户分片锁内复制出的不可变余额快照
     * @note 快照读取与资金写入使用同一分片锁，避免观察到多字段中间状态。
     */
    public LedgerBalance snapshot(long userId) {
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            return new LedgerBalance(account.userId, account.available, account.frozen,
                    account.traded, account.conservationConstant);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查找已开户的内存账本账户。
     *
     * @param userId 用户资产账户标识
     * @return 内存账本账户
     * @throws IllegalArgumentException 用户尚未开户时抛出
     */
    private LedgerAccount account(long userId) {
        LedgerAccount account = accounts.get(userId);
        if (account == null) {
            throw new IllegalArgumentException("account does not exist: " + userId);
        }
        return account;
    }

    /**
     * 根据用户标识选择锁分片。
     *
     * @param userId 用户资产账户标识
     * @return 用户对应的非公平分片锁
     */
    private ReentrantLock lockFor(long userId) {
        int hash = Long.hashCode(userId);
        hash ^= hash >>> 16;
        return stripes[hash & stripeMask];
    }

    /**
     * 校验资金操作金额为正数。
     *
     * @param amount 待校验的资产最小单位金额
     * @throws IllegalArgumentException amount 不为正数时抛出
     */
    private static void requirePositive(long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }

    /** 受所属用户分片锁保护的可变账本账户。 */
    private static final class LedgerAccount {

        /** 账户所属用户标识。 */
        private final long userId;
        /** 开户时确定的资产守恒常量，资金单位为资产最小单位。 */
        private final long conservationConstant;
        /** 可用余额，资金单位为资产最小单位。 */
        private long available;
        /** 冻结余额，资金单位为资产最小单位。 */
        private long frozen;
        /** 有符号成交结算偏移，参与 available + frozen + traded 守恒。 */
        private long traded;

        /**
         * 创建内存账本账户。
         *
         * @param userId 用户账户标识
         * @param initialAvailable 初始可用余额，资金单位为资产最小单位
         */
        private LedgerAccount(long userId, long initialAvailable) {
            this.userId = userId;
            this.available = initialAvailable;
            this.conservationConstant = initialAvailable;
        }
    }
}
