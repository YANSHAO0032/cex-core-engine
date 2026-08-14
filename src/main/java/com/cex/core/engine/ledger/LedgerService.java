package com.cex.core.engine.ledger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存资产账本
 *
 * <p>每次操作在锁定期间更新所有余额组件用户的Stripe。
 * 锁采用非公平策略以最小化上下文切换
 * 热路径上的排队开销。</p>
 */
public final class LedgerService {

    /** 默认账本锁分片数量，必须为 2 的幂以便快速按位路由。 */
    private static final int DEFAULT_STRIPE_COUNT = 1 << 10;

    /** 按用户标识保存内存账本账户，账户对象只在对应分片锁内修改。 */
    private final ConcurrentHashMap<Long, LedgerAccount> accounts = new ConcurrentHashMap<>();
    /** 已成功结算的成交幂等索引，避免同一 tradeId 重复转账。 */
    private final ConcurrentHashMap<Long, SettledTrade> settledTrades =
            new ConcurrentHashMap<>();
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
     * 将冻结资金借记到本账户的成交待转余额。
     *
     * @param userId 用户资产账户标识
     * @param amount 成交借记金额，使用资产最小资金单位且必须为正数
     * @return 冻结余额充足并完成借记时返回 true，否则返回 false
     * @note 在同一临界区执行 frozen 减少与 traded 增加，保证余额非负且守恒。
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
     * 将本账户已有的成交待转余额转回可用余额。
     *
     * @param userId 用户资产账户标识
     * @param amount 成交贷记金额，使用资产最小资金单位且必须为正数
     * @return 成交贷记完成时返回 true
     * @note 只有 traded 足够时才允许贷记，禁止无对应借记的资金增加。
     */
    public boolean tradeCredit(long userId, long amount) {
        requirePositive(amount);
        LedgerAccount account = account(userId);
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            // 贷记只能消费本账户已有的成交借记，禁止凭空制造可用余额。
            if (account.traded < amount) {
                return false;
            }
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
     * 将买方已冻结资金原子转移到卖方可用余额。
     *
     * <p>该操作是成交结算的真实资金路径：买方减少冻结余额，卖方增加可用余额，
     * 不使用有符号偏移掩盖资金流转。两个账户按锁分片序号排序加锁，避免交叉成交死锁。</p>
     *
     * @param tradeId 成交幂等标识
     * @param buyerUserId 买方账户
     * @param sellerUserId 卖方账户
     * @param amount 成交金额，必须为正数
     * @return 资金充足且首次结算成功，或相同 tradeId 已成功结算时返回 true；资金不足返回 false
     * @throws IllegalArgumentException 参数非法、买卖双方相同或 tradeId 复用方式冲突时抛出
     */
    public boolean settleTrade(long tradeId,
                               long buyerUserId,
                               long sellerUserId,
                               long amount) {
        requirePositive(tradeId);
        requirePositive(amount);
        if (buyerUserId == sellerUserId) {
            throw new IllegalArgumentException("buyer and seller must be different accounts");
        }

        LedgerAccount buyer = account(buyerUserId);
        LedgerAccount seller = account(sellerUserId);
        ReentrantLock buyerLock = lockFor(buyerUserId);
        ReentrantLock sellerLock = lockFor(sellerUserId);
        ReentrantLock first = buyerLock;
        ReentrantLock second = sellerLock;
        if (stripeIndex(sellerUserId) < stripeIndex(buyerUserId)) {
            first = sellerLock;
            second = buyerLock;
        }

        first.lock();
        if (second != first) {
            second.lock();
        }
        try {
            SettledTrade existing = settledTrades.get(tradeId);
            if (existing != null) {
                if (!existing.matches(buyerUserId, sellerUserId, amount)) {
                    throw new IllegalArgumentException("tradeId already used for another settlement: "
                            + tradeId);
                }
                return true;
            }
            if (buyer.frozen < amount) {
                return false;
            }

            long newBuyerFrozen = Math.subtractExact(buyer.frozen, amount);
            long newSellerAvailable = Math.addExact(seller.available, amount);
            long newBuyerConstant = Math.subtractExact(buyer.conservationConstant, amount);
            long newSellerConstant = Math.addExact(seller.conservationConstant, amount);

            buyer.frozen = newBuyerFrozen;
            buyer.conservationConstant = newBuyerConstant;
            seller.available = newSellerAvailable;
            seller.conservationConstant = newSellerConstant;
            settledTrades.put(tradeId,
                    new SettledTrade(buyerUserId, sellerUserId, amount));
            return true;
        } finally {
            if (second != first) {
                second.unlock();
            }
            first.unlock();
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
     * 获取已成功结算的成交数量，用于监控和混沌测试确认真实结算路径被执行。
     *
     * @return 已写入幂等索引的成交数量
     */
    public long settledTradeCount() {
        return settledTrades.size();
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
        return stripes[stripeIndex(userId)];
    }

    /** 计算用户对应的锁分片序号。 */
    private int stripeIndex(long userId) {
        int hash = Long.hashCode(userId);
        hash ^= hash >>> 16;
        return hash & stripeMask;
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
        /** 当前账户资产守恒基线，资金单位为资产最小单位。 */
        private long conservationConstant;
        /** 可用余额，资金单位为资产最小单位。 */
        private long available;
        /** 冻结余额，资金单位为资产最小单位。 */
        private long frozen;
        /** 非负成交待转余额，参与 available + frozen + traded 守恒。 */
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

    /** 已成功结算成交的不可变幂等记录。 */
    private static final class SettledTrade {

        private final long buyerUserId;
        private final long sellerUserId;
        private final long amount;

        private SettledTrade(long buyerUserId, long sellerUserId, long amount) {
            this.buyerUserId = buyerUserId;
            this.sellerUserId = sellerUserId;
            this.amount = amount;
        }

        private boolean matches(long buyerUserId, long sellerUserId, long amount) {
            return this.buyerUserId == buyerUserId
                    && this.sellerUserId == sellerUserId
                    && this.amount == amount;
        }
    }
}
