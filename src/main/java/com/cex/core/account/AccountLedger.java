package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import com.cex.core.util.MoneyMath;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 管理用户多资产余额及系统已结算资金的内存账本。
 *
 * <p>能力：创建资产余额、在外部持锁条件下冻结、解冻和成交结算，并逐资产校验守恒。</p>
 * <p>线程安全：账户余额由用户条带锁串行化；余额创建额外持有账本监视器；系统结算余额使用 CAS 更新。</p>
 * <p>限制：锁定操作不会自行加锁，调用方必须先持有相关用户的所有条带锁。</p>
 */
public final class AccountLedger {
    /** 旧版单资产接口所使用的内部资产标识。 */
    private static final AssetId LEGACY_ASSET = new AssetId("LEGACY");
    /** 为用户操作提供条带化互斥的锁管理器。 */
    private final StripedLockManager lockManager;
    /** 按用户标识索引的账户集合。 */
    private final Map<Long, Account> accounts = new ConcurrentHashMap<>();
    /** 按资产累计的系统已结算资金。 */
    private final Map<AssetId, AtomicLong> systemSettledByAsset = new ConcurrentHashMap<>();
    /** 各资产应保持守恒的初始总额，仅在账本监视器内更新。 */
    private final Map<AssetId, Long> initialTotalByAsset = new HashMap<>();

    /**
     * 使用零初始已结算金额创建账本。
     *
     * @param lockManager 用户条带锁管理器
     */
    public AccountLedger(StripedLockManager lockManager) { this(lockManager, 0L); }

    /**
     * 使用指定旧版单资产初始已结算金额创建账本。
     *
     * @param lockManager 用户条带锁管理器
     * @param initialSystemSettledAmount 初始系统已结算资产，必须非负
     * @throws IllegalArgumentException 当初始已结算金额为负数时抛出
     */
    public AccountLedger(StripedLockManager lockManager, long initialSystemSettledAmount) {
        this.lockManager = Objects.requireNonNull(lockManager, "lockManager");
        long seeded = MoneyMath.requireNonNegative(initialSystemSettledAmount);
        systemSettledByAsset.put(LEGACY_ASSET, new AtomicLong(seeded));
        initialTotalByAsset.put(LEGACY_ASSET, seeded);
    }

    /**
     * 获取账本使用的条带锁管理器。
     *
     * @return 条带锁管理器
     */
    public StripedLockManager lockManager() { return lockManager; }

    /**
     * 使用零冻结余额创建旧版单资产账户。
     *
     * @param userId 用户标识，必须为正数
     * @param available 初始可用资产，必须非负
     * @return 新创建的账户
     * @throws IllegalArgumentException 当用户标识、余额无效或账户已存在时抛出
     */
    public Account createAccount(long userId, long available) {
        return createAccount(userId, available, 0L);
    }

    /**
     * 创建同时包含可用与冻结旧版资产的账户。
     *
     * @param userId 用户标识，必须为正数
     * @param available 初始可用资产，必须非负
     * @param frozen 初始冻结资产，必须非负
     * @return 新创建的账户
     * @throws IllegalArgumentException 当参数无效或账户已存在时抛出
     * @throws ArithmeticException 当初始资产总额溢出时抛出
     * @note 兼容接口只委托到内部 LEGACY 资产，不与真实交易资产混用。
     */
    public Account createAccount(long userId, long available, long frozen) {
        return createBalance(userId, LEGACY_ASSET, available, frozen, true);
    }

    /**
     * 为用户创建零冻结的指定资产余额。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param available 初始可用数量
     * @return 包含该余额的用户账户
     * @throws IllegalArgumentException 当参数无效或该资产余额已存在时抛出
     * @throws ArithmeticException 当初始资产总额溢出时抛出
     */
    public Account createBalance(long userId, AssetId asset, long available) {
        return createBalance(userId, asset, available, 0L, false);
    }

    /**
     * 为用户创建指定资产的可用与冻结余额。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param available 初始可用数量
     * @param frozen 初始冻结数量
     * @return 包含该余额的用户账户
     * @throws IllegalArgumentException 当参数无效或该资产余额已存在时抛出
     * @throws ArithmeticException 当初始资产总额溢出时抛出
     * @note 先完成逐资产总额精确加法，再发布账户和余额桶，失败时不留下部分状态。
     */
    public Account createBalance(long userId, AssetId asset, long available, long frozen) {
        return createBalance(userId, asset, available, frozen, false);
    }

    private Account createBalance(long userId, AssetId asset, long available, long frozen, boolean requireNewAccount) {
        requirePositiveId(userId, "userId");
        Objects.requireNonNull(asset, "asset");
        long normalizedAvailable = MoneyMath.requireNonNegative(available);
        long normalizedFrozen = MoneyMath.requireNonNegative(frozen);
        ReentrantLock userLock = lockManager.lockForUser(userId);
        userLock.lock();
        try {
            synchronized (this) {
                Account existing = accounts.get(userId);
                if (requireNewAccount && existing != null) {
                    throw new IllegalArgumentException("account already exists for userId=" + userId);
                }
                if (existing != null && existing.hasBalance(asset)) {
                    throw new IllegalArgumentException("balance already exists: userId=" + userId + ", asset=" + asset.value());
                }
                long amount = MoneyMath.checkedAdd(normalizedAvailable, normalizedFrozen);
                long initial = initialTotalByAsset.getOrDefault(asset, 0L);
                long updatedInitial = MoneyMath.checkedAdd(initial, amount);
                Account account = existing == null ? new Account(userId) : existing;
                AssetBalance balance = new AssetBalance(normalizedAvailable, normalizedFrozen);
                account.addBalance(asset, balance);
                if (existing == null) {
                    accounts.put(userId, account);
                }
                initialTotalByAsset.put(asset, updatedInitial);
                systemSettledByAsset.putIfAbsent(asset, new AtomicLong());
                return account;
            }
        } finally {
            userLock.unlock();
        }
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
     * 获取用户指定资产的不可变余额快照。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @return 可用与冻结余额快照
     * @throws IllegalArgumentException 当账户或资产余额不存在时抛出
     * @note 调用方应持有用户条带锁以取得与其他资产操作一致的快照。
     */
    public BalanceSnapshot balance(long userId, AssetId asset) {
        Objects.requireNonNull(asset, "asset");
        AssetBalance balance = getRequiredAccount(userId).requiredBalance(asset);
        return new BalanceSnapshot(balance.available(), balance.frozen());
    }

    /**
     * 获取旧版单资产已结算总额。
     *
     * @return 旧版资产的已结算数量
     */
    public long systemSettledAmount() { return systemSettledAmount(LEGACY_ASSET); }

    /**
     * 获取指定资产的系统已结算总额。
     *
     * @param asset 资产标识
     * @return 已结算数量
     */
    public long systemSettledAmount(AssetId asset) {
        Objects.requireNonNull(asset, "asset");
        AtomicLong amount = systemSettledByAsset.get(asset);
        return amount == null ? 0L : amount.get();
    }

    /**
     * 获取旧版单资产应守恒的初始总额。
     *
     * @return 初始资产总额
     * @note 初始总额本身由账本监视器保护；若要与当前余额组成一致快照，调用方必须持有全部条带锁，或调用 {@link InvariantChecker#check()}。
     */
    public synchronized long initialTotalAsset() { return initialTotalByAsset.getOrDefault(LEGACY_ASSET, 0L); }

    /**
     * 获取各资产应守恒的初始总额快照。
     *
     * @return 资产到初始总额的不可修改映射
     * @note 初始总额本身由账本监视器保护；若要与当前余额组成一致快照，调用方必须持有全部条带锁，或调用 {@link InvariantChecker#check()}。
     */
    public synchronized Map<AssetId, Long> initialTotalAssets() { return Map.copyOf(initialTotalByAsset); }

    /**
     * 汇总旧版单资产的账户与系统已结算数量。
     *
     * @return 当前旧版资产总额
     * @throws ArithmeticException 当汇总结果溢出时抛出
     * @note 要获得一致汇总，调用方必须持有全部条带锁；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public long currentTotalAsset() { return currentTotalAssets().getOrDefault(LEGACY_ASSET, 0L); }

    /**
     * 汇总各资产的账户可用、冻结及系统已结算数量。
     *
     * @return 资产到当前总额的不可修改映射
     * @throws ArithmeticException 当任一资产汇总结果溢出时抛出
     * @note 要获得一致汇总，调用方必须持有全部条带锁；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public Map<AssetId, Long> currentTotalAssets() {
        Map<AssetId, Long> totals = new HashMap<>();
        for (Map.Entry<AssetId, AtomicLong> entry : systemSettledByAsset.entrySet()) {
            totals.put(entry.getKey(), entry.getValue().get());
        }
        for (Account account : accounts.values()) {
            for (Map.Entry<AssetId, AssetBalance> entry : account.balancesSnapshot().entrySet()) {
                AssetBalance balance = entry.getValue();
                long current = totals.getOrDefault(entry.getKey(), 0L);
                current = MoneyMath.checkedAdd(current, balance.available());
                totals.put(entry.getKey(), MoneyMath.checkedAdd(current, balance.frozen()));
            }
        }
        return Map.copyOf(totals);
    }

    /**
     * 判断旧版单资产总额是否守恒。
     *
     * @return 旧版资产总额守恒时为 {@code true}
     * @note 调用方必须持有全部条带锁，否则并发成交的中间赋值可能产生瞬时假失败；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public boolean invariantHolds() { return invariantHolds(LEGACY_ASSET); }

    /**
     * 判断指定资产总额是否守恒。
     *
     * @param asset 资产标识
     * @return 指定资产总额守恒时为 {@code true}
     * @note 调用方必须持有全部条带锁，否则并发成交的中间赋值可能产生瞬时假失败；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public boolean invariantHolds(AssetId asset) {
        Objects.requireNonNull(asset, "asset");
        return currentTotalAssets().getOrDefault(asset, 0L)
                .equals(initialTotalAssets().getOrDefault(asset, 0L));
    }

    /**
     * 判断所有资产总额均守恒且所有余额桶均非负。
     *
     * @return 全部资产不变量成立时为 {@code true}
     * @note 调用方必须持有全部条带锁，否则并发成交的中间赋值可能产生瞬时假失败；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public boolean allAssetInvariantsHold() {
        return allBalancesNonNegative() && currentTotalAssets().equals(initialTotalAssets());
    }

    /**
     * 判断所有账户资产余额桶是否均为非负数。
     *
     * @return 所有可用与冻结余额非负时为 {@code true}
     * @note 调用方必须持有全部条带锁，否则并发成交的中间赋值可能产生瞬时假失败；外部一致性检查应调用 {@link InvariantChecker#check()}。
     */
    public boolean allBalancesNonNegative() {
        for (Account account : accounts.values()) {
            for (AssetBalance balance : account.balancesSnapshot().values()) {
                if (balance.available() < 0L || balance.frozen() < 0L) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 返回当前账户引用的不可修改快照列表。
     *
     * @return 账户快照
     */
    public Collection<Account> accountsSnapshot() { return java.util.List.copyOf(accounts.values()); }

    /**
     * 将用户指定资产的可用资金转入冻结资金。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param amount 要冻结的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户或余额不存在时抛出
     * @throws InsufficientBalanceException 当可用余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 调用前必须持有用户条带锁；同额资金仅在同一资产的可用与冻结余额间迁移。
     */
    public void freezeLocked(long userId, AssetId asset, long amount) {
        long normalized = MoneyMath.requirePositive(amount);
        AssetBalance balance = getRequiredAccount(userId).requiredBalance(Objects.requireNonNull(asset, "asset"));
        if (balance.available() < normalized) {
            throw new InsufficientBalanceException("available balance is insufficient");
        }
        long availableAfter = MoneyMath.checkedSubtract(balance.available(), normalized);
        long frozenAfter = MoneyMath.checkedAdd(balance.frozen(), normalized);
        balance.setAvailable(availableAfter);
        balance.setFrozen(frozenAfter);
    }

    /**
     * 将用户旧版资产的可用资金转入冻结资金。
     *
     * @param userId 用户标识
     * @param amount 要冻结的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或可用余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 兼容接口委托给内部 LEGACY 资产；调用前必须持有用户条带锁。
     */
    public void freezeLocked(long userId, long amount) { freezeLocked(userId, LEGACY_ASSET, amount); }

    /**
     * 为指定资产准备从冻结资金回到可用资金的变更。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param amount 要解冻的正数资产数量
     * @return 已校验的余额变更
     * @throws IllegalArgumentException 当金额无效、账户或余额不存在时抛出
     * @throws InsufficientBalanceException 当冻结余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 调用前必须持有用户条带锁；准备阶段不写入余额以支持取消的失败原子性。
     */
    public BalanceMutation prepareUnfreezeLocked(long userId, AssetId asset, long amount) {
        long normalized = MoneyMath.requirePositive(amount);
        AssetBalance balance = getRequiredAccount(userId).requiredBalance(Objects.requireNonNull(asset, "asset"));
        if (balance.frozen() < normalized) {
            throw new InsufficientBalanceException("frozen balance is insufficient");
        }
        return new BalanceMutation(
                balance,
                MoneyMath.checkedAdd(balance.available(), normalized),
                MoneyMath.checkedSubtract(balance.frozen(), normalized));
    }

    /**
     * 提交已预计算的单余额变更。
     *
     * @param mutation 已校验的余额变更
     * @note 调用前必须持有用户条带锁；本方法仅执行预计算字段赋值，不进行算术或校验。
     */
    public void commitBalanceLocked(BalanceMutation mutation) {
        mutation.balance().setAvailable(mutation.availableAfter());
        mutation.balance().setFrozen(mutation.frozenAfter());
    }

    /**
     * 将用户指定资产的冻结资金转回可用资金。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param amount 要解冻的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户或余额不存在时抛出
     * @throws InsufficientBalanceException 当冻结余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 调用前必须持有用户条带锁；以同一预计算变更完成可用与冻结资金迁移。
     */
    public void unfreezeLocked(long userId, AssetId asset, long amount) {
        commitBalanceLocked(prepareUnfreezeLocked(userId, asset, amount));
    }

    /**
     * 将用户旧版资产的冻结资金转回可用资金。
     *
     * @param userId 用户标识
     * @param amount 要解冻的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或冻结余额不足时抛出
     * @throws ArithmeticException 当余额计算溢出时抛出
     * @note 兼容接口委托给内部 LEGACY 资产；调用前必须持有用户条带锁。
     */
    public void unfreezeLocked(long userId, long amount) { unfreezeLocked(userId, LEGACY_ASSET, amount); }

    /**
     * 准备买卖双方的基础资产与报价资产交割。
     *
     * @param buyerUserId 买方用户标识
     * @param sellerUserId 卖方用户标识
     * @param baseAsset 基础资产标识
     * @param quoteAsset 报价资产标识
     * @param baseQuantity 交付给买方的基础资产数量
     * @param quoteQuantity 支付给卖方的报价资产数量
     * @param buyerQuoteRelease 买方完全成交后释放的未花费报价预留
     * @return 包含全部最终余额的不可变成交变更
     * @throws IllegalArgumentException 当用户、资产或金额无效，或余额桶不存在时抛出
     * @throws InsufficientBalanceException 当买方报价冻结或卖方基础冻结不足时抛出
     * @throws ArithmeticException 当最终余额计算溢出时抛出
     * @note 调用前必须持有买卖双方用户条带锁；报价支出和价格改善释放被合并到同一余额桶变更中。
     */
    public TradeLedgerMutation prepareTradeLocked(
            long buyerUserId,
            long sellerUserId,
            AssetId baseAsset,
            AssetId quoteAsset,
            long baseQuantity,
            long quoteQuantity,
            long buyerQuoteRelease) {
        requirePositiveId(buyerUserId, "buyerUserId");
        requirePositiveId(sellerUserId, "sellerUserId");
        if (buyerUserId == sellerUserId) {
            throw new IllegalArgumentException("buyer and seller must differ");
        }
        Objects.requireNonNull(baseAsset, "baseAsset");
        Objects.requireNonNull(quoteAsset, "quoteAsset");
        if (baseAsset.equals(quoteAsset)) {
            throw new IllegalArgumentException("base and quote assets must differ");
        }
        long normalizedBase = MoneyMath.requirePositive(baseQuantity);
        long normalizedQuote = MoneyMath.requirePositive(quoteQuantity);
        long normalizedRelease = MoneyMath.requireNonNegative(buyerQuoteRelease);

        Account buyer = getRequiredAccount(buyerUserId);
        Account seller = getRequiredAccount(sellerUserId);
        AssetBalance buyerQuote = buyer.requiredBalance(quoteAsset);
        AssetBalance buyerBase = buyer.requiredBalance(baseAsset);
        AssetBalance sellerBase = seller.requiredBalance(baseAsset);
        AssetBalance sellerQuote = seller.requiredBalance(quoteAsset);
        long buyerQuoteDeduction = MoneyMath.checkedAdd(normalizedQuote, normalizedRelease);
        if (buyerQuote.frozen() < buyerQuoteDeduction) {
            throw new InsufficientBalanceException("buyer quote frozen balance is insufficient");
        }
        if (sellerBase.frozen() < normalizedBase) {
            throw new InsufficientBalanceException("seller base frozen balance is insufficient");
        }
        long buyerQuoteFrozenAfter = MoneyMath.checkedSubtract(buyerQuote.frozen(), buyerQuoteDeduction);
        long buyerQuoteAvailableAfter = MoneyMath.checkedAdd(buyerQuote.available(), normalizedRelease);
        long buyerBaseAvailableAfter = MoneyMath.checkedAdd(buyerBase.available(), normalizedBase);
        long sellerBaseFrozenAfter = MoneyMath.checkedSubtract(sellerBase.frozen(), normalizedBase);
        long sellerQuoteAvailableAfter = MoneyMath.checkedAdd(sellerQuote.available(), normalizedQuote);
        return new TradeLedgerMutation(
                buyerQuote,
                buyerBase,
                sellerBase,
                sellerQuote,
                buyerQuoteFrozenAfter,
                buyerQuoteAvailableAfter,
                buyerBaseAvailableAfter,
                sellerBaseFrozenAfter,
                sellerQuoteAvailableAfter);
    }

    /**
     * 提交已预计算的双边成交变更。
     *
     * @param mutation 已校验的成交变更
     * @note 调用前必须持有买卖双方用户条带锁；本方法仅执行预计算赋值，不使用全局结算锁或重新计算金额。
     */
    public void commitTradeLocked(TradeLedgerMutation mutation) {
        mutation.buyerQuote().setFrozen(mutation.buyerQuoteFrozenAfter());
        mutation.buyerQuote().setAvailable(mutation.buyerQuoteAvailableAfter());
        mutation.buyerBase().setAvailable(mutation.buyerBaseAvailableAfter());
        mutation.sellerBase().setFrozen(mutation.sellerBaseFrozenAfter());
        mutation.sellerQuote().setAvailable(mutation.sellerQuoteAvailableAfter());
    }

    /**
     * 将用户指定资产的冻结资金结算至系统资金。
     *
     * @param userId 用户标识
     * @param asset 资产标识
     * @param amount 要结算的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户或余额不存在时抛出
     * @throws InsufficientBalanceException 当冻结余额不足时抛出
     * @throws ArithmeticException 当已结算金额累计溢出时抛出
     * @note 调用前必须持有用户条带锁；CAS 成功后再扣减同一资产冻结余额，保持该资产守恒。
     */
    public void settleLocked(long userId, AssetId asset, long amount) {
        long normalized = MoneyMath.requirePositive(amount);
        AssetId requiredAsset = Objects.requireNonNull(asset, "asset");
        AssetBalance balance = getRequiredAccount(userId).requiredBalance(requiredAsset);
        if (balance.frozen() < normalized) {
            throw new InsufficientBalanceException("frozen balance is insufficient");
        }
        long frozenAfter = MoneyMath.checkedSubtract(balance.frozen(), normalized);
        reserveSystemSettledAmount(requiredAsset, normalized);
        balance.setFrozen(frozenAfter);
    }

    /**
     * 将用户旧版资产的冻结资金结算至系统资金。
     *
     * @param userId 用户标识
     * @param amount 要结算的正数资产数量
     * @throws IllegalArgumentException 当金额无效、账户不存在或冻结余额不足时抛出
     * @throws ArithmeticException 当已结算金额累计溢出时抛出
     * @note 兼容接口委托给内部 LEGACY 资产；调用前必须持有用户条带锁。
     */
    public void settleLocked(long userId, long amount) { settleLocked(userId, LEGACY_ASSET, amount); }

    private void reserveSystemSettledAmount(AssetId asset, long amount) {
        AtomicLong settled = systemSettledByAsset.computeIfAbsent(asset, ignored -> new AtomicLong());
        while (true) {
            long current = settled.get();
            long updated = MoneyMath.checkedAdd(current, amount);
            if (settled.compareAndSet(current, updated)) {
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
