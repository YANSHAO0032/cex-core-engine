package com.cex.core.account;

import com.cex.core.order.AssetId;
import java.util.HashMap;
import java.util.Map;

/**
 * 用户账户的多资产余额载体。
 *
 * <p>能力：按资产维护可用与冻结资金，并向账本提供受锁余额桶。</p>
 * <p>线程安全：余额映射及其中的余额桶均由所属用户的条带锁保护。</p>
 * <p>限制：仅限账户包内账本在持锁状态下变更，不包含流水或持久化信息。</p>
 */
public final class Account {
    /** 账户所属用户的唯一标识。 */
    private final long userId;
    /** 由资产标识索引的余额桶。 */
    private final Map<AssetId, AssetBalance> balances = new HashMap<>();

    /**
     * 创建尚未配置任何资产余额的账户。
     *
     * @param userId 用户标识
     * @note 仅由账本在持有用户条带锁后创建和发布。
     */
    Account(long userId) { this.userId = userId; }

    /**
     * 获取账户所属用户标识。
     *
     * @return 用户标识
     */
    public long userId() { return userId; }

    /**
     * 返回指定资产的既有余额桶。
     *
     * @param asset 资产标识
     * @return 指定资产的余额桶
     * @throws IllegalArgumentException 当资产余额不存在时抛出
     * @note 调用方必须持有所属用户条带锁，以保证余额桶引用及其值的一致性。
     */
    AssetBalance requiredBalance(AssetId asset) {
        AssetBalance balance = balances.get(asset);
        if (balance == null) {
            throw new IllegalArgumentException("balance not found: userId=" + userId + ", asset=" + asset.value());
        }
        return balance;
    }

    /**
     * 判断账户是否已配置指定资产余额。
     *
     * @param asset 资产标识
     * @return 已配置时为 {@code true}
     * @note 调用方必须持有所属用户条带锁。
     */
    boolean hasBalance(AssetId asset) { return balances.containsKey(asset); }

    /**
     * 新增指定资产余额桶。
     *
     * @param asset 资产标识
     * @param balance 新建余额桶
     * @throws IllegalArgumentException 当资产余额已存在时抛出
     * @note 调用方必须持有所属用户条带锁；账本先完成总额校验后才调用此方法。
     */
    void addBalance(AssetId asset, AssetBalance balance) {
        if (balances.putIfAbsent(asset, balance) != null) {
            throw new IllegalArgumentException("balance already exists: userId=" + userId + ", asset=" + asset.value());
        }
    }

    /**
     * 获取账户所有资产余额桶的不可修改浅快照。
     *
     * @return 资产到余额桶的映射快照
     * @note 调用方必须持有所属用户条带锁，余额桶本身仍为受锁保护的可变对象。
     */
    Map<AssetId, AssetBalance> balancesSnapshot() { return Map.copyOf(balances); }
}
