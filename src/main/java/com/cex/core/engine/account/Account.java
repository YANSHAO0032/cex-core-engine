package com.cex.core.engine.account;

/**
 * 用户资产账户领域模型。
 *
 * <p>对象不可变，仅保存用户标识和可用、冻结余额；资金单位由上层资产配置定义，
 * 余额总量应与账本资产守恒规则一致。</p>
 */
public final class Account {

    /** 用户资产账户标识。 */
    private final long userId;
    /** 可用余额，使用最小资金单位表示，可直接用于下单或转出。 */
    private final long available;
    /** 冻结余额，使用最小资金单位表示，属于资产守恒总量的一部分。 */
    private final long frozen;

    /**
     * 创建不可变用户账户快照。
     *
     * @param userId 用户资产账户标识
     * @param available 可用余额，使用最小资金单位表示
     * @param frozen 冻结余额，使用最小资金单位表示
     */
    public Account(long userId, long available, long frozen) {
        this.userId = userId;
        this.available = available;
        this.frozen = frozen;
    }

    /**
     * 获取用户账户标识。
     *
     * @return 用户账户标识
     */
    public long getUserId() {
        return userId;
    }

    /**
     * 获取可用余额。
     *
     * @return 使用最小资金单位表示的可用余额
     */
    public long getAvailable() {
        return available;
    }

    /**
     * 获取冻结余额。
     *
     * @return 使用最小资金单位表示的冻结余额
     */
    public long getFrozen() {
        return frozen;
    }
}
