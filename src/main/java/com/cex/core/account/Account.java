package com.cex.core.account;

/**
 * 用户账户的余额载体，维护可用与冻结两类资金。
 *
 * <p>能力：为账本提供账户余额的读取与受控更新。</p>
 * <p>线程安全：账户状态需由所属用户条带锁保护，调用方不得无锁读写并依赖一致快照。</p>
 * <p>限制：仅限账户包内的账本修改余额，不包含币种、流水或持久化信息。</p>
 */
public final class Account {

    /** 账户所属用户的唯一标识。 */
    private final long userId;
    /** 可用余额，单位为货币最小单位，与冻结余额及系统已结算金额共同满足总资产守恒。 */
    private long available;
    /** 冻结余额，单位为货币最小单位，与可用余额及系统已结算金额共同满足总资产守恒。 */
    private long frozen;

    /**
     * 使用账本已校验的初始余额创建账户。
     *
     * @param userId 用户标识
     * @param available 初始可用资产数量
     * @param frozen 初始冻结资产数量
     * @note 仅由账本在完成金额校验和资产守恒计算后调用。
     */
    Account(long userId, long available, long frozen) {
        this.userId = userId;
        this.available = available;
        this.frozen = frozen;
    }

    /**
     * 获取账户所属用户标识。
     *
     * @return 用户标识
     */
    public long userId() {
        return userId;
    }

    /**
     * 获取当前可用余额。
     *
     * @return 可用资产数量
     */
    public long available() {
        return available;
    }

    /**
     * 获取当前冻结余额。
     *
     * @return 冻结资产数量
     */
    public long frozen() {
        return frozen;
    }

    /**
     * 在账本持有用户条带锁时更新可用余额。
     *
     * @param available 更新后的可用资产数量
     * @note 调用方必须持有该用户对应的条带锁，以保证余额变更的串行性。
     */
    void setAvailable(long available) {
        this.available = available;
    }

    /**
     * 在账本持有用户条带锁时更新冻结余额。
     *
     * @param frozen 更新后的冻结资产数量
     * @note 调用方必须持有该用户对应的条带锁，以保证余额变更的串行性。
     */
    void setFrozen(long frozen) {
        this.frozen = frozen;
    }
}
