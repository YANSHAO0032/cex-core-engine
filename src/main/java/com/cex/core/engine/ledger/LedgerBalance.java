package com.cex.core.engine.ledger;

/**
 * 用户账本余额的一致性快照。
 *
 * <p>余额使用最小资金单位表示；available、frozen、traded 三项均不得为负，且应满足资产守恒等式。
 * 快照不可变，适合并发读取。</p>
 */
public final class LedgerBalance {

    /** 余额所属用户标识。 */
    private final long userId;
    /** 可用余额，资金单位为资产最小单位。 */
    private final long available;
    /** 冻结余额，资金单位为资产最小单位，属于守恒总量。 */
    private final long frozen;
    /** 已完成但尚未转移出本账本的成交借记余额。 */
    private final long traded;
    /** 当前账户所有权对应的资产守恒基线，跨账户结算时随转入转出调整。 */
    private final long conservationConstant;

    /**
     * 创建账本余额快照。
     *
     * @param userId 用户标识
     * @param available 可用余额，使用资产最小单位
     * @param frozen 冻结余额，使用资产最小单位
     * @param traded 已完成但尚未转移出本账本的成交借记余额
     * @param conservationConstant 资产守恒常量
     */
    LedgerBalance(long userId,
                  long available,
                  long frozen,
                  long traded,
                  long conservationConstant) {
        this.userId = userId;
        this.available = available;
        this.frozen = frozen;
        this.traded = traded;
        this.conservationConstant = conservationConstant;
    }

    /**
     * 获取用户标识。
     *
     * @return 用户标识
     */
    public long getUserId() {
        return userId;
    }

    /**
     * 获取可用余额。
     *
     * @return 资产最小单位表示的可用余额
     */
    public long getAvailable() {
        return available;
    }

    /**
     * 获取冻结余额。
     *
     * @return 资产最小单位表示的冻结余额
     */
    public long getFrozen() {
        return frozen;
    }

    /**
     * 获取成交借记余额。
     *
     * @return 非负的成交借记余额
     */
    public long getTraded() {
        return traded;
    }

    /**
     * 获取资产守恒常量。
     *
     * @return 当前账户资产守恒基线
     */
    public long getConservationConstant() {
        return conservationConstant;
    }

    /**
     * 校验当前余额是否满足资产守恒规则。
     *
     * @return 三项余额均非负且总和等于守恒常量时返回 true
     * @note 快照不可变，因此本次校验不受并发余额变更影响。
     */
    public boolean isConserved() {
        return available >= 0L && frozen >= 0L && traded >= 0L
                && Math.addExact(Math.addExact(available, frozen), traded)
                == conservationConstant;
    }
}
