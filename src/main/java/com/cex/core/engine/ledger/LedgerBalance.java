package com.cex.core.engine.ledger;

/**
 * 用户账本余额的一致性快照。
 *
 * <p>余额使用最小资金单位表示；available、frozen、traded 三项应满足资产守恒等式。
 * 快照不可变，适合并发读取。</p>
 */
public final class LedgerBalance {

    /** 余额所属用户标识。 */
    private final long userId;
    /** 可用余额，资金单位为资产最小单位。 */
    private final long available;
    /** 冻结余额，资金单位为资产最小单位，属于守恒总量。 */
    private final long frozen;
    /** 有符号成交结算偏移，借记增加、贷记减少，参与资产守恒。 */
    private final long traded;
    /** 开户时确定的资产守恒常量。 */
    private final long conservationConstant;

    /**
     * 创建账本余额快照。
     *
     * @param userId 用户标识
     * @param available 可用余额，使用资产最小单位
     * @param frozen 冻结余额，使用资产最小单位
     * @param traded 有符号成交结算偏移，使用资产最小单位
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
     * 获取有符号成交结算偏移。
     *
     * @return 借记增加、贷记减少的成交结算偏移
     */
    public long getTraded() {
        return traded;
    }

    /**
     * 获取资产守恒常量。
     *
     * @return 开户时确定的守恒常量
     */
    public long getConservationConstant() {
        return conservationConstant;
    }

    /**
     * 校验当前余额是否满足资产守恒规则。
     *
     * @return available + frozen + traded 等于守恒常量时返回 true
     * @note 快照不可变，因此本次校验不受并发余额变更影响。
     */
    public boolean isConserved() {
        return Math.addExact(Math.addExact(available, frozen), traded)
                == conservationConstant;
    }
}
