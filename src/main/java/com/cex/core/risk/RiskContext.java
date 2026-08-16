package com.cex.core.risk;

/**
 * 单次风险评估所需的不可变订单与窗口快照。
 * 核心能力：向规则提供订单金额和近期已结算金额；线程安全：全部字段不可变，天然线程安全；使用限制：金额语义由订单与账务模块保证。
 */
public final class RiskContext {
    /** 当前评估订单的唯一标识。 */
    private final long orderId;
    /** 当前评估订单所属用户标识。 */
    private final long userId;
    /** 当前评估订单金额，单位为货币最小单位。 */
    private final long amount;
    /** 创建该快照时的当前毫秒时间。 */
    private final long nowMillis;
    /** 风控窗口内已结算金额总额，单位为货币最小单位。 */
    private final long recentSettledAmount;

    /**
     * 创建风险评估上下文。
     *
     * @param orderId 订单标识
     * @param userId 用户标识
     * @param amount 当前订单金额
     * @param nowMillis 风险评估时的毫秒时间
     * @param recentSettledAmount 窗口内已结算金额
     */
    public RiskContext(long orderId, long userId, long amount, long nowMillis, long recentSettledAmount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.nowMillis = nowMillis;
        this.recentSettledAmount = recentSettledAmount;
    }

    /**
     * 获取当前评估订单ID。
     *
     * @return 当前订单ID
     */
    public long orderId() { return orderId; }
    /**
     * 获取当前评估用户ID。
     *
     * @return 当前用户ID
     */
    public long userId() { return userId; }
    /**
     * 获取当前订单金额。
     *
     * @return 当前订单金额，单位为货币最小单位
     */
    public long amount() { return amount; }
    /**
     * 获取风险评估业务时间。
     *
     * @return 风险评估毫秒时间
     */
    public long nowMillis() { return nowMillis; }
    /**
     * 获取滑动窗口内已结算金额总额。
     *
     * @return 窗口内已结算金额总额，单位为货币最小单位
     */
    public long recentSettledAmount() { return recentSettledAmount; }
}
