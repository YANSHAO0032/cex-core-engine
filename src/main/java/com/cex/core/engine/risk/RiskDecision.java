package com.cex.core.engine.risk;

/**
 * 单笔成交风控评估结果。
 *
 * <p>结果不可变，可由事件分发线程读取；是否真正修改订单由独立的 RISK_HOLD 事件完成。</p>
 */
public final class RiskDecision {

    /** 风控统计所属用户标识。 */
    private final long userId;
    /** 触发本次风控评估的订单标识。 */
    private final long orderId;
    /** 成交加入并清理过期数据后的十秒窗口累计金额。 */
    private final long windowAmount;
    /** 用户成交金额风控阈值，资金单位为资产最小单位。 */
    private final long thresholdAmount;
    /** 本次评估得到的风控状态。 */
    private final RiskState state;
    /** 是否首次接收该 tradeId，false 表示重复成交未计入窗口。 */
    private final boolean newTransaction;

    /**
     * 创建不可变风控结果。
     *
     * @param userId 用户标识
     * @param orderId 订单标识
     * @param windowAmount 十秒窗口累计成交金额
     * @param thresholdAmount 风控阈值
     * @param state 风控状态
     * @param newTransaction 是否为首次接收的成交
     */
    RiskDecision(long userId,
                 long orderId,
                 long windowAmount,
                 long thresholdAmount,
                 RiskState state,
                 boolean newTransaction) {
        this.userId = userId;
        this.orderId = orderId;
        this.windowAmount = windowAmount;
        this.thresholdAmount = thresholdAmount;
        this.state = state;
        this.newTransaction = newTransaction;
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
     * 获取订单标识。
     *
     * @return 订单标识
     */
    public long getOrderId() {
        return orderId;
    }

    /**
     * 获取十秒成交金额。
     *
     * @return 窗口内累计成交金额，使用资金最小单位
     */
    public long getWindowAmount() {
        return windowAmount;
    }

    /**
     * 获取风控阈值。
     *
     * @return 风控阈值，使用资金最小单位
     */
    public long getThresholdAmount() {
        return thresholdAmount;
    }

    /**
     * 获取风控状态。
     *
     * @return NORMAL 或 RISK_HOLD
     */
    public RiskState getState() {
        return state;
    }

    /**
     * 判断本次评估是否需要风控冻结。
     *
     * @return 状态为 RISK_HOLD 时返回 true
     */
    public boolean isRiskHold() {
        return state == RiskState.RISK_HOLD;
    }

    /**
     * 判断本次成交是否首次进入窗口。
     *
     * @return tradeId 首次出现时返回 true，重复成交返回 false
     */
    public boolean isNewTransaction() {
        return newTransaction;
    }
}
