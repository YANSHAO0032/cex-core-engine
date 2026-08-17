package com.cex.core.risk;

import com.cex.core.order.AssetId;
import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 单次风险评估所需的不可变订单与窗口快照。
 * 核心能力：向规则提供报价资产、上游风险名义金额和同资产近期已结算金额；线程安全：全部字段不可变，天然线程安全；使用限制：不同报价资产的窗口值不得混用。
 */
public final class RiskContext {
    /** 当前评估订单的唯一标识。 */
    private final long orderId;
    /** 当前评估订单所属用户标识。 */
    private final long userId;
    /** 当前评估订单的报价资产。 */
    private final AssetId quoteAsset;
    /** 上游提供的报价资产风险名义金额，单位为报价资产最小单位。 */
    private final long riskQuoteAmount;
    /** 创建该快照时的当前毫秒时间。 */
    private final long nowMillis;
    /** 风控窗口内已结算金额总额，单位为货币最小单位。 */
    private final long recentSettledAmount;

    /**
     * 创建风险评估上下文。
     *
     * @param orderId 订单标识
     * @param userId 用户标识
     * @param quoteAsset 当前订单的报价资产
     * @param riskQuoteAmount 上游提供的报价资产风险名义金额
     * @param nowMillis 风险评估时的毫秒时间
     * @param recentSettledAmount 窗口内已结算金额
     * @throws NullPointerException 当报价资产为 {@code null} 时抛出
     * @throws IllegalArgumentException 当标识、金额或时间不符合边界时抛出
     */
    public RiskContext(
            long orderId,
            long userId,
            AssetId quoteAsset,
            long riskQuoteAmount,
            long nowMillis,
            long recentSettledAmount) {
        this.orderId = MoneyMath.requirePositive(orderId);
        this.userId = MoneyMath.requirePositive(userId);
        this.quoteAsset = Objects.requireNonNull(quoteAsset, "quoteAsset");
        this.riskQuoteAmount = MoneyMath.requirePositive(riskQuoteAmount);
        this.nowMillis = MoneyMath.requireNonNegative(nowMillis);
        this.recentSettledAmount = MoneyMath.requireNonNegative(recentSettledAmount);
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
     * 获取当前订单的报价资产。
     *
     * @return 当前订单报价资产
     */
    public AssetId quoteAsset() { return quoteAsset; }
    /**
     * 获取上游提供的报价资产风险名义金额。
     *
     * @return 当前订单风险名义金额，单位为报价资产最小单位
     */
    public long riskQuoteAmount() { return riskQuoteAmount; }
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
