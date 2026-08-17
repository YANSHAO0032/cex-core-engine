package com.cex.core.risk;

import com.cex.core.order.AssetId;
import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 用户与报价资产组成的不可变风险窗口键。
 *
 * <p>核心能力：隔离同一用户在 USDT、USDC 等不同报价资产上的成交风险累计。</p>
 * <p>线程安全：记录组件不可变，可安全用于并发映射键。</p>
 * <p>使用限制：仅标识风险窗口，不表达账户余额或交易对。</p>
 *
 * @param userId 严格为正的用户标识
 * @param quoteAsset 非空的报价资产标识
 */
public record RiskWindowKey(long userId, AssetId quoteAsset) {

    /**
     * 创建并校验风险窗口键。
     *
     * @param userId 严格为正的用户标识
     * @param quoteAsset 非空的报价资产标识
     * @throws IllegalArgumentException 当用户标识不为正数时抛出
     * @throws NullPointerException 当报价资产为 {@code null} 时抛出
     */
    public RiskWindowKey {
        MoneyMath.requirePositive(userId);
        Objects.requireNonNull(quoteAsset, "quoteAsset");
    }
}
