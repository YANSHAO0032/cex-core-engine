package com.cex.core.order;

import java.util.Objects;

/**
 * 由基础资产和报价资产组成的不可变交易对。
 *
 * <p>核心能力：为订单和成交提供方向明确的资产组合。</p>
 * <p>线程安全：记录组件均不可变，可在线程间安全共享。</p>
 * <p>使用限制：不包含价格、最小变动单位或交易状态。</p>
 *
 * @param baseAsset 被买卖数量所对应的基础资产
 * @param quoteAsset 为基础资产计价和预留资金所用的报价资产
 */
public record TradingPair(AssetId baseAsset, AssetId quoteAsset) {

    /**
     * 创建并校验交易对。
     *
     * @param baseAsset 被买卖数量所对应的基础资产
     * @param quoteAsset 为基础资产计价和预留资金所用的报价资产
     * @throws NullPointerException 当任一资产为 {@code null} 时抛出
     * @throws IllegalArgumentException 当基础资产与报价资产相同时抛出
     */
    public TradingPair {
        Objects.requireNonNull(baseAsset, "baseAsset");
        Objects.requireNonNull(quoteAsset, "quoteAsset");
        if (baseAsset.equals(quoteAsset)) {
            throw new IllegalArgumentException("base and quote assets must differ");
        }
    }
}
