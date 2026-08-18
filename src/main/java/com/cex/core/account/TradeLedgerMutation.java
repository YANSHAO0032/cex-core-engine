package com.cex.core.account;

/**
 * 双边成交的预计算多资产余额变更。
 *
 * <p>能力：将买方报价支出、买方价格改善释放及双方资产交割合并为一次提交。</p>
 * <p>线程安全：对象不可变；提交时调用方必须持续持有买卖双方用户条带锁。</p>
 * <p>限制：不携带用户标识或锁状态，不能在另一笔准备结果之间交错提交。</p>
 */
public final class TradeLedgerMutation {
    /** 买方报价资产余额桶，成交扣减与余款释放均在此桶结算。 */
    private final AssetBalance buyerQuote;
    /** 买方基础资产余额桶，接收卖方交付并保持基础资产守恒。 */
    private final AssetBalance buyerBase;
    /** 卖方基础资产余额桶，从冻结余额扣减实际交付数量。 */
    private final AssetBalance sellerBase;
    /** 卖方报价资产余额桶，接收买方支付并保持报价资产守恒。 */
    private final AssetBalance sellerQuote;
    /** 买方报价资产提交后的冻结余额，单位为报价资产最小单位。 */
    private final long buyerQuoteFrozenAfter;
    /** 买方报价资产提交后的可用余额，包含最终价格改善释放额。 */
    private final long buyerQuoteAvailableAfter;
    /** 买方基础资产提交后的可用余额，包含本次成交交付量。 */
    private final long buyerBaseAvailableAfter;
    /** 卖方基础资产提交后的冻结余额，已扣除本次成交交付量。 */
    private final long sellerBaseFrozenAfter;
    /** 卖方报价资产提交后的可用余额，包含本次成交收款。 */
    private final long sellerQuoteAvailableAfter;

    /**
     * 创建已通过全部资金校验的成交变更。
     *
     * @param buyerQuote 买方报价资产余额桶
     * @param buyerBase 买方基础资产余额桶
     * @param sellerBase 卖方基础资产余额桶
     * @param sellerQuote 卖方报价资产余额桶
     * @param buyerQuoteFrozenAfter 买方报价资产提交后的冻结数量
     * @param buyerQuoteAvailableAfter 买方报价资产提交后的可用数量
     * @param buyerBaseAvailableAfter 买方基础资产提交后的可用数量
     * @param sellerBaseFrozenAfter 卖方基础资产提交后的冻结数量
     * @param sellerQuoteAvailableAfter 卖方报价资产提交后的可用数量
     * @note 仅由账本准备阶段创建；提交阶段只能执行这些预计算赋值。
     */
    TradeLedgerMutation(
            AssetBalance buyerQuote,
            AssetBalance buyerBase,
            AssetBalance sellerBase,
            AssetBalance sellerQuote,
            long buyerQuoteFrozenAfter,
            long buyerQuoteAvailableAfter,
            long buyerBaseAvailableAfter,
            long sellerBaseFrozenAfter,
            long sellerQuoteAvailableAfter) {
        this.buyerQuote = buyerQuote;
        this.buyerBase = buyerBase;
        this.sellerBase = sellerBase;
        this.sellerQuote = sellerQuote;
        this.buyerQuoteFrozenAfter = buyerQuoteFrozenAfter;
        this.buyerQuoteAvailableAfter = buyerQuoteAvailableAfter;
        this.buyerBaseAvailableAfter = buyerBaseAvailableAfter;
        this.sellerBaseFrozenAfter = sellerBaseFrozenAfter;
        this.sellerQuoteAvailableAfter = sellerQuoteAvailableAfter;
    }

    /**
     * 获取买方报价资产余额桶。
     *
     * @return 买方报价资产余额桶
     */
    AssetBalance buyerQuote() { return buyerQuote; }

    /**
     * 获取买方基础资产余额桶。
     *
     * @return 买方基础资产余额桶
     */
    AssetBalance buyerBase() { return buyerBase; }

    /**
     * 获取卖方基础资产余额桶。
     *
     * @return 卖方基础资产余额桶
     */
    AssetBalance sellerBase() { return sellerBase; }

    /**
     * 获取卖方报价资产余额桶。
     *
     * @return 卖方报价资产余额桶
     */
    AssetBalance sellerQuote() { return sellerQuote; }

    /**
     * 获取买方报价资产提交后的冻结余额。
     *
     * @return 报价资产最小单位数量
     */
    long buyerQuoteFrozenAfter() { return buyerQuoteFrozenAfter; }

    /**
     * 获取买方报价资产提交后的可用余额。
     *
     * @return 报价资产最小单位数量
     */
    long buyerQuoteAvailableAfter() { return buyerQuoteAvailableAfter; }

    /**
     * 获取买方基础资产提交后的可用余额。
     *
     * @return 基础资产最小单位数量
     */
    long buyerBaseAvailableAfter() { return buyerBaseAvailableAfter; }

    /**
     * 获取卖方基础资产提交后的冻结余额。
     *
     * @return 基础资产最小单位数量
     */
    long sellerBaseFrozenAfter() { return sellerBaseFrozenAfter; }

    /**
     * 获取卖方报价资产提交后的可用余额。
     *
     * @return 报价资产最小单位数量
     */
    long sellerQuoteAvailableAfter() { return sellerQuoteAvailableAfter; }
}
