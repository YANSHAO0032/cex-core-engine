package com.cex.core.account;

/**
 * 双边成交的预计算多资产余额变更。
 *
 * <p>能力：将买方报价支出、买方价格改善释放及双方资产交割合并为一次提交。</p>
 * <p>线程安全：对象不可变；提交时调用方必须持续持有买卖双方用户条带锁。</p>
 * <p>限制：不携带用户标识或锁状态，不能在另一笔准备结果之间交错提交。</p>
 */
public final class TradeLedgerMutation {
    private final AssetBalance buyerQuote;
    private final AssetBalance buyerBase;
    private final AssetBalance sellerBase;
    private final AssetBalance sellerQuote;
    private final long buyerQuoteFrozenAfter;
    private final long buyerQuoteAvailableAfter;
    private final long buyerBaseAvailableAfter;
    private final long sellerBaseFrozenAfter;
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

    AssetBalance buyerQuote() { return buyerQuote; }
    AssetBalance buyerBase() { return buyerBase; }
    AssetBalance sellerBase() { return sellerBase; }
    AssetBalance sellerQuote() { return sellerQuote; }
    long buyerQuoteFrozenAfter() { return buyerQuoteFrozenAfter; }
    long buyerQuoteAvailableAfter() { return buyerQuoteAvailableAfter; }
    long buyerBaseAvailableAfter() { return buyerBaseAvailableAfter; }
    long sellerBaseFrozenAfter() { return sellerBaseFrozenAfter; }
    long sellerQuoteAvailableAfter() { return sellerQuoteAvailableAfter; }
}
