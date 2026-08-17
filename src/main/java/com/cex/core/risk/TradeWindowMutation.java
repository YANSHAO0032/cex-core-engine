package com.cex.core.risk;

/**
 * 成交滑动窗口的一次预计算无异常提交变更。
 *
 * <p>核心能力：携带逻辑过期回收、可选扩容和新增成交后的完整标量结果。</p>
 * <p>线程安全：实例不可变；内部 primitive 数组仅交回创建它的窗口提交。</p>
 * <p>使用限制：调用方须在同一外部用户锁内按准备顺序提交，不得跨窗口复用。</p>
 *
 * @note 对象复用窗口现有 primitive 数组，仅在准备阶段确需扩容时分配替换数组，减少 GC 并适配 256MB 堆限制。
 */
public final class TradeWindowMutation {
    /** 扩容时预先构建的时间戳数组；无需扩容时为 {@code null}。 */
    private final long[] replacementTimestamps;
    /** 扩容时预先构建的成交金额数组；无需扩容时为 {@code null}。 */
    private final long[] replacementAmounts;
    /** 无需扩容时待写入新记录的数组索引。 */
    private final int tailIndex;
    /** 待提交成交的毫秒时间戳。 */
    private final long timestampMillis;
    /** 待提交成交的报价资产金额。 */
    private final long amount;
    /** 提交后的最早有效记录索引。 */
    private final int headAfter;
    /** 提交后的有效记录数量。 */
    private final int sizeAfter;
    /** 提交后的窗口累计金额。 */
    private final long rollingSumAfter;

    /**
     * 创建预计算窗口变更。
     *
     * @param replacementTimestamps 扩容后的时间戳数组；无需扩容时为 {@code null}
     * @param replacementAmounts 扩容后的金额数组；无需扩容时为 {@code null}
     * @param tailIndex 原数组中新记录的写入索引
     * @param timestampMillis 新成交毫秒时间戳
     * @param amount 新成交报价资产金额
     * @param headAfter 提交后的头索引
     * @param sizeAfter 提交后的有效记录数
     * @param rollingSumAfter 提交后的窗口累计金额
     */
    TradeWindowMutation(
            long[] replacementTimestamps,
            long[] replacementAmounts,
            int tailIndex,
            long timestampMillis,
            long amount,
            int headAfter,
            int sizeAfter,
            long rollingSumAfter) {
        this.replacementTimestamps = replacementTimestamps;
        this.replacementAmounts = replacementAmounts;
        this.tailIndex = tailIndex;
        this.timestampMillis = timestampMillis;
        this.amount = amount;
        this.headAfter = headAfter;
        this.sizeAfter = sizeAfter;
        this.rollingSumAfter = rollingSumAfter;
    }

    /** @return 扩容后的时间戳数组；未扩容时为 {@code null} */
    long[] replacementTimestamps() { return replacementTimestamps; }
    /** @return 扩容后的成交金额数组；未扩容时为 {@code null} */
    long[] replacementAmounts() { return replacementAmounts; }
    /** @return 原数组中新成交的写入索引 */
    int tailIndex() { return tailIndex; }
    /** @return 新成交的毫秒时间戳 */
    long timestampMillis() { return timestampMillis; }
    /** @return 新成交的报价资产金额 */
    long amount() { return amount; }
    /** @return 提交后的最早有效记录索引 */
    int headAfter() { return headAfter; }
    /** @return 提交后的有效记录数量 */
    int sizeAfter() { return sizeAfter; }
    /** @return 提交后的窗口累计金额 */
    long rollingSumAfter() { return rollingSumAfter; }
}
