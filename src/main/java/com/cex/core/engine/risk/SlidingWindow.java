package com.cex.core.engine.risk;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * 单用户按时间排序的成交金额滑动窗口。
 *
 * <p>默认由 RiskEngine 的用户分片锁保护；窗口使用 ArrayDeque 保存事件，只从队头清理
 * 过期成交，不扫描全部保留数据。</p>
 */
public final class SlidingWindow {

    /** 窗口时长，单位为毫秒，默认由 RiskEngine 设置为 10 秒。 */
    private final long windowMillis;
    /** 按时间到达顺序保存成交记录的内存双端队列。 */
    private final ArrayDeque<Transaction> transactions = new ArrayDeque<>();
    /** 当前窗口内成交 ID 集合，用于重复成交幂等。 */
    private final Set<Long> transactionIds = new HashSet<>();
    /** 当前窗口累计成交金额，资金单位为资产最小单位。 */
    private long totalAmount;

    /**
     * 创建指定时长的成交金额窗口。
     *
     * @param windowMillis 窗口时长，单位为毫秒
     * @throws IllegalArgumentException windowMillis 非正数时抛出
     */
    public SlidingWindow(long windowMillis) {
        if (windowMillis <= 0L) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        this.windowMillis = windowMillis;
    }

    /**
     * 清理过期成交后记录一笔新成交。
     *
     * @param transactionId 成交幂等标识
     * @param amount 成交金额，使用资产最小资金单位且必须为正数
     * @param timestampMillis 成交处理时间，单位为毫秒
     * @return 本次是否新增成交及更新后的窗口金额
     * @note 先清理队头过期记录，再以 HashSet 判断重复 tradeId；重复成交不会再次累计金额。
     * @note 依赖同一用户时间戳基本单调递增，过期逻辑只处理 Deque 头部，复杂度与实际过期数量相关。
     */
    public RecordResult record(long transactionId, long amount, long timestampMillis) {
        if (amount <= 0L) {
            throw new IllegalArgumentException("amount must be positive");
        }
        // 新成交到达时顺便回收过期前缀，避免单独扫描全部交易记录。
        expireOldTransactions(timestampMillis);
        if (!transactionIds.add(transactionId)) {
            return new RecordResult(false, totalAmount);
        }

        totalAmount = Math.addExact(totalAmount, amount);
        transactions.addLast(new Transaction(transactionId, amount, timestampMillis));
        return new RecordResult(true, totalAmount);
    }

    /**
     * 清理窗口外的旧成交记录。
     *
     * @param nowMillis 当前处理时间，单位为毫秒
     * @return 本次移除的过期成交数量
     * @note 10 秒窗口使用半开区间；只从 Deque 队头回收，过期数据回收后同步扣减 totalAmount。
     */
    public int expireOldTransactions(long nowMillis) {
        long cutoff = nowMillis - windowMillis;
        int expired = 0;
        while (!transactions.isEmpty()
                && transactions.peekFirst().timestampMillis <= cutoff) {
            Transaction transaction = transactions.removeFirst();
            transactionIds.remove(transaction.transactionId);
            totalAmount -= transaction.amount;
            expired++;
        }
        return expired;
    }

    /**
     * 获取当前窗口累计成交金额。
     *
     * @return 窗口内累计成交金额，使用资产最小资金单位
     */
    public long getTotalAmount() {
        return totalAmount;
    }

    /**
     * 获取当前窗口成交记录数量。
     *
     * @return Deque 中尚未过期的成交记录数量
     */
    public int size() {
        return transactions.size();
    }

    /**
     * 获取窗口时长。
     *
     * @return 窗口时长，单位为毫秒
     */
    public long getWindowMillis() {
        return windowMillis;
    }

    /** 成交写入窗口后的不可变结果。 */
    public static final class RecordResult {

        /** 是否首次接收该成交。 */
        private final boolean accepted;
        /** 记录完成后的窗口累计成交金额。 */
        private final long totalAmount;

        /**
         * 创建窗口记录结果。
         *
         * @param accepted 是否新增成交
         * @param totalAmount 更新后的窗口成交金额
         */
        private RecordResult(boolean accepted, long totalAmount) {
            this.accepted = accepted;
            this.totalAmount = totalAmount;
        }

        /**
         * 判断成交是否被新增记录。
         *
         * @return 首次成交返回 true，重复成交返回 false
         */
        public boolean isAccepted() {
            return accepted;
        }

        /**
         * 判断成交是否为重复事件。
         *
         * @return 重复成交返回 true
         */
        public boolean isDuplicate() {
            return !accepted;
        }

        /**
         * 获取记录后的窗口累计金额。
         *
         * @return 窗口累计成交金额，使用资产最小资金单位
         */
        public long getTotalAmount() {
            return totalAmount;
        }
    }

    /** 窗口内部成交记录，按时间顺序存放于 ArrayDeque。 */
    private static final class Transaction {

        /** 成交幂等标识。 */
        private final long transactionId;
        /** 成交金额，使用资产最小资金单位。 */
        private final long amount;
        /** 成交处理时间，单位为毫秒。 */
        private final long timestampMillis;

        /**
         * 创建窗口内部成交记录。
         *
         * @param transactionId 成交幂等标识
         * @param amount 成交金额
         * @param timestampMillis 成交时间
         */
        private Transaction(long transactionId, long amount, long timestampMillis) {
            this.transactionId = transactionId;
            this.amount = amount;
            this.timestampMillis = timestampMillis;
        }
    }
}
