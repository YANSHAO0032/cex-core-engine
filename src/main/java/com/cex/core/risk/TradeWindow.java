package com.cex.core.risk;

import com.cex.core.util.MoneyMath;

/**
 * 维护近期成交金额的可变滑动窗口。
 * 核心能力：记录含乱序时间戳的成交并按时间淘汰过期金额，提供窗口累计值；线程安全：非线程安全，应由订单处理的外部串行或锁保护；使用限制：记录金额必须为正。
 */
public final class TradeWindow {
    /** 滑动窗口时长；订单流程当前使用 10 秒（10,000 毫秒）窗口。 */
    private final long windowMillis;
    /** primitive 环形缓冲中各成交记录的毫秒时间戳。 */
    private long[] timestamps;
    /** primitive 环形缓冲中与时间戳对应的成交金额，单位为货币最小单位。 */
    private long[] amounts;
    /** 环形缓冲中最早未过期记录的索引。 */
    private int head;
    /** 环形缓冲中有效成交记录数量。 */
    private int size;
    /** 当前窗口内未过期成交金额累计值，单位为货币最小单位。 */
    private long rollingSum;
    /** 当前窗口或查询已观察到的最大毫秒时间戳；尚未观察时为 -1。 */
    private long latestObservedTimestamp = -1L;

    /**
     * 创建指定时长的成交滑动窗口。
     *
     * @param windowMillis 窗口时长（毫秒），必须为正数；业务默认值为 10 秒
     * @throws IllegalArgumentException 当窗口时长不为正数时抛出
     */
    public TradeWindow(long windowMillis) {
        if (windowMillis <= 0L) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        this.windowMillis = windowMillis;
        this.timestamps = new long[16];
        this.amounts = new long[16];
    }

    /**
     * 记录一笔成交金额并先回收此时刻已过期的记录。
     * @param timestampMillis 成交发生的非负毫秒时间戳
     * @param amount 正的成交金额
     * @throws IllegalArgumentException 当时间为负数或金额不为正数时抛出
     * @note 使用 primitive 环形缓冲；单调时间戳无分配追加，逆序时间戳仅在仍位于窗口内时构造排序替换数组。
     */
    public void record(long timestampMillis, long amount) {
        commitRecord(prepareRecord(timestampMillis, amount));
    }

    /**
     * 无副作用地准备一笔成交记录及逻辑过期回收。
     *
     * @param timestampMillis 成交发生的非负毫秒时间戳
     * @param amount 正的报价资产成交金额
     * @return 已完成过期回收、精确加法和可选扩容的窗口变更
     * @throws IllegalArgumentException 当时间为负数或金额不为正数时抛出
     * @throws ArithmeticException 当累计金额或扩容计算溢出时抛出
     * @note 准备阶段不修改 live 数组、头索引、记录数或累计值；调用方可先准备双方窗口再统一提交。
     */
    public TradeWindowMutation prepareRecord(long timestampMillis, long amount) {
        long normalizedTimestamp = MoneyMath.requireNonNegative(timestampMillis);
        long normalizedAmount = MoneyMath.requirePositive(amount);
        long latestAfter = Math.max(latestObservedTimestamp, normalizedTimestamp);
        long cutoff = latestAfter - windowMillis;
        int evicted = 0;
        long logicalSum = rollingSum;
        while (evicted < size) {
            int index = (head + evicted) % timestamps.length;
            if (timestamps[index] >= cutoff) {
                break;
            }
            logicalSum = Math.subtractExact(logicalSum, amounts[index]);
            evicted++;
        }
        int logicalHead = (head + evicted) % timestamps.length;
        int logicalSize = size - evicted;
        if (normalizedTimestamp < cutoff) {
            return new TradeWindowMutation(
                    null, null, -1, normalizedTimestamp, normalizedAmount,
                    logicalHead, logicalSize, logicalSum, latestAfter);
        }
        long sumAfter = MoneyMath.checkedAdd(logicalSum, normalizedAmount);
        int sizeAfter = Math.addExact(logicalSize, 1);
        if (sizeAfter <= timestamps.length
                && (logicalSize == 0
                || normalizedTimestamp >= timestampAt(logicalHead, logicalSize - 1))) {
            int tail = (logicalHead + logicalSize) % timestamps.length;
            return new TradeWindowMutation(
                    null, null, tail, normalizedTimestamp, normalizedAmount,
                    logicalHead, sizeAfter, sumAfter, latestAfter);
        }

        int replacementCapacity = timestamps.length;
        if (sizeAfter > replacementCapacity) {
            replacementCapacity = Math.multiplyExact(replacementCapacity, 2);
            while (replacementCapacity < sizeAfter) {
                replacementCapacity = Math.multiplyExact(replacementCapacity, 2);
            }
        }
        long[] newTimestamps = new long[replacementCapacity];
        long[] newAmounts = new long[replacementCapacity];
        int insertionIndex = 0;
        while (insertionIndex < logicalSize
                && timestampAt(logicalHead, insertionIndex) <= normalizedTimestamp) {
            copyEntry(logicalHead, insertionIndex,
                    newTimestamps, newAmounts, insertionIndex);
            insertionIndex++;
        }
        newTimestamps[insertionIndex] = normalizedTimestamp;
        newAmounts[insertionIndex] = normalizedAmount;
        for (int sourceOffset = insertionIndex; sourceOffset < logicalSize; sourceOffset++) {
            copyEntry(logicalHead, sourceOffset,
                    newTimestamps, newAmounts, sourceOffset + 1);
        }
        return new TradeWindowMutation(
                newTimestamps, newAmounts, -1, normalizedTimestamp, normalizedAmount,
                0, sizeAfter, sumAfter, latestAfter);
    }

    /**
     * 提交一份由本窗口当前状态准备的成交变更。
     *
     * @param mutation 尚未提交且由本窗口最新状态准备的变更
     * @throws NullPointerException 当变更为 {@code null} 时抛出
     * @note 提交阶段仅执行有界 primitive 数组或字段赋值，不进行算术、扩容和业务校验；调用方须持续持有对应用户锁。
     */
    public void commitRecord(TradeWindowMutation mutation) {
        long[] replacementTimestamps = mutation.replacementTimestamps();
        if (replacementTimestamps == null && mutation.tailIndex() >= 0) {
            timestamps[mutation.tailIndex()] = mutation.timestampMillis();
            amounts[mutation.tailIndex()] = mutation.amount();
        } else if (replacementTimestamps != null) {
            timestamps = replacementTimestamps;
            amounts = mutation.replacementAmounts();
        }
        head = mutation.headAfter();
        size = mutation.sizeAfter();
        rollingSum = mutation.rollingSumAfter();
        latestObservedTimestamp = mutation.latestObservedTimestampAfter();
    }

    /**
     * 获取当前时间下窗口内的成交金额总和。
     * @param nowMillis 当前毫秒时间，用于回收过期记录
     * @return 回收过期记录后的窗口累计金额
     * @throws IllegalArgumentException 当前时间为负数时抛出
     * @note 查询同样触发过期回收，并以已观察最大时间防止时钟回拨使窗口边界倒退。
     */
    public long currentSum(long nowMillis) {
        long normalizedNow = MoneyMath.requireNonNegative(nowMillis);
        latestObservedTimestamp = Math.max(latestObservedTimestamp, normalizedNow);
        evict(latestObservedTimestamp);
        return rollingSum;
    }

    /** 获取未过期成交记录数。
     * @return 环形缓冲中有效记录数量
     */
    public int size() {
        return size;
    }

    /**
     * 回收严格早于窗口下界的成交记录。
     *
     * @param nowMillis 当前毫秒时间
     * @note 以 head 顺序弹出 primitive 环形缓冲；记录恰在边界时仍保留在窗口内。
     */
    private void evict(long nowMillis) {
        long cutoff = nowMillis - windowMillis;
        // 仅从最旧端回收，时间有序输入下每条记录最多被处理一次。
        while (size > 0 && timestamps[head] < cutoff) {
            rollingSum = MoneyMath.checkedSubtract(rollingSum, amounts[head]);
            timestamps[head] = 0L;
            amounts[head] = 0L;
            head = (head + 1) % timestamps.length;
            size--;
        }
    }

    /**
     * 读取逻辑有序窗口中指定偏移的时间戳。
     *
     * @param logicalHead 逻辑头索引
     * @param offset 相对逻辑头的非负偏移
     * @return 指定有效记录的毫秒时间戳
     */
    private long timestampAt(int logicalHead, int offset) {
        return timestamps[(logicalHead + offset) % timestamps.length];
    }

    /**
     * 将逻辑有序窗口中的一条 primitive 记录复制到替换数组。
     *
     * @param logicalHead 逻辑头索引
     * @param sourceOffset 相对逻辑头的源偏移
     * @param targetTimestamps 目标时间戳数组
     * @param targetAmounts 目标金额数组
     * @param targetIndex 目标数组索引
     */
    private void copyEntry(int logicalHead, int sourceOffset,
                           long[] targetTimestamps, long[] targetAmounts,
                           int targetIndex) {
        int sourceIndex = (logicalHead + sourceOffset) % timestamps.length;
        targetTimestamps[targetIndex] = timestamps[sourceIndex];
        targetAmounts[targetIndex] = amounts[sourceIndex];
    }

}
