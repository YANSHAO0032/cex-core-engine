package com.cex.core.risk;

import com.cex.core.util.MoneyMath;

/**
 * 维护近期成交金额的可变滑动窗口。
 * 核心能力：记录成交并按时间淘汰过期金额，提供窗口累计值；线程安全：非线程安全，应由订单处理的外部串行或锁保护；使用限制：记录金额必须为正且时间戳应单调递增。
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
     * @param timestampMillis 成交发生的毫秒时间戳
     * @param amount 正的成交金额
     * @throws IllegalArgumentException 当金额不为正数时抛出
     * @note 使用 primitive 环形缓冲避免对象分配；调用方需提供时间有序事件以维持过期回收正确性。
     */
    public void record(long timestampMillis, long amount) {
        MoneyMath.requirePositive(amount);
        evict(timestampMillis);
        ensureCapacity(size + 1);
        int tail = (head + size) % timestamps.length;
        timestamps[tail] = timestampMillis;
        amounts[tail] = amount;
        size++;
        rollingSum = MoneyMath.checkedAdd(rollingSum, amount);
    }

    /**
     * 获取当前时间下窗口内的成交金额总和。
     * @param nowMillis 当前毫秒时间，用于回收过期记录
     * @return 回收过期记录后的窗口累计金额
     * @note 查询同样触发过期回收，使 10 秒窗口无需后台清理线程。
     */
    public long currentSum(long nowMillis) {
        evict(nowMillis);
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
     * 在容量不足时扩展环形缓冲并保持记录的时间顺序。
     *
     * @param required 所需最小容量
     * @note 扩容仅在写入路径发生；复制后重置 head，保留 primitive 数组的低开销特性。
     */
    private void ensureCapacity(int required) {
        if (required <= timestamps.length) {
            return;
        }
        int next = timestamps.length << 1;
        while (next < required) {
            next <<= 1;
        }
        long[] newTimestamps = new long[next];
        long[] newAmounts = new long[next];
        for (int i = 0; i < size; i++) {
            int index = (head + i) % timestamps.length;
            newTimestamps[i] = timestamps[index];
            newAmounts[i] = amounts[index];
        }
        timestamps = newTimestamps;
        amounts = newAmounts;
        head = 0;
    }
}
