package com.cex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 使用预设纳秒桶并发汇总调用延迟的直方图。
 *
 * <p>能力：记录延迟、读取总量与最大值，并基于桶边界估算常用分位数。</p>
 * <p>线程安全：桶、计数和累计值使用并发原子累加器维护，可被多线程同时记录和读取。</p>
 * <p>限制：分位数是桶上界估算值而非精确样本值；超过最后桶的值以最后桶两倍作为估算边界。</p>
 */
public final class LatencyHistogram {
    /** 各延迟桶的开区间上界，单位为纳秒。 */
    private static final long[] UPPER_BOUNDS_NANOS = {
            10_000L, 25_000L, 50_000L, 100_000L, 250_000L,
            500_000L, 1_000_000L, 2_000_000L, 5_000_000L
    };
    /** 各延迟区间及溢出区间的并发计数器。 */
    private final LongAdder[] buckets = new LongAdder[UPPER_BOUNDS_NANOS.length + 1];
    /** 已记录样本总数。 */
    private final LongAdder count = new LongAdder();
    /** 已记录延迟的纳秒总和。 */
    private final LongAdder totalNanos = new LongAdder();
    /** 已记录的最大延迟，使用原子更新维护。 */
    private final AtomicLong maxNanos = new AtomicLong();

    /** 创建并初始化全部延迟桶。 */
    public LatencyHistogram() {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /**
     * 记录一个非负纳秒延迟样本。
     *
     * @param nanos 延迟值，单位为纳秒，必须非负
     * @throws IllegalArgumentException 当延迟为负数时抛出
     * @note 桶计数和汇总值使用原子累加器，无全局锁，支持多个性能工作线程并发记录。
     */
    public void record(long nanos) {
        if (nanos < 0L) {
            throw new IllegalArgumentException("nanos must be non-negative");
        }
        int bucket = 0;
        while (bucket < UPPER_BOUNDS_NANOS.length && nanos >= UPPER_BOUNDS_NANOS[bucket]) {
            bucket++;
        }
        buckets[bucket].increment();
        count.increment();
        totalNanos.add(nanos);
        maxNanos.accumulateAndGet(nanos, Math::max);
    }

    /**
     * 获取已记录延迟样本数。
     *
     * @return 样本总数
     * @note 并发读取为弱一致快照，不阻塞正在记录的线程。
     */
    public long count() { return count.sum(); }

    /**
     * 获取已记录延迟的纳秒总和。
     *
     * @return 延迟纳秒总和
     * @note 并发读取为弱一致快照，不阻塞正在记录的线程。
     */
    public long totalNanos() { return totalNanos.sum(); }

    /**
     * 获取已记录的最大延迟。
     *
     * @return 最大延迟，单位为纳秒
     * @note 使用原子读取返回单值一致结果。
     */
    public long maxNanos() { return maxNanos.get(); }

    /**
     * 计算平均延迟，单位为微秒。
     *
     * @return 平均延迟；没有样本时返回 {@code 0.0}
     * @note 计数和总和分别读取，并发记录期间结果为弱一致估算值。
     */
    public double averageMicros() { return count() == 0L ? 0.0 : totalNanos() / (double) count() / 1_000.0; }

    /**
     * 根据桶累计计数估算给定分位数的纳秒延迟。
     *
     * @param percentile 分位数，范围为 {@code [0.0, 1.0]}
     * @return 对应桶上界的估算延迟；没有样本时返回 {@code 0L}
     * @throws IllegalArgumentException 当分位数不在有效范围内时抛出
     * @note 逐桶读取并发计数形成弱一致快照，不锁定正在记录的线程。
     */
    public long percentileNanos(double percentile) {
        if (percentile < 0.0 || percentile > 1.0) {
            throw new IllegalArgumentException("percentile must be between 0 and 1");
        }
        long total = count();
        if (total == 0L) {
            return 0L;
        }
        long target = Math.max(1L, (long) Math.ceil(total * percentile));
        long seen = 0L;
        for (int i = 0; i < buckets.length; i++) {
            seen += buckets[i].sum();
            if (seen >= target) {
                return i < UPPER_BOUNDS_NANOS.length ? UPPER_BOUNDS_NANOS[i] : UPPER_BOUNDS_NANOS[UPPER_BOUNDS_NANOS.length - 1] * 2L;
            }
        }
        return maxNanos();
    }

    /**
     * 估算 P50 延迟。
     *
     * @return P50 延迟估算值，单位为纳秒
     * @note 委托并发安全的桶快照估算，不阻塞记录线程。
     */
    public long p50Nanos() { return percentileNanos(0.50); }

    /**
     * 估算 P95 延迟。
     *
     * @return P95 延迟估算值，单位为纳秒
     * @note 委托并发安全的桶快照估算，不阻塞记录线程。
     */
    public long p95Nanos() { return percentileNanos(0.95); }

    /**
     * 估算 P99 延迟。
     *
     * @return P99 延迟估算值，单位为纳秒
     * @note 委托并发安全的桶快照估算，不阻塞记录线程。
     */
    public long p99Nanos() { return percentileNanos(0.99); }
}
