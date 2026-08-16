package com.cex.core.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * JVM 垃圾回收次数与耗时的不可变快照。
 *
 * <p>能力：采集全部收集器及疑似老年代收集器的累计指标。</p>
 * <p>线程安全：实例为不可变对象，快照采集仅读取 JVM 管理接口。</p>
 * <p>限制：老年代收集器通过名称关键字识别，具体分类依赖 JVM 实现与收集器命名。</p>
 */
public final class GcMetrics {
    /** 所有收集器累计的回收次数。 */
    private final long collectionCount;
    /** 所有收集器累计的回收耗时，单位为毫秒。 */
    private final long collectionTimeMillis;
    /** 被识别为老年代收集器的累计回收次数。 */
    private final long oldCollectorCount;
    /** 被识别为老年代收集器的累计回收耗时，单位为毫秒。 */
    private final long oldCollectorTimeMillis;

    /**
     * 创建一次不可变的 GC 累计指标快照。
     *
     * @param collectionCount 全部收集器累计回收次数
     * @param collectionTimeMillis 全部收集器累计回收耗时，单位为毫秒
     * @param oldCollectorCount 老年代或 Full GC 收集器累计回收次数
     * @param oldCollectorTimeMillis 老年代或 Full GC 收集器累计回收耗时，单位为毫秒
     */
    private GcMetrics(long collectionCount, long collectionTimeMillis,
                      long oldCollectorCount, long oldCollectorTimeMillis) {
        this.collectionCount = collectionCount;
        this.collectionTimeMillis = collectionTimeMillis;
        this.oldCollectorCount = oldCollectorCount;
        this.oldCollectorTimeMillis = oldCollectorTimeMillis;
    }

    /**
     * 读取 JVM 当前的垃圾回收累计指标并创建快照。
     *
     * @return 当前 GC 指标快照
     */
    public static GcMetrics snapshot() {
        long count = 0L;
        long time = 0L;
        long oldCount = 0L;
        long oldTime = 0L;
        List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : beans) {
            long beanCount = Math.max(0L, bean.getCollectionCount());
            long beanTime = Math.max(0L, bean.getCollectionTime());
            count += beanCount;
            time += beanTime;
            String name = bean.getName().toLowerCase(java.util.Locale.ROOT);
            if (name.contains("old") || name.contains("mark") || name.contains("mixed")) {
                oldCount += beanCount;
                oldTime += beanTime;
            }
        }
        return new GcMetrics(count, time, oldCount, oldTime);
    }

    /**
     * 获取全部收集器的累计回收次数。
     *
     * @return 累计回收次数
     */
    public long collectionCount() { return collectionCount; }

    /**
     * 获取全部收集器的累计回收耗时。
     *
     * @return 累计耗时，单位为毫秒
     */
    public long collectionTimeMillis() { return collectionTimeMillis; }

    /**
     * 获取识别为老年代的收集器累计回收次数。
     *
     * @return 老年代收集器累计回收次数
     */
    public long oldCollectorCount() { return oldCollectorCount; }

    /**
     * 获取识别为老年代的收集器累计回收耗时。
     *
     * @return 老年代收集器累计耗时，单位为毫秒
     */
    public long oldCollectorTimeMillis() { return oldCollectorTimeMillis; }
}
