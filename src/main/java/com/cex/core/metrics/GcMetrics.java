package com.cex.core.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public final class GcMetrics {
    private final long collectionCount;
    private final long collectionTimeMillis;
    private final long oldCollectorCount;
    private final long oldCollectorTimeMillis;

    private GcMetrics(long collectionCount, long collectionTimeMillis,
                      long oldCollectorCount, long oldCollectorTimeMillis) {
        this.collectionCount = collectionCount;
        this.collectionTimeMillis = collectionTimeMillis;
        this.oldCollectorCount = oldCollectorCount;
        this.oldCollectorTimeMillis = oldCollectorTimeMillis;
    }

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

    public long collectionCount() { return collectionCount; }
    public long collectionTimeMillis() { return collectionTimeMillis; }
    public long oldCollectorCount() { return oldCollectorCount; }
    public long oldCollectorTimeMillis() { return oldCollectorTimeMillis; }
}
