package com.cex.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class LatencyHistogram {
    private static final long[] UPPER_BOUNDS_NANOS = {
            10_000L, 25_000L, 50_000L, 100_000L, 250_000L,
            500_000L, 1_000_000L, 2_000_000L, 5_000_000L
    };
    private final LongAdder[] buckets = new LongAdder[UPPER_BOUNDS_NANOS.length + 1];
    private final LongAdder count = new LongAdder();
    private final LongAdder totalNanos = new LongAdder();
    private final AtomicLong maxNanos = new AtomicLong();

    public LatencyHistogram() {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

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

    public long count() { return count.sum(); }
    public long totalNanos() { return totalNanos.sum(); }
    public long maxNanos() { return maxNanos.get(); }
    public double averageMicros() { return count() == 0L ? 0.0 : totalNanos() / (double) count() / 1_000.0; }
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

    public long p50Nanos() { return percentileNanos(0.50); }
    public long p95Nanos() { return percentileNanos(0.95); }
    public long p99Nanos() { return percentileNanos(0.99); }
}
