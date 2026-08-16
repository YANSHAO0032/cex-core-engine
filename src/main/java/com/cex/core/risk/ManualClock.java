package com.cex.core.risk;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 可由测试或模拟流程推进的原子毫秒时钟。
 * 核心能力：无休眠地验证时间相关风控规则；线程安全：以 {@link AtomicLong} 支持并发读取和推进；使用限制：不允许回拨时间。
 */
public final class ManualClock implements Clock {
    /** 当前可控毫秒时间戳。 */
    private final AtomicLong now;

    /**
     * 创建指定初始时间的手工时钟。
     *
     * @param initialMillis 初始毫秒时间戳，必须非负
     * @throws IllegalArgumentException 当初始时间为负数时抛出
     */
    public ManualClock(long initialMillis) {
        if (initialMillis < 0L) {
            throw new IllegalArgumentException("initialMillis must be non-negative");
        }
        now = new AtomicLong(initialMillis);
    }

    /**
     * 获取当前手工时间。
     *
     * @return 当前毫秒时间戳
     */
    @Override
    public long currentTimeMillis() {
        return now.get();
    }

    /**
     * 将时钟向前推进指定毫秒数。
     *
     * @param millis 要推进的毫秒数，必须非负
     * @return 推进后的毫秒时间戳
     * @throws IllegalArgumentException 当推进量为负数时抛出
     */
    public long advanceMillis(long millis) {
        if (millis < 0L) {
            throw new IllegalArgumentException("millis must be non-negative");
        }
        return now.addAndGet(millis);
    }
}
