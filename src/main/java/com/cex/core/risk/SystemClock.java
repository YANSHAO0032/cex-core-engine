package com.cex.core.risk;

/**
 * 基于操作系统时间的生产环境时钟。
 * 核心能力：提供实时毫秒时间；线程安全：无状态且天然线程安全；使用限制：依赖系统时钟，测试时间敏感场景应使用 {@link ManualClock}。
 */
public final class SystemClock implements Clock {
    /**
     * 获取系统当前时间。
     *
     * @return 当前系统毫秒时间戳
     */
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
