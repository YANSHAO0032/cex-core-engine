package com.cex.core.risk;

/**
 * 为风控窗口提供当前时间的抽象时钟。
 * 核心能力：隔离系统时间与可控测试时间；线程安全：取决于实现；使用限制：返回值应为非负毫秒时间戳。
 */
public interface Clock {
    /**
     * 获取当前毫秒时间。
     *
     * @return 当前毫秒时间戳
     */
    long currentTimeMillis();
}
