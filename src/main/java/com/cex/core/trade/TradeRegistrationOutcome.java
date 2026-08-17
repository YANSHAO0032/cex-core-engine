package com.cex.core.trade;

import java.util.Objects;

/**
 * 成交存储在单一注册线性化结果中返回的不可变结果。
 *
 * <p>核心能力：同时携带已发布记录和当前调用是否为精确重复，避免调用方在注册前后另做竞争读取。</p>
 * <p>线程安全：记录引用与重复标志均不可变，可在线程间安全共享。</p>
 * <p>使用限制：重复标志只描述本次注册调用，不表示记录是否已进入终态。</p>
 *
 * @param record 已发布或已存在的权威成交记录
 * @param duplicate 当前调用命中相同成交标识与载荷时为 {@code true}
 */
public record TradeRegistrationOutcome(
        TradeExecutionRecord record,
        boolean duplicate) {
    /**
     * 创建成交注册结果并校验记录引用。
     *
     * @param record 已发布或已存在的权威成交记录
     * @param duplicate 当前调用是否为精确重复
     * @throws NullPointerException 当记录为 {@code null} 时抛出
     */
    public TradeRegistrationOutcome {
        Objects.requireNonNull(record, "record");
    }
}
