package com.cex.core.order;

import com.cex.core.util.MoneyMath;

/**
 * 可由请求标识幂等识别的不可变撤单请求。
 *
 * <p>核心能力：向外部撤单边界传递请求身份、订单身份和请求时间。</p>
 * <p>线程安全：记录所有组件不可变，可在线程间安全传递。</p>
 * <p>使用限制：请求不代表订单已撤销，仍须等待权威撤单确认。</p>
 *
 * @param cancelRequestId 严格为正的幂等撤单请求标识
 * @param orderId 严格为正的待撤销订单标识
 * @param requestedAtMillis 非负的请求毫秒时间戳
 */
public record CancelRequest(long cancelRequestId, long orderId, long requestedAtMillis) {

    /**
     * 创建并校验撤单请求。
     *
     * @param cancelRequestId 严格为正的幂等撤单请求标识
     * @param orderId 严格为正的待撤销订单标识
     * @param requestedAtMillis 非负的请求毫秒时间戳
     * @throws IllegalArgumentException 当标识不为正数或时间为负数时抛出
     */
    public CancelRequest {
        MoneyMath.requirePositive(cancelRequestId);
        MoneyMath.requirePositive(orderId);
        MoneyMath.requireNonNegative(requestedAtMillis);
    }
}
