package com.cex.core.order;

/**
 * 发送撤单请求至外部边界的函数式契约。
 *
 * <p>核心能力：隔离订单域与外部撤单传输机制。</p>
 * <p>线程安全：实现方负责其外部传输资源的并发安全。</p>
 * <p>使用限制：接收请求不等同于撤单确认，确认须通过独立事件回流。</p>
 */
@FunctionalInterface
public interface CancelRequestSink {

    /**
     * 提交并发送一个经过校验的撤单请求。
     *
     * @param request 待发送的不可变撤单请求，不能为空
     */
    void submit(CancelRequest request);
}
