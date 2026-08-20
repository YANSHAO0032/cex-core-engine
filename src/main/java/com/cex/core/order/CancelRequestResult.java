package com.cex.core.order;

/**
 * 撤单请求入口的确定性处理结果。
 *
 * <p>核心能力：区分首次接收、幂等重复和已终态订单三种结果。</p>
 * <p>线程安全：枚举实例不可变且天然线程安全。</p>
 * <p>使用限制：不表示外部撤单是否已确认。</p>
 */
public enum CancelRequestResult {
    /** 首次接收撤单请求，已提交至后续处理。 */
    SUBMITTED,
    /** 已接收相同请求标识，按幂等语义不重复处理。 */
    DUPLICATE,
    /** 订单已经处于终态，不能再提交撤单请求。 */
    ALREADY_TERMINAL
}
