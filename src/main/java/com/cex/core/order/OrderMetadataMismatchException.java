package com.cex.core.order;

/**
 * 同一订单编号携带不一致不可变元数据时抛出的异常。
 * 核心能力是阻止用户、金额等身份属性被后续事件篡改；实例无共享可变状态，线程安全。
 * 限制：仅校验订单上下文元数据，不校验事件时间或状态合法性。
 */
public final class OrderMetadataMismatchException extends IllegalStateException {

    /**
     * 使用冲突说明创建异常。
     *
     * @param message 元数据不一致的诊断信息
     */
    public OrderMetadataMismatchException(String message) {
        super(message);
    }
}
