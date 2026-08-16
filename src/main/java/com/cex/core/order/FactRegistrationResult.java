package com.cex.core.order;

/**
 * 事实事件登记结果，供订单状态机识别首次到达与重复到达。
 * 核心能力是为幂等处理提供明确分支；枚举不可变且线程安全。
 * 限制：仅表示登记结果，不代表订单是否已经完成状态迁移。
 */
public enum FactRegistrationResult {
    /** 首次观察到该类业务事实，可触发后续对账。 */
    NEW,
    /** 已观察到该类业务事实，属于重放事件但仍可参与对账。 */
    DUPLICATE
}
