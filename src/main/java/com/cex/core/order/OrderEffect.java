package com.cex.core.order;

/**
 * 订单副作用的幂等提交标记。
 * 核心能力是记录冻结、结算、解冻、风控记账和审批投递是否已执行；枚举不可变且线程安全。
 * 限制：标记本身不执行副作用，必须在用户锁保护下由 {@link OrderContext} 提交。
 */
public enum OrderEffect {
    /** 资金冻结已提交，防止重复冻结。 */
    FREEZE_APPLIED(1 << 0),
    /** 资金结算已提交，防止重复结算。 */
    SETTLE_APPLIED(1 << 1),
    /** 冻结资金已释放，防止重复解冻。 */
    UNFREEZE_APPLIED(1 << 2),
    /** 成交金额已写入风控窗口，防止重复计入风险敞口。 */
    RISK_RECORDED(1 << 3),
    /** 审批任务已投递，防止重复异步调度。 */
    APPROVAL_SCHEDULED(1 << 4);

    /** 当前副作用在订单副作用位图中的位掩码。 */
    private final int mask;

    /**
     * 创建订单副作用及其唯一幂等位掩码映射。
     *
     * @param mask 副作用在用户锁保护位图中的唯一二进制掩码
     */
    OrderEffect(int mask) {
        this.mask = mask;
    }

    /**
     * 返回当前副作用的位掩码。
     *
     * @return 用于订单副作用位图的唯一位掩码
     */
    public int mask() {
        return mask;
    }
}
