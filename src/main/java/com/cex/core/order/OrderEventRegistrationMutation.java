package com.cex.core.order;

/**
 * 单订单权威事件登记准备阶段生成的不透明不可变变更。
 *
 * <p>核心能力：绑定目标订单、候选事件、登记结果和预计算插入决策，使多订单协调器可先验证全部登记再统一提交。</p>
 * <p>线程安全：对象不可变；从准备至提交期间仍须持续持有目标订单所属用户锁。</p>
 * <p>使用限制：只能由 {@link OrderStateMachine} 创建和提交，调用方不能读取或改写内部提交字段。</p>
 *
 * @note mutation 不复制订单映射；提交只执行准备阶段确定的插入或无操作，不再校验身份、序号、冲突或容量。
 */
public final class OrderEventRegistrationMutation {
    /** 已验证且与 mutation 绑定的目标订单。 */
    private final OrderContext order;
    /** 已验证的候选权威事件。 */
    private final SequencedOrderEvent event;
    /** 准备阶段确定的登记结果。 */
    private final SequenceRegistrationResult result;
    /** 提交阶段是否向待处理映射写入候选事件。 */
    private final boolean insertsEvent;

    /**
     * 创建已完成全部登记校验的不可变变更。
     *
     * @param order 已验证的目标订单
     * @param event 已验证的候选权威事件
     * @param result 预计算登记结果
     * @param insertsEvent 提交时是否插入候选事件
     * @note 构造器限制在订单包内；调用方不得绕过状态机自行构造登记变更。
     */
    OrderEventRegistrationMutation(
            OrderContext order,
            SequencedOrderEvent event,
            SequenceRegistrationResult result,
            boolean insertsEvent) {
        this.order = order;
        this.event = event;
        this.result = result;
        this.insertsEvent = insertsEvent;
    }

    /**
     * 获取变更绑定的目标订单。
     *
     * @return 已验证的目标订单上下文
     */
    OrderContext order() { return order; }

    /**
     * 获取待登记的权威事件。
     *
     * @return 已验证的候选事件
     */
    SequencedOrderEvent event() { return event; }

    /**
     * 获取准备阶段确定的登记结果。
     *
     * @return 可处理、已缓存、重复或过期结果
     */
    SequenceRegistrationResult result() { return result; }

    /**
     * 判断提交阶段是否需要写入待处理事件映射。
     *
     * @return 需要插入候选事件时为 {@code true}
     */
    boolean insertsEvent() { return insertsEvent; }
}
