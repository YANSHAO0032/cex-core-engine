package com.cex.core.order;

import java.util.concurrent.atomic.LongAdder;

/**
 * 订单引擎的并发累计指标。
 * 核心能力是以 {@link LongAdder} 低竞争地记录处理、资金和冲突数据；线程安全。
 * 限制：读数是统计快照，不保证跨多个指标的一致原子视图。
 *
 * @note 所有累加和读取方法均基于 {@link LongAdder}，无全局锁并可被交易线程并发调用；读取结果为弱一致性快照。
 */
public final class OrderEngineMetrics {
    /** 已进入引擎的事件总数。 */
    private final LongAdder processedEvents = new LongAdder();
    /** 首次登记成功的事实总数。 */
    private final LongAdder acceptedFacts = new LongAdder();
    /** 被识别为重复的事件总数。 */
    private final LongAdder duplicateEvents = new LongAdder();
    /** 在创建事实前到达的乱序事件总数。 */
    private final LongAdder outOfOrderEvents = new LongAdder();
    /** 实际发生的订单状态迁移总数。 */
    private final LongAdder stateTransitions = new LongAdder();
    /** 成功冻结资金的次数。 */
    private final LongAdder freezeCount = new LongAdder();
    /** 成功结算资金的次数。 */
    private final LongAdder settleCount = new LongAdder();
    /** 成功解冻资金的次数。 */
    private final LongAdder unfreezeCount = new LongAdder();
    /** 成交被成功计入风控窗口的次数。 */
    private final LongAdder riskRecordedCount = new LongAdder();
    /** 被风控暂挂的订单次数。 */
    private final LongAdder riskHoldCount = new LongAdder();
    /** 成功投递审批任务的次数。 */
    private final LongAdder approvalScheduledCount = new LongAdder();
    /** 首次收到审批通过事实的次数。 */
    private final LongAdder approvalPassCount = new LongAdder();
    /** 首次收到审批拒绝事实的次数。 */
    private final LongAdder approvalRejectCount = new LongAdder();
    /** 成交与取消同时出现的终态冲突次数。 */
    private final LongAdder conflictingTerminalEvents = new LongAdder();
    /** 审批通过与拒绝同时出现的冲突次数。 */
    private final LongAdder approvalConflictEvents = new LongAdder();
    /** 同订单元数据不一致的冲突次数。 */
    private final LongAdder metadataConflictEvents = new LongAdder();

    /** 累加已处理事件。 */
    public void processedEvent() { processedEvents.increment(); }
    /** 累加已接受事实。 */
    public void acceptedFact() { acceptedFacts.increment(); }
    /** 累加重复事件。 */
    public void duplicateEvent() { duplicateEvents.increment(); }
    /** 累加乱序事件。 */
    public void outOfOrderEvent() { outOfOrderEvents.increment(); }
    /** 累加状态迁移。 */
    public void stateTransition() { stateTransitions.increment(); }
    /** 累加资金冻结。 */
    public void freeze() { freezeCount.increment(); }
    /** 累加资金结算。 */
    public void settle() { settleCount.increment(); }
    /** 累加资金解冻。 */
    public void unfreeze() { unfreezeCount.increment(); }
    /** 累加风控成交记账。 */
    public void riskRecorded() { riskRecordedCount.increment(); }
    /** 累加风控暂挂。 */
    public void riskHold() { riskHoldCount.increment(); }
    /** 累加审批任务投递。 */
    public void approvalScheduled() { approvalScheduledCount.increment(); }
    /** 累加审批通过。 */
    public void approvalPass() { approvalPassCount.increment(); }
    /** 累加审批拒绝。 */
    public void approvalReject() { approvalRejectCount.increment(); }
    /** 累加终态冲突。 */
    public void conflictingTerminalEvent() { conflictingTerminalEvents.increment(); }
    /** 累加审批冲突。 */
    public void approvalConflict() { approvalConflictEvents.increment(); }
    /** 累加元数据冲突。 */
    public void metadataConflict() { metadataConflictEvents.increment(); }

    /**
     * 获取已处理事件总数。
     *
     * @return 已处理事件总数
     */
    public long processedEvents() { return processedEvents.sum(); }
    /**
     * 获取已接受事实总数。
     *
     * @return 已接受事实总数
     */
    public long acceptedFacts() { return acceptedFacts.sum(); }
    /**
     * 获取重复事件总数。
     *
     * @return 重复事件总数
     */
    public long duplicateEvents() { return duplicateEvents.sum(); }
    /**
     * 获取乱序事件总数。
     *
     * @return 乱序事件总数
     */
    public long outOfOrderEvents() { return outOfOrderEvents.sum(); }
    /**
     * 获取状态迁移总数。
     *
     * @return 状态迁移总数
     */
    public long stateTransitions() { return stateTransitions.sum(); }
    /**
     * 获取资金冻结总数。
     *
     * @return 资金冻结总数
     */
    public long freezeCount() { return freezeCount.sum(); }
    /**
     * 获取资金结算总数。
     *
     * @return 资金结算总数
     */
    public long settleCount() { return settleCount.sum(); }
    /**
     * 获取资金解冻总数。
     *
     * @return 资金解冻总数
     */
    public long unfreezeCount() { return unfreezeCount.sum(); }
    /**
     * 获取风控成交记账总数。
     *
     * @return 风控成交记账总数
     */
    public long riskRecordedCount() { return riskRecordedCount.sum(); }
    /**
     * 获取风控暂挂总数。
     *
     * @return 风控暂挂总数
     */
    public long riskHoldCount() { return riskHoldCount.sum(); }
    /**
     * 获取审批任务投递总数。
     *
     * @return 审批任务投递总数
     */
    public long approvalScheduledCount() { return approvalScheduledCount.sum(); }
    /**
     * 获取审批通过总数。
     *
     * @return 审批通过总数
     */
    public long approvalPassCount() { return approvalPassCount.sum(); }
    /**
     * 获取审批拒绝总数。
     *
     * @return 审批拒绝总数
     */
    public long approvalRejectCount() { return approvalRejectCount.sum(); }
    /**
     * 获取终态冲突总数。
     *
     * @return 终态冲突总数
     */
    public long conflictingTerminalEvents() { return conflictingTerminalEvents.sum(); }
    /**
     * 获取审批冲突总数。
     *
     * @return 审批冲突总数
     */
    public long approvalConflictEvents() { return approvalConflictEvents.sum(); }
    /**
     * 获取元数据冲突总数。
     *
     * @return 元数据冲突总数
     */
    public long metadataConflictEvents() { return metadataConflictEvents.sum(); }
}
