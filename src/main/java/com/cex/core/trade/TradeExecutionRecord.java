package com.cex.core.trade;

import com.cex.core.order.TradeExecution;
import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 存储层固化的成交载荷及其一次性终态结果。
 *
 * <p>核心能力：保留权威成交元数据，并使挂起状态至终态的迁移只发生一次。</p>
 * <p>线程安全：终态迁移由记录自身监视器串行化，状态和终态元数据使用 {@code volatile} 发布。</p>
 * <p>使用限制：记录不执行订单、账本或索引副作用；这些由存储和协调器完成。</p>
 */
public final class TradeExecutionRecord {
    /** 不可替换的权威成交载荷。 */
    private final TradeExecution execution;
    /** 当前生命周期状态。 */
    private volatile TradeExecutionState state = TradeExecutionState.PENDING;
    /** 终态完成时间；挂起时为 {@code -1}。 */
    private volatile long completedAtMillis = -1L;
    /** 拒绝原因；除 {@link TradeExecutionState#REJECTED} 外为 {@code null}。 */
    private volatile String rejectionReason;

    /**
     * 以不可变权威成交创建一个挂起记录。
     *
     * @param execution 权威成交载荷，不能为空
     */
    TradeExecutionRecord(TradeExecution execution) {
        this.execution = Objects.requireNonNull(execution, "execution");
    }

    /**
     * 返回不可替换的权威成交载荷。
     *
     * @return 登记时固化的成交
     */
    public TradeExecution execution() {
        return execution;
    }

    /**
     * 返回当前生命周期状态。
     *
     * @return 挂起、已结算或已拒绝状态
     */
    public TradeExecutionState state() {
        return state;
    }

    /**
     * 返回终态完成时间。
     *
     * @return 终态完成毫秒；挂起时为 {@code -1}
     */
    public long completedAtMillis() {
        return completedAtMillis;
    }

    /**
     * 返回确定拒绝的原因。
     *
     * @return 拒绝原因；未拒绝时为 {@code null}
     */
    public String rejectionReason() {
        return rejectionReason;
    }

    /**
     * 将记录当前状态映射为非重复的入口结果。
     *
     * @return 与当前记录状态对应的结果
     */
    public TradeResult result() {
        return switch (state) {
            case PENDING -> TradeResult.PENDING;
            case SETTLED -> TradeResult.SETTLED;
            case REJECTED -> TradeResult.REJECTED;
        };
    }

    /**
     * 判断候选成交是否与固化载荷完全相同。
     *
     * @param candidate 待比较的候选成交，不能为空
     * @return 两个成交所有记录组件均相同时为 {@code true}
     */
    boolean hasSameExecution(TradeExecution candidate) {
        return execution.equals(Objects.requireNonNull(candidate, "candidate"));
    }

    /**
     * 仅当当前状态仍为挂起时登记已结算终态。
     *
     * @param completedAtMillis 非负的终态完成毫秒
     * @return 本次调用实际完成迁移时为 {@code true}
     * @note 记录自身锁确保与拒绝迁移竞争时仅有一个调用赢得终态，调用方据此只删除一次索引并释放一次挂起容量。
     */
    synchronized boolean markSettled(long completedAtMillis) {
        MoneyMath.requireNonNegative(completedAtMillis);
        if (state != TradeExecutionState.PENDING) {
            return false;
        }
        this.completedAtMillis = completedAtMillis;
        state = TradeExecutionState.SETTLED;
        return true;
    }

    /**
     * 仅当当前状态仍为挂起时登记已拒绝终态。
     *
     * @param reason 非空的确定拒绝原因
     * @param completedAtMillis 非负的终态完成毫秒
     * @return 本次调用实际完成迁移时为 {@code true}
     * @note 记录自身锁确保与结算迁移竞争时仅有一个调用赢得终态，调用方据此只删除一次索引并释放一次挂起容量。
     */
    synchronized boolean markRejected(String reason, long completedAtMillis) {
        Objects.requireNonNull(reason, "reason");
        MoneyMath.requireNonNegative(completedAtMillis);
        if (state != TradeExecutionState.PENDING) {
            return false;
        }
        rejectionReason = reason;
        this.completedAtMillis = completedAtMillis;
        state = TradeExecutionState.REJECTED;
        return true;
    }
}
