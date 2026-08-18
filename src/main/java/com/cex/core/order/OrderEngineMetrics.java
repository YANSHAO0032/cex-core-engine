package com.cex.core.order;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 强类型订单与双边成交路径的并发指标聚合器。
 *
 * <p>核心能力：记录部分成交、幂等、序号空洞、撤单和确定拒绝，并暴露当前挂起成交数。</p>
 * <p>线程安全：累计值使用 {@link LongAdder}，当前值使用 {@link AtomicInteger}，无全局指标锁。</p>
 * <p>使用限制：多个指标的读取是弱一致快照，不构成同一时刻的原子报表。</p>
 *
 * @note 写入方法可由订单入口和双边协调器并发调用，适配高并发交易路径。
 */
public final class OrderEngineMetrics {
    /** 至少一侧订单在成交后仍处于部分成交的成交次数。 */
    private final LongAdder partialFillCount = new LongAdder();
    /** 成功完成双边原子结算的成交次数。 */
    private final LongAdder settledTradeCount = new LongAdder();
    /** 相同成交标识与载荷的重复投递次数。 */
    private final LongAdder duplicateTradeCount = new LongAdder();
    /** 当前仍在成交存储中等待终结的记录数量。 */
    private final AtomicInteger pendingTradeCount = new AtomicInteger();
    /** 相同成交标识绑定不同载荷的协议冲突次数。 */
    private final LongAdder tradeMetadataConflictCount = new LongAdder();
    /** 因订单权威序号不连续而缓存的输入次数。 */
    private final LongAdder sequenceGapCount = new LongAdder();
    /** 首次进入等待外部撤单确认状态的订单次数。 */
    private final LongAdder pendingCancelCount = new LongAdder();
    /** 已被订单序号消费的迟到撤单确认次数。 */
    private final LongAdder staleCancelConfirmationCount = new LongAdder();
    /** 双方权威序号被消费且确定拒绝的成交次数。 */
    private final LongAdder tradeRejectedCount = new LongAdder();

    /** 创建全部累计值为零的订单引擎指标。 */
    public OrderEngineMetrics() {
    }

    /**
     * 累加一次部分成交。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加，可由多个结算线程并发调用。
     */
    public void partialFill() {
        partialFillCount.increment();
    }

    /**
     * 累加一次成功双边结算。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加，可由多个结算线程并发调用。
     */
    public void settledTrade() {
        settledTradeCount.increment();
    }

    /**
     * 累加一次精确重复成交投递。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加，注册线性化结果保证每个重复调用只计一次。
     */
    public void duplicateTrade() {
        duplicateTradeCount.increment();
    }

    /**
     * 发布成交存储当前挂起记录数。
     *
     * @param count 非负挂起成交数量
     * @throws IllegalArgumentException 当数量为负数时抛出
     * @note 使用 {@link AtomicInteger} 原子发布当前值；并发读取可见最新一次完整写入。
     */
    public void pendingTradeCount(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("pending trade count must be non-negative");
        }
        pendingTradeCount.set(count);
    }

    /**
     * 累加一次成交元数据冲突。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加，可由并发注册入口调用。
     */
    public void tradeMetadataConflict() {
        tradeMetadataConflictCount.increment();
    }

    /**
     * 累加一次权威序号空洞。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加；调用方保证同一事件首次进入空洞时才计数。
     */
    public void sequenceGap() {
        sequenceGapCount.increment();
    }

    /**
     * 累加一次首次等待撤单确认。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加；撤单登记幂等边界保证同一订单只计首次迁移。
     */
    public void pendingCancel() {
        pendingCancelCount.increment();
    }

    /**
     * 累加一次过期撤单确认。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加，可由多个订单入口并发调用。
     */
    public void staleCancelConfirmation() {
        staleCancelConfirmationCount.increment();
    }

    /**
     * 累加一次确定拒绝成交；序号占用冲突拒绝不代表双方序号已被消费。
     *
     * @note 使用 {@link LongAdder} 无全局锁累加；成交记录终态竞争保证同一拒绝只计一次。
     */
    public void tradeRejected() {
        tradeRejectedCount.increment();
    }

    /**
     * 获取部分成交次数。
     *
     * @return 部分成交次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long partialFillCount() {
        return partialFillCount.sum();
    }

    /**
     * 获取成功双边结算次数。
     *
     * @return 成功双边结算次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long settledTradeCount() {
        return settledTradeCount.sum();
    }

    /**
     * 获取精确重复成交投递次数。
     *
     * @return 精确重复成交投递次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long duplicateTradeCount() {
        return duplicateTradeCount.sum();
    }

    /**
     * 获取当前挂起成交记录数。
     *
     * @return 当前挂起成交记录数
     * @note 通过原子读取返回单值一致结果，不与其他指标组成原子快照。
     */
    public int pendingTradeCount() {
        return pendingTradeCount.get();
    }

    /**
     * 获取成交元数据冲突次数。
     *
     * @return 成交元数据冲突次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long tradeMetadataConflictCount() {
        return tradeMetadataConflictCount.sum();
    }

    /**
     * 获取权威序号空洞次数。
     *
     * @return 权威序号空洞次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long sequenceGapCount() {
        return sequenceGapCount.sum();
    }

    /**
     * 获取首次等待撤单确认次数。
     *
     * @return 首次等待撤单确认次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long pendingCancelCount() {
        return pendingCancelCount.sum();
    }

    /**
     * 获取过期撤单确认次数。
     *
     * @return 过期撤单确认次数
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long staleCancelConfirmationCount() {
        return staleCancelConfirmationCount.sum();
    }

    /**
     * 获取确定拒绝成交次数。
     *
     * @return 确定拒绝成交次数，包含不消费双方序号的权威序号占用冲突
     * @note 并发读取为弱一致快照，不阻塞正在执行的累计写入。
     */
    public long tradeRejectedCount() {
        return tradeRejectedCount.sum();
    }
}
