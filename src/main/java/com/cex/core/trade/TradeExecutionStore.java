package com.cex.core.trade;

import com.cex.core.order.TradeExecution;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 有界、无逐出的成交标识幂等存储。
 *
 * <p>核心能力：为每个 {@code tradeId} 固化唯一权威载荷，保留终态记录，并按订单索引仍待处理的成交。</p>
 * <p>线程安全：使用并发映射、CAS 容量预留和每个成交标识独立的登记协调，不使用存储级全局锁。</p>
 * <p>使用限制：这是进程内的易失性存储，不提供持久化、淘汰或跨进程一致性。</p>
 */
public final class TradeExecutionStore {
    /** 默认最大挂起成交记录数。 */
    public static final int DEFAULT_MAX_PENDING_RECORDS = 50_000;
    /** 默认最大总成交记录数，包含保留的终态记录。 */
    public static final int DEFAULT_MAX_TOTAL_RECORDS = 250_000;

    /** 固定的挂起记录容量。 */
    private final int maxPendingRecords;
    /** 固定的总记录容量。 */
    private final int maxTotalRecords;
    /** 按成交标识固化的权威记录。 */
    private final ConcurrentMap<Long, TradeExecutionRecord> records = new ConcurrentHashMap<>();
    /** 按订单标识索引的挂起成交标识集合。 */
    private final ConcurrentMap<Long, Set<Long>> pendingTradeIdsByOrder = new ConcurrentHashMap<>();
    /** 按成交标识协调尚未完成登记的调用，防止容量满时并发重复误判为新成交。 */
    private final ConcurrentMap<Long, Registration> registrations = new ConcurrentHashMap<>();
    /** 已预留或已发布的挂起记录数。 */
    private final AtomicInteger pendingRecords = new AtomicInteger();
    /** 已预留或已发布的总记录数。 */
    private final AtomicInteger totalRecords = new AtomicInteger();

    /**
     * 使用引擎默认容量创建成交存储。
     */
    public TradeExecutionStore() {
        this(DEFAULT_MAX_PENDING_RECORDS, DEFAULT_MAX_TOTAL_RECORDS);
    }

    /**
     * 使用固定且不可变的容量创建成交存储。
     *
     * @param maxPendingRecords 最大挂起成交记录数，必须为正且不大于总容量
     * @param maxTotalRecords 最大总成交记录数，必须为正
     * @throws IllegalArgumentException 当容量不为正或挂起容量大于总容量时抛出
     */
    public TradeExecutionStore(int maxPendingRecords, int maxTotalRecords) {
        if (maxPendingRecords <= 0 || maxTotalRecords <= 0 || maxPendingRecords > maxTotalRecords) {
            throw new IllegalArgumentException("pending and total capacities must be positive and pending <= total");
        }
        this.maxPendingRecords = maxPendingRecords;
        this.maxTotalRecords = maxTotalRecords;
    }

    /**
     * 登记成交，或识别同一成交标识的精确重复。
     *
     * @param execution 待登记的权威成交，不能为空
     * @return 新建记录或已存在的同一记录实例
     * @throws TradeMetadataMismatchException 当相同成交标识载荷不同
     * @throws PendingCapacityExceededException 当新成交标识超过挂起或总记录容量
     * @note 先检查已发布记录；并发同 ID 调用由独立登记槽协调，保证精确重复即使容量已满也返回原记录，竞争失败的容量预留会回滚。
     */
    public TradeExecutionRecord register(TradeExecution execution) {
        Objects.requireNonNull(execution, "execution");
        long tradeId = execution.tradeId();
        for (;;) {
            TradeExecutionRecord existing = records.get(tradeId);
            if (existing != null) {
                return sameOrConflict(existing, execution);
            }

            Registration candidate = new Registration(execution);
            Registration inProgress = registrations.putIfAbsent(tradeId, candidate);
            if (inProgress != null) {
                if (!inProgress.hasSameExecution(execution)) {
                    throw metadataMismatch(tradeId);
                }
                awaitRegistration(tradeId, inProgress);
                continue;
            }

            try {
                return registerOwned(candidate);
            } finally {
                registrations.remove(tradeId, candidate);
            }
        }
    }

    /**
     * 查询已登记成交记录。
     *
     * @param tradeId 成交标识
     * @return 对应记录；尚未登记时为 {@code null}
     */
    public TradeExecutionRecord record(long tradeId) {
        return records.get(tradeId);
    }

    /**
     * 返回关联订单仍处于挂起状态的成交标识快照。
     *
     * @param orderId 订单标识
     * @return 不可修改的挂起成交标识快照
     * @note 返回前复核记录状态，避免调用方在终态索引清理的并发短窗口中看到已终结成交。
     */
    public Collection<Long> pendingTradeIds(long orderId) {
        Set<Long> tradeIds = pendingTradeIdsByOrder.get(orderId);
        if (tradeIds == null || tradeIds.isEmpty()) {
            return List.of();
        }
        return tradeIds.stream()
                .filter(tradeId -> isPending(tradeId))
                .toList();
    }

    /**
     * 将指定挂起成交标记为已结算。
     *
     * @param tradeId 已登记成交标识
     * @param completedAtMillis 非负的完成毫秒时间戳
     * @throws IllegalArgumentException 当成交不存在或完成时间为负时抛出
     * @note 只有赢得记录本地终态迁移的调用才会删除双方订单索引并释放一个挂起容量，重复终态调用不产生副作用。
     */
    public void markSettled(long tradeId, long completedAtMillis) {
        TradeExecutionRecord record = requireRecord(tradeId);
        if (record.markSettled(completedAtMillis)) {
            removePendingIndexes(record);
            pendingRecords.decrementAndGet();
        }
    }

    /**
     * 将指定挂起成交标记为已拒绝。
     *
     * @param tradeId 已登记成交标识
     * @param reason 非空的确定拒绝原因
     * @param completedAtMillis 非负的完成毫秒时间戳
     * @throws IllegalArgumentException 当成交不存在或完成时间为负时抛出
     * @throws NullPointerException 当拒绝原因为 {@code null} 时抛出
     * @note 只有赢得记录本地终态迁移的调用才会删除双方订单索引并释放一个挂起容量，重复终态调用不产生副作用。
     */
    public void markRejected(long tradeId, String reason, long completedAtMillis) {
        TradeExecutionRecord record = requireRecord(tradeId);
        if (record.markRejected(reason, completedAtMillis)) {
            removePendingIndexes(record);
            pendingRecords.decrementAndGet();
        }
    }

    /**
     * 返回当前已预留或已发布的挂起记录数。
     *
     * @return 不大于配置挂起容量的计数
     */
    public int pendingCount() {
        return pendingRecords.get();
    }

    /**
     * 返回当前已预留或已发布的总记录数。
     *
     * @return 不大于配置总容量的计数，包含终态记录
     */
    public int totalCount() {
        return totalRecords.get();
    }

    /**
     * 由当前线程完成其独占登记槽的容量预留和发布。
     *
     * @param registration 当前成交标识的独占登记槽
     * @return 新建记录或极端竞争下已有的精确重复记录
     */
    private TradeExecutionRecord registerOwned(Registration registration) {
        long tradeId = registration.execution.tradeId();
        if (!tryReserve(totalRecords, maxTotalRecords)) {
            throw capacityExceeded("total trade record capacity exceeded");
        }
        boolean totalReserved = true;
        boolean pendingReserved = false;
        boolean published = false;
        try {
            if (!tryReserve(pendingRecords, maxPendingRecords)) {
                throw capacityExceeded("pending trade record capacity exceeded");
            }
            pendingReserved = true;

            TradeExecutionRecord candidate = new TradeExecutionRecord(registration.execution);
            TradeExecutionRecord existing;
            synchronized (candidate) {
                existing = records.putIfAbsent(tradeId, candidate);
                if (existing == null) {
                    addPendingIndexes(candidate);
                    published = true;
                }
            }
            if (published) {
                return candidate;
            }
            return sameOrConflict(existing, registration.execution);
        } finally {
            if (!published) {
                if (pendingReserved) {
                    pendingRecords.decrementAndGet();
                }
                if (totalReserved) {
                    totalRecords.decrementAndGet();
                }
            }
        }
    }

    /**
     * 在另一个线程发布或放弃同一成交标识时进行短暂自旋等待。
     *
     * @param tradeId 等待的成交标识
     * @param registration 当前观察到的登记槽
     */
    private void awaitRegistration(long tradeId, Registration registration) {
        while (registrations.get(tradeId) == registration) {
            Thread.onSpinWait();
        }
    }

    /**
     * 将候选成交与固化记录比较并返回精确重复。
     *
     * @param existing 已固化记录
     * @param execution 候选成交
     * @return 已固化记录
     */
    private TradeExecutionRecord sameOrConflict(TradeExecutionRecord existing, TradeExecution execution) {
        if (!existing.hasSameExecution(execution)) {
            throw metadataMismatch(execution.tradeId());
        }
        return existing;
    }

    /**
     * 为已发布的挂起记录加入双方订单索引。
     *
     * @param record 已发布的挂起记录
     */
    private void addPendingIndexes(TradeExecutionRecord record) {
        long tradeId = record.execution().tradeId();
        addPendingIndex(record.execution().buyOrderId(), tradeId);
        addPendingIndex(record.execution().sellOrderId(), tradeId);
    }

    /**
     * 为一个订单加入挂起成交标识。
     *
     * @param orderId 订单标识
     * @param tradeId 成交标识
     */
    private void addPendingIndex(long orderId, long tradeId) {
        pendingTradeIdsByOrder.compute(orderId, (ignored, tradeIds) -> {
            Set<Long> result = tradeIds == null ? ConcurrentHashMap.newKeySet() : tradeIds;
            result.add(tradeId);
            return result;
        });
    }

    /**
     * 从双方订单索引删除终态成交标识。
     *
     * @param record 已进入终态的成交记录
     */
    private void removePendingIndexes(TradeExecutionRecord record) {
        long tradeId = record.execution().tradeId();
        removePendingIndex(record.execution().buyOrderId(), tradeId);
        removePendingIndex(record.execution().sellOrderId(), tradeId);
    }

    /**
     * 从一个订单索引删除成交标识，并在集合为空时删除索引键。
     *
     * @param orderId 订单标识
     * @param tradeId 成交标识
     */
    private void removePendingIndex(long orderId, long tradeId) {
        pendingTradeIdsByOrder.computeIfPresent(orderId, (ignored, tradeIds) -> {
            tradeIds.remove(tradeId);
            return tradeIds.isEmpty() ? null : tradeIds;
        });
    }

    /**
     * 尝试以 CAS 保留一个固定容量名额。
     *
     * @param counter 当前预留计数
     * @param maximum 固定容量上限
     * @return 成功保留名额时为 {@code true}
     */
    private static boolean tryReserve(AtomicInteger counter, int maximum) {
        for (;;) {
            int current = counter.get();
            if (current >= maximum) {
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * 查询一个索引成员在读取时是否仍为挂起。
     *
     * @param tradeId 成交标识
     * @return 对应记录存在且仍为挂起时为 {@code true}
     */
    private boolean isPending(long tradeId) {
        TradeExecutionRecord record = records.get(tradeId);
        return record != null && record.state() == TradeExecutionState.PENDING;
    }

    /**
     * 获取必须已经登记的成交记录。
     *
     * @param tradeId 成交标识
     * @return 已登记成交记录
     */
    private TradeExecutionRecord requireRecord(long tradeId) {
        TradeExecutionRecord record = records.get(tradeId);
        if (record == null) {
            throw new IllegalArgumentException("unknown tradeId=" + tradeId);
        }
        return record;
    }

    /**
     * 创建与成交标识绑定的元数据冲突异常。
     *
     * @param tradeId 发生冲突的成交标识
     * @return 新建异常
     */
    private static TradeMetadataMismatchException metadataMismatch(long tradeId) {
        return new TradeMetadataMismatchException("trade metadata mismatch for tradeId=" + tradeId);
    }

    /**
     * 创建容量背压异常。
     *
     * @param message 容量不足的说明
     * @return 新建异常
     */
    private static PendingCapacityExceededException capacityExceeded(String message) {
        return new PendingCapacityExceededException(message);
    }

    /**
     * 单一成交标识尚未发布期间的独占登记信息。
     *
     * <p>核心能力：让并发精确重复等待首个调用完成发布或回滚容量预留。</p>
     * <p>线程安全：不可变成交载荷通过并发映射安全发布。</p>
     * <p>使用限制：只在 {@link TradeExecutionStore#register(TradeExecution)} 的极短登记窗口存在。</p>
     */
    private static final class Registration {
        /** 正在登记的不可变成交载荷。 */
        private final TradeExecution execution;

        /**
         * 创建登记信息。
         *
         * @param execution 正在登记的成交载荷
         */
        private Registration(TradeExecution execution) {
            this.execution = execution;
        }

        /**
         * 判断候选成交是否与正在登记的成交相同。
         *
         * @param candidate 待比较的候选成交
         * @return 所有成交记录组件均相同时为 {@code true}
         */
        private boolean hasSameExecution(TradeExecution candidate) {
            return execution.equals(candidate);
        }
    }
}
