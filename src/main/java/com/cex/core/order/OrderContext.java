package com.cex.core.order;

import com.cex.core.risk.RiskDecision;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单个订单的不可变元数据、权威序号、累计成交量与临时旧版事实上下文。
 *
 * <p>核心能力：保存强类型订单状态并支持有界乱序事件、两阶段成交和撤单提交。</p>
 * <p>线程安全：状态对无锁读者可见；除旧版事实位外的可变字段必须由所属用户锁保护。</p>
 * <p>使用限制：上下文本身不修改资金；旧版事实与副作用适配器将在类型化迁移完成后删除。</p>
 */
public final class OrderContext {

    private static final AssetId LEGACY_BASE = new AssetId("LEGACY");
    private static final AssetId LEGACY_QUOTE = new AssetId("LEGACYQ");
    private static final TradingPair LEGACY_PAIR = new TradingPair(LEGACY_BASE, LEGACY_QUOTE);
    private static final NavigableMap<Long, SequencedOrderEvent> LEGACY_PENDING_EVENTS =
            Collections.emptyNavigableMap();

    private final long orderId;
    private final long userId;
    /** 创建本强类型上下文的完整不可变载荷；旧版适配上下文为 {@code null}。 */
    private final OrderSubmission originalSubmission;
    private final OrderSide side;
    private final TradingPair pair;
    private final long originalBaseQuantity;
    private final long originalReservedAmount;
    private final long riskQuoteAmount;
    private final long amount;
    private final AtomicInteger factBits = new AtomicInteger();

    private int effectBits;
    private volatile OrderStatus status;
    private long cumulativeBaseFilled;
    private long cumulativeQuoteFilled;
    private long remainingBaseQuantity;
    private long remainingReservedAmount;
    private long lastAppliedSequence;
    private final NavigableMap<Long, SequencedOrderEvent> pendingEvents;
    /** 已登记的稳定撤单请求标识；尚未登记时为零。 */
    private long cancelRequestId;
    /** 撤单请求在本地登记与外部发送之间的用户锁内交付状态。 */
    private CancelRequestDeliveryState cancelRequestDeliveryState =
            CancelRequestDeliveryState.NOT_REGISTERED;
    private boolean terminalConflictRecorded;
    private boolean approvalConflictRecorded;

    /**
     * 创建强类型订单上下文。
     *
     * @param submission 已通过输入边界校验的订单提交
     */
    private OrderContext(OrderSubmission submission) {
        this.orderId = submission.orderId();
        this.userId = submission.userId();
        this.originalSubmission = submission;
        this.side = submission.side();
        this.pair = submission.pair();
        this.originalBaseQuantity = submission.baseQuantity();
        this.originalReservedAmount = submission.reservedAmount();
        this.riskQuoteAmount = submission.riskQuoteAmount();
        this.amount = submission.reservedAmount();
        this.status = OrderStatus.NEW;
        this.remainingBaseQuantity = submission.baseQuantity();
        this.remainingReservedAmount = submission.reservedAmount();
        this.lastAppliedSequence = submission.orderSequence();
        this.pendingEvents = new TreeMap<>();
    }

    /**
     * 创建旧版通用事件适配上下文。
     *
     * @param orderId 全局唯一订单ID
     * @param userId 订单归属用户ID
     * @param amount 旧版标量订单金额
     */
    private OrderContext(long orderId, long userId, long amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.originalSubmission = null;
        this.side = OrderSide.BUY;
        this.pair = LEGACY_PAIR;
        this.originalBaseQuantity = amount;
        this.originalReservedAmount = amount;
        this.riskQuoteAmount = amount;
        this.amount = amount;
        this.status = OrderStatus.INIT;
        this.remainingBaseQuantity = amount;
        this.remainingReservedAmount = amount;
        this.pendingEvents = LEGACY_PENDING_EVENTS;
    }

    /**
     * 依据强类型提交建立处于 {@link OrderStatus#NEW} 的订单上下文。
     *
     * @param submission 已通过值对象校验的订单提交，不能为空
     * @return 保存提交元数据、初始剩余量和起始权威序号的新上下文
     * @throws NullPointerException 当提交为 {@code null} 时抛出
     * @note 创建只初始化订单内存状态，不冻结资金；调用方须在同一用户锁内完成后续风险和资金步骤。
     */
    public static OrderContext fromSubmission(OrderSubmission submission) {
        return new OrderContext(Objects.requireNonNull(submission, "submission"));
    }

    /**
     * 依据首个旧版事件建立订单上下文。
     *
     * @param firstEvent 首次到达的旧版订单事件，不能为空
     * @return 持有该事件元数据的新订单上下文
     * @throws NullPointerException 当事件为 {@code null} 时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用；类型化入口完成后删除
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public static OrderContext fromFirstEvent(OrderEvent firstEvent) {
        Objects.requireNonNull(firstEvent, "firstEvent");
        return new OrderContext(firstEvent.orderId(), firstEvent.userId(), firstEvent.amount());
    }

    /** @return 不可变订单唯一标识 */
    public long orderId() { return orderId; }

    /** @return 不可变用户唯一标识 */
    public long userId() { return userId; }

    /** @return 决定冻结资产类型的订单方向 */
    public OrderSide side() { return side; }

    /** @return 不可变基础/报价资产对 */
    public TradingPair pair() { return pair; }

    /** @return 严格为正的原始基础资产数量 */
    public long originalBaseQuantity() { return originalBaseQuantity; }

    /** @return 买单报价资产或卖单基础资产的原始冻结量 */
    public long originalReservedAmount() { return originalReservedAmount; }

    /** @return 严格为正的报价资产风控名义金额 */
    public long riskQuoteAmount() { return riskQuoteAmount; }

    /**
     * @return 非负且不大于原始基础数量的累计值
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long cumulativeBaseFilled() { return cumulativeBaseFilled; }

    /**
     * @return 非负的权威累计报价资产数量
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long cumulativeQuoteFilled() { return cumulativeQuoteFilled; }

    /**
     * @return 原始基础数量减累计基础成交量
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long remainingBaseQuantity() { return remainingBaseQuantity; }

    /**
     * @return 活动买单的剩余报价资产或活动卖单的剩余基础资产；终态为零
     * @note 最终买单未花费报价资产通过成交变更单独释放，本字段提交后归零。
     */
    public long remainingReservedAmount() { return remainingReservedAmount; }

    /**
     * @return 创建、成交或撤单确认最后提交的序号
     * @note 读取和推进必须在所属用户锁内进行，防止跳过序号空洞。
     */
    public long lastAppliedSequence() { return lastAppliedSequence; }

    /**
     * @return 严格为正的撤单请求标识，尚未请求时为零
     * @note 读取和写入必须由所属用户锁保护。
     */
    public long cancelRequestId() { return cancelRequestId; }

    /**
     * @return 旧版事件携带的金额
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public long amount() { return amount; }

    /** @return 当前可见订单状态 */
    public OrderStatus status() { return status; }

    /**
     * 校验候选强类型提交与创建本上下文的原始载荷完全一致。
     *
     * @param submission 候选订单提交，不能为空
     * @throws NullPointerException 当提交为 {@code null} 时抛出
     * @throws OrderMetadataMismatchException 当上下文来自旧版事件或任一提交组件不一致时抛出
     * @note 比较使用不可变原始提交，不依赖会随成交推进的剩余量、状态或最后序号。
     */
    void validateSubmission(OrderSubmission submission) {
        Objects.requireNonNull(submission, "submission");
        if (originalSubmission == null || !originalSubmission.equals(submission)) {
            throw new OrderMetadataMismatchException(
                    "order submission metadata mismatch for orderId=" + orderId);
        }
    }

    /**
     * 在首次发布前应用创建阶段的最小风控分类。
     *
     * @param riskDecision 创建阶段风控结论，不能为空
     * @throws NullPointerException 当风控结论为 {@code null} 时抛出
     * @throws IllegalStateException 当上下文不是尚未发布的强类型新单时抛出
     * @note 调用方必须持有用户锁；本方法只在资金冻结成功后、订单发布前写入 {@code NEW/RISK_HOLD}。
     */
    void classifyInitialRiskLocked(RiskDecision riskDecision) {
        Objects.requireNonNull(riskDecision, "riskDecision");
        if (originalSubmission == null || status != OrderStatus.NEW) {
            throw new IllegalStateException("initial risk classification requires a typed NEW order");
        }
        status = riskDecision == RiskDecision.HOLD
                ? OrderStatus.RISK_HOLD
                : OrderStatus.NEW;
    }

    /**
     * 将已通过审批的强类型暂挂订单恢复为新单。
     *
     * @return 本次发生 {@code RISK_HOLD -> NEW} 迁移时为 {@code true}
     * @note 调用方必须持有用户锁；拒绝审批的撤单请求生成由后续风险集成任务负责。
     */
    boolean approveRiskHoldLocked() {
        if (originalSubmission != null && status == OrderStatus.RISK_HOLD) {
            status = OrderStatus.NEW;
            return true;
        }
        return false;
    }

    /**
     * 为旧版订单适配器建立强类型审批输入，强类型订单直接返回原始提交。
     *
     * @param submittedAtMillis 旧版适配提交使用的非负毫秒时间戳
     * @return 可交给强类型审批服务的不可变订单提交
     * @deprecated 仅供旧版 {@link OrderEngine#process(OrderEvent)} 迁移期使用
     */
    @Deprecated(since = "typed-approval", forRemoval = true)
    OrderSubmission approvalSubmission(long submittedAtMillis) {
        if (originalSubmission != null) {
            return originalSubmission;
        }
        return new OrderSubmission(
                orderId,
                userId,
                side,
                pair,
                originalBaseQuantity,
                originalReservedAmount,
                riskQuoteAmount,
                1L,
                submittedAtMillis);
    }

    /**
     * 为风控拒绝派生稳定撤单请求。
     *
     * @return 同一暂挂订单始终相等且请求 ID 相同的撤单请求
     * @throws IllegalStateException 当订单不是强类型风控暂挂单或由该请求进入的等待撤单状态时抛出
     * @note 调用方必须持有用户锁；请求 ID 由订单 ID 双射派生，请求时间固定取原始提交时间，发送失败后的重试因此复用完全相同的载荷。
     * @note 不在每个订单上下文保存额外请求引用，避免 256MB 堆下旧版大规模兼容测试为从不触发的风险撤单支付常驻内存。
     */
    CancelRequest riskCancelRequestLocked() {
        long derivedRequestId = derivedRiskCancelRequestId();
        if (status != OrderStatus.RISK_HOLD
                && (status != OrderStatus.PENDING_CANCEL
                || cancelRequestId != derivedRequestId)) {
            throw new IllegalStateException("risk cancellation requires a held typed order");
        }
        return new CancelRequest(
                derivedRequestId, orderId, originalSubmission.submittedAtMillis());
    }

    /**
     * 判断当前等待撤单状态是否由稳定风险请求建立。
     *
     * @return 当前请求标识等于该订单派生风险请求标识时为 {@code true}
     * @note 调用方必须持有用户锁。
     */
    boolean hasRiskCancelRequestLocked() {
        return originalSubmission != null
                && cancelRequestId != 0L
                && cancelRequestId == derivedRiskCancelRequestId();
    }

    /**
     * 返回订单标识双射派生的正数风险撤单请求标识。
     *
     * @return 与订单一一对应的正数风险撤单请求 ID
     * @throws IllegalStateException 当上下文不是强类型订单时抛出
     */
    private long derivedRiskCancelRequestId() {
        if (originalSubmission == null) {
            throw new IllegalStateException("risk cancellation requires a typed order");
        }
        return Long.MAX_VALUE - orderId + 1L;
    }

    /**
     * 在迁移期旧版引擎的用户锁内更新兼容状态。
     *
     * @param status 目标旧版状态，不能为空
     * @throws NullPointerException 当目标状态为 {@code null} 时抛出
     * @throws IllegalStateException 当强类型上下文尝试绕过订单状态机时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用；强类型上下文必须通过状态机变更
     * @note 方法限制在订单包内；volatile 写只提供可见性，调用方仍须持有所属用户锁。
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    void setLegacyStatusLocked(OrderStatus status) {
        if (pendingEvents != LEGACY_PENDING_EVENTS) {
            throw new IllegalStateException("typed order status must use OrderStateMachine");
        }
        this.status = Objects.requireNonNull(status, "status");
    }

    SequencedOrderEvent pendingEventLocked(long sequence) {
        return pendingEvents.get(sequence);
    }

    int pendingEventCountLocked() { return pendingEvents.size(); }

    /**
     * 提交已预计算的事件登记变更。
     *
     * @param mutation 与本上下文绑定且已完成全部校验的登记变更
     * @note 方法只执行预计算插入或无操作，不重新校验身份、序号、冲突和容量。
     */
    void commitEventRegistrationLocked(OrderEventRegistrationMutation mutation) {
        if (mutation.insertsEvent()) {
            SequencedOrderEvent event = mutation.event();
            pendingEvents.put(event.orderSequence(), event);
        }
    }

    void commitFillLocked(OrderFillMutation mutation) {
        cumulativeBaseFilled = mutation.cumulativeBaseFilled();
        cumulativeQuoteFilled = mutation.cumulativeQuoteFilled();
        remainingBaseQuantity = mutation.remainingBaseQuantity();
        remainingReservedAmount = mutation.remainingReservedAmount();
        lastAppliedSequence = mutation.orderSequence();
        pendingEvents.remove(mutation.orderSequence());
        status = mutation.status();
    }

    void commitCancelLocked(OrderCancelMutation mutation) {
        remainingReservedAmount = 0L;
        lastAppliedSequence = mutation.orderSequence();
        pendingEvents.remove(mutation.orderSequence());
        status = mutation.status();
    }

    void commitPreparedSequenceLocked(long orderSequence) {
        lastAppliedSequence = orderSequence;
        pendingEvents.remove(orderSequence);
    }

    void startCancelLocked(long requestId) {
        cancelRequestId = requestId;
        cancelRequestDeliveryState = CancelRequestDeliveryState.REGISTERED;
        status = OrderStatus.PENDING_CANCEL;
    }

    /**
     * 尝试独占同一撤单请求的下一次外部发送。
     *
     * @param requestId 必须等于已登记标识的撤单请求 ID
     * @return 从 {@code REGISTERED} 成功迁移为 {@code SENDING} 时为 {@code true}；正在发送或已发送时为 {@code false}
     * @throws IllegalArgumentException 当请求标识与已登记值不一致时抛出
     * @note 调用方必须持有订单所属用户锁；只有返回 {@code true} 的线程才能在释放锁后调用外部 sink。
     */
    boolean tryStartCancelRequestDeliveryLocked(long requestId) {
        requireRegisteredCancelRequestId(requestId);
        if (cancelRequestDeliveryState != CancelRequestDeliveryState.REGISTERED) {
            return false;
        }
        cancelRequestDeliveryState = CancelRequestDeliveryState.SENDING;
        return true;
    }

    /**
     * 提交已成功完成的外部撤单请求发送。
     *
     * @param requestId 必须等于当前正在发送的撤单请求 ID
     * @throws IllegalArgumentException 当请求标识与已登记值不一致时抛出
     * @throws IllegalStateException 当请求当前不处于发送中时抛出
     * @note 调用方必须在外部 sink 返回成功后重新获取用户锁再调用本方法。
     */
    void completeCancelRequestDeliveryLocked(long requestId) {
        requireSendingCancelRequest(requestId);
        cancelRequestDeliveryState = CancelRequestDeliveryState.SENT;
    }

    /**
     * 回滚失败的外部撤单请求发送，使相同请求标识可以再次尝试。
     *
     * @param requestId 必须等于当前正在发送的撤单请求 ID
     * @throws IllegalArgumentException 当请求标识与已登记值不一致时抛出
     * @throws IllegalStateException 当请求当前不处于发送中时抛出
     * @note 调用方必须在外部 sink 抛出后重新获取用户锁再调用本方法；订单状态继续保持 {@code PENDING_CANCEL}。
     */
    void failCancelRequestDeliveryLocked(long requestId) {
        requireSendingCancelRequest(requestId);
        cancelRequestDeliveryState = CancelRequestDeliveryState.REGISTERED;
    }

    /**
     * 校验请求标识与当前已登记撤单一致。
     *
     * @param requestId 候选撤单请求标识
     * @throws IllegalArgumentException 当尚未登记或标识不一致时抛出
     */
    private void requireRegisteredCancelRequestId(long requestId) {
        if (cancelRequestId == 0L || cancelRequestId != requestId) {
            throw new IllegalArgumentException(
                    "cancel request ID mismatch for orderId=" + orderId);
        }
    }

    /**
     * 校验请求标识一致且交付状态为发送中。
     *
     * @param requestId 候选撤单请求标识
     * @throws IllegalArgumentException 当标识不一致时抛出
     * @throws IllegalStateException 当交付状态不是发送中时抛出
     */
    private void requireSendingCancelRequest(long requestId) {
        requireRegisteredCancelRequestId(requestId);
        if (cancelRequestDeliveryState != CancelRequestDeliveryState.SENDING) {
            throw new IllegalStateException(
                    "cancel request is not being delivered for orderId=" + orderId);
        }
    }

    /**
     * 校验后续旧版事件未改变订单的身份和金额元数据。
     *
     * @param event 待校验事件，不能为空
     * @throws NullPointerException 当事件为 {@code null} 时抛出
     * @throws OrderMetadataMismatchException 当订单 ID、用户 ID 或金额不一致时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public void validateMetadata(OrderEvent event) {
        Objects.requireNonNull(event, "event");
        if (orderId != event.orderId() || userId != event.userId() || amount != event.amount()) {
            throw new OrderMetadataMismatchException(
                    "order metadata mismatch for orderId=" + orderId);
        }
    }

    /**
     * 原子登记旧版事件对应的事实位。
     *
     * @param eventType 待登记的事件类型，不能为空
     * @return 首次成功置位时为 {@link FactRegistrationResult#NEW}，否则为重复结果
     * @throws NullPointerException 当事件类型为 {@code null} 时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public FactRegistrationResult registerFact(OrderEventType eventType) {
        int mask = OrderFact.fromEventType(Objects.requireNonNull(eventType, "eventType")).mask();
        while (true) {
            int current = factBits.get();
            if ((current & mask) != 0) {
                return FactRegistrationResult.DUPLICATE;
            }
            int updated = current | mask;
            if (factBits.compareAndSet(current, updated)) {
                return FactRegistrationResult.NEW;
            }
        }
    }

    /**
     * 判断某类旧版事件事实是否已缓存。
     *
     * @param fact 待查询事实，不能为空
     * @return 已登记该事实时为 {@code true}
     * @throws NullPointerException 当事实为 {@code null} 时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public boolean hasFact(OrderFact fact) {
        return (factBits.get() & Objects.requireNonNull(fact, "fact").mask()) != 0;
    }

    /**
     * 在用户锁内判断旧版副作用是否已提交。
     *
     * @param effect 待查询副作用，不能为空
     * @return 副作用已成功提交时为 {@code true}
     * @throws NullPointerException 当副作用为 {@code null} 时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public boolean hasEffect(OrderEffect effect) {
        return (effectBits & Objects.requireNonNull(effect, "effect").mask()) != 0;
    }

    /**
     * 在用户锁内执行一次旧版副作用，并在成功后提交幂等标记。
     *
     * @param effect 待提交的副作用标记，不能为空
     * @param operation 实际副作用操作，不能为空
     * @return 本次执行并提交标记时为 {@code true}；已提交时为 {@code false}
     * @throws NullPointerException 当副作用或操作为 {@code null} 时抛出
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public boolean applyEffectLocked(OrderEffect effect, LockedEffectOperation operation) {
        Objects.requireNonNull(effect, "effect");
        Objects.requireNonNull(operation, "operation");
        if (hasEffect(effect)) {
            return false;
        }
        operation.run();
        effectBits |= effect.mask();
        return true;
    }

    /**
     * @return 首次标记旧版终态冲突时为 {@code true}，否则为 {@code false}
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public boolean markTerminalConflictLocked() {
        if (terminalConflictRecorded) {
            return false;
        }
        terminalConflictRecorded = true;
        return true;
    }

    /**
     * @return 首次标记旧版审批冲突时为 {@code true}，否则为 {@code false}
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    public boolean markApprovalConflictLocked() {
        if (approvalConflictRecorded) {
            return false;
        }
        approvalConflictRecorded = true;
        return true;
    }

    /**
     * 单个撤单请求从本地登记到外部发送完成的互斥状态。
     *
     * <p>线程安全：字段只在订单所属用户锁内读取和迁移。</p>
     * <p>使用限制：状态不表示外部撤单已确认；确认仍由 {@link CancelConfirmation} 独立推进订单序号。</p>
     */
    private enum CancelRequestDeliveryState {
        /** 尚未登记本地撤单请求。 */
        NOT_REGISTERED,
        /** 已登记但当前没有线程执行外部发送，可由同 ID 调用重试。 */
        REGISTERED,
        /** 一个线程已独占发送权并正在用户锁外调用外部 sink。 */
        SENDING,
        /** 外部 sink 已成功返回，相同请求后续只返回幂等结果。 */
        SENT
    }

    /**
     * 由调用方在持有用户锁时执行的旧版副作用操作。
     *
     * <p>线程安全：实现由调用方的同一用户锁保护。</p>
     * <p>使用限制：异常时对应旧版副作用位不会提交。</p>
     *
     * @deprecated 仅供旧版 {@link OrderEngine} 迁移期使用
     */
    @Deprecated(since = "typed-order-state", forRemoval = true)
    @FunctionalInterface
    public interface LockedEffectOperation {
        /** 执行一次副作用操作。 */
        void run();
    }
}
