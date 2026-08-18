package com.cex.core.order;

import com.cex.core.risk.RiskDecision;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/**
 * 单个订单的不可变元数据、权威序号与累计成交量上下文。
 *
 * <p>核心能力：保存强类型订单状态并支持有界乱序事件、两阶段成交和撤单提交。</p>
 * <p>线程安全：状态对无锁读者可见；全部可变字段必须由所属用户锁保护。</p>
 * <p>使用限制：上下文本身不修改资金，资金变更由账本在相同用户锁内提交。</p>
 */
public final class OrderContext {

    /** 订单唯一标识。 */
    private final long orderId;
    /** 订单归属用户标识。 */
    private final long userId;
    /** 创建本上下文的完整不可变提交载荷。 */
    private final OrderSubmission originalSubmission;
    /** 决定冻结资产的买卖方向。 */
    private final OrderSide side;
    /** 基础资产与报价资产交易对。 */
    private final TradingPair pair;
    /** 原始基础资产委托数量。 */
    private final long originalBaseQuantity;
    /** 原始报价或基础资产冻结量。 */
    private final long originalReservedAmount;
    /** 上游提供的报价资产风控名义金额。 */
    private final long riskQuoteAmount;
    /** 当前可见订单状态。 */
    private volatile OrderStatus status;
    /** 已成交基础资产累计量。 */
    private long cumulativeBaseFilled;
    /** 已成交报价资产累计量。 */
    private long cumulativeQuoteFilled;
    /** 尚未成交的基础资产数量。 */
    private long remainingBaseQuantity;
    /** 活动订单尚在冻结的资产数量。 */
    private long remainingReservedAmount;
    /** 最后成功提交的权威订单序号。 */
    private long lastAppliedSequence;
    /** 按权威序号排列的待处理输入。 */
    private final NavigableMap<Long, SequencedOrderEvent> pendingEvents;
    /** 已登记的稳定撤单请求标识；尚未登记时为零。 */
    private long cancelRequestId;
    /** 撤单请求在本地登记与外部发送之间的用户锁内交付状态。 */
    private CancelRequestDeliveryState cancelRequestDeliveryState =
            CancelRequestDeliveryState.NOT_REGISTERED;

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
        this.status = OrderStatus.NEW;
        this.remainingBaseQuantity = submission.baseQuantity();
        this.remainingReservedAmount = submission.reservedAmount();
        this.lastAppliedSequence = submission.orderSequence();
        this.pendingEvents = new TreeMap<>();
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
     * 获取不可变订单唯一标识。
     *
     * @return 不可变订单唯一标识
     */
    public long orderId() { return orderId; }

    /**
     * 获取不可变用户唯一标识。
     *
     * @return 不可变用户唯一标识
     */
    public long userId() { return userId; }

    /**
     * 获取决定冻结资产类型的订单方向。
     *
     * @return 买入或卖出方向
     */
    public OrderSide side() { return side; }

    /**
     * 获取不可变基础资产与报价资产交易对。
     *
     * @return 基础资产与报价资产交易对
     */
    public TradingPair pair() { return pair; }

    /**
     * 获取原始基础资产委托数量。
     *
     * @return 严格为正的基础资产最小单位数量
     */
    public long originalBaseQuantity() { return originalBaseQuantity; }

    /**
     * 获取订单创建时的原始冻结资产数量。
     *
     * @return 买单报价资产或卖单基础资产的最小单位数量
     */
    public long originalReservedAmount() { return originalReservedAmount; }

    /**
     * 获取上游声明的报价资产风控名义金额。
     *
     * @return 严格为正的报价资产最小单位数量
     */
    public long riskQuoteAmount() { return riskQuoteAmount; }

    /**
     * 获取累计基础资产成交量。
     *
     * @return 非负且不大于原始基础数量的累计值
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long cumulativeBaseFilled() { return cumulativeBaseFilled; }

    /**
     * 获取累计报价资产成交量。
     *
     * @return 非负的权威累计报价资产数量
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long cumulativeQuoteFilled() { return cumulativeQuoteFilled; }

    /**
     * 获取尚未成交的基础资产数量。
     *
     * @return 原始基础数量减累计基础成交量
     * @note 未持用户锁时只适合诊断快照；一致业务决策必须在锁内读取。
     */
    public long remainingBaseQuantity() { return remainingBaseQuantity; }

    /**
     * 获取活动订单仍处于冻结状态的预留资产数量。
     *
     * @return 活动买单的剩余报价资产或活动卖单的剩余基础资产；终态为零
     * @note 最终买单未花费报价资产通过成交变更单独释放，本字段提交后归零。
     */
    public long remainingReservedAmount() { return remainingReservedAmount; }

    /**
     * 获取最后成功提交的订单权威序号。
     *
     * @return 创建、成交或撤单确认最后提交的序号
     * @note 读取和推进必须在所属用户锁内进行，防止跳过序号空洞。
     */
    public long lastAppliedSequence() { return lastAppliedSequence; }

    /**
     * 获取当前稳定撤单请求标识。
     *
     * @return 严格为正的撤单请求标识，尚未请求时为零
     * @note 读取和写入必须由所属用户锁保护。
     */
    public long cancelRequestId() { return cancelRequestId; }

    /**
     * 获取当前对无锁读者可见的订单状态。
     *
     * @return 当前订单状态
     */
    public OrderStatus status() { return status; }

    /**
     * 校验候选强类型提交与创建本上下文的原始载荷完全一致。
     *
     * @param submission 候选订单提交，不能为空
     * @throws NullPointerException 当提交为 {@code null} 时抛出
     * @throws OrderMetadataMismatchException 当任一提交组件不一致时抛出
     * @note 比较使用不可变原始提交，不依赖会随成交推进的剩余量、状态或最后序号。
     */
    void validateSubmission(OrderSubmission submission) {
        Objects.requireNonNull(submission, "submission");
        if (!originalSubmission.equals(submission)) {
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
        if (status != OrderStatus.NEW) {
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
        if (status == OrderStatus.RISK_HOLD) {
            status = OrderStatus.NEW;
            return true;
        }
        return false;
    }

    /**
     * 为风控拒绝派生稳定撤单请求。
     *
     * @return 同一暂挂订单始终相等且请求 ID 相同的撤单请求
     * @throws IllegalStateException 当订单不是强类型风控暂挂单或由该请求进入的等待撤单状态时抛出
     * @note 调用方必须持有用户锁；请求 ID 由订单 ID 双射派生，请求时间固定取原始提交时间，发送失败后的重试因此复用完全相同的载荷。
     * @note 不在每个订单上下文保存额外请求引用，避免 256MB 堆下大规模性能测试为从不触发的风险撤单支付常驻内存。
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
        return cancelRequestId != 0L
                && cancelRequestId == derivedRiskCancelRequestId();
    }

    /**
     * 返回订单标识双射派生的正数风险撤单请求标识。
     *
     * @return 与订单一一对应的正数风险撤单请求 ID
     */
    private long derivedRiskCancelRequestId() {
        return Long.MAX_VALUE - orderId + 1L;
    }

    /**
     * 查询指定权威序号尚未消费的输入。
     *
     * @param sequence 权威订单序号
     * @return 已登记输入；不存在时为 {@code null}
     * @note 调用方必须持有订单所属用户锁。
     */
    SequencedOrderEvent pendingEventLocked(long sequence) {
        return pendingEvents.get(sequence);
    }

    /**
     * 返回尚未消费的权威输入数量。
     *
     * @return 待处理输入数量
     * @note 调用方必须持有订单所属用户锁。
     */
    int pendingEventCountLocked() {
        return pendingEvents.size();
    }

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

    /**
     * 提交已完成全部校验的成交状态变更。
     *
     * @param mutation 预计算成交变更
     * @note 调用方必须持续持有用户锁；方法仅赋值并移除已消费序号，不执行算术或资金操作。
     */
    void commitFillLocked(OrderFillMutation mutation) {
        cumulativeBaseFilled = mutation.cumulativeBaseFilled();
        cumulativeQuoteFilled = mutation.cumulativeQuoteFilled();
        remainingBaseQuantity = mutation.remainingBaseQuantity();
        remainingReservedAmount = mutation.remainingReservedAmount();
        lastAppliedSequence = mutation.orderSequence();
        pendingEvents.remove(mutation.orderSequence());
        status = mutation.status();
    }

    /**
     * 提交已完成资金解冻准备的撤单状态变更。
     *
     * @param mutation 预计算撤单变更
     * @note 调用方必须持续持有用户锁，并先提交对应账本解冻变更。
     */
    void commitCancelLocked(OrderCancelMutation mutation) {
        remainingReservedAmount = 0L;
        lastAppliedSequence = mutation.orderSequence();
        pendingEvents.remove(mutation.orderSequence());
        status = mutation.status();
    }

    /**
     * 提交已预检的权威序号消费。
     *
     * @param orderSequence 要提交并从缓存移除的序号
     * @note 用于确定拒绝成交的双边同步推进，调用方必须持有用户锁。
     */
    void commitPreparedSequenceLocked(long orderSequence) {
        lastAppliedSequence = orderSequence;
        pendingEvents.remove(orderSequence);
    }

    /**
     * 首次登记撤单请求并进入等待确认状态。
     *
     * @param requestId 稳定且严格为正的撤单请求标识
     * @note 调用方必须持有用户锁；本方法不解冻资产。
     */
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

}
