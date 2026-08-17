package com.cex.core.order;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.BalanceMutation;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalPolicy;
import com.cex.core.risk.ApprovalResult;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.Clock;
import com.cex.core.risk.RiskContext;
import com.cex.core.risk.RiskDecision;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.risk.RiskWindowKey;
import com.cex.core.risk.SystemClock;
import com.cex.core.risk.TradeWindow;
import com.cex.core.trade.TradeExecutionRecord;
import com.cex.core.trade.TradeExecutionState;
import com.cex.core.trade.TradeExecutionStore;
import com.cex.core.trade.TradeResult;
import com.cex.core.trade.TradeSettlementCoordinator;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 强类型订单、双边成交和兼容事实事件的并发入口门面。
 *
 * <p>核心能力：冻结方向对应资产，按双方权威序号委托唯一成交协调器，并将撤单确认与剩余资金原子收敛。</p>
 * <p>线程安全：订单索引与事件存储并发发布；单订单变更由用户条带锁保护，双边成交由协调器按升序条带获取双方锁。</p>
 * <p>使用限制：不实现撮合、价格计算或持久化；旧版 {@link #process(OrderEvent)} 保留至类型化迁移任务完成。</p>
 *
 * @note BUY 冻结报价资产、SELL 冻结基础资产；所有成交数量均直接采用外部权威整数。
 * @note 任何可能获取双方锁的协调器调用都发生在单用户锁外，避免形成高条带到低条带的锁序反转。
 */
public final class OrderEngine implements AutoCloseable {
    /** 风控成交窗口长度，单位为毫秒。 */
    private static final long RISK_WINDOW_MILLIS = 10_000L;
    /** 单订单默认允许缓存的未来权威事件数。 */
    private static final int DEFAULT_MAX_PENDING_ORDER_EVENTS = 1_024;
    /** 负责资金冻结、结算和解冻的账户账本。 */
    private final AccountLedger ledger;
    /** 按用户提供互斥保护的分片锁管理器。 */
    private final StripedLockManager locks;
    /** 初始订单准入使用的风控流水线。 */
    private final RiskPipeline riskPipeline;
    /** 为风控窗口与审批事件提供时间的时钟。 */
    private final Clock clock;
    /** 异步审批任务的执行与回调服务。 */
    private final ApprovalService approvalService;
    /** 决定审批任务结果的审批策略。 */
    private final ApprovalPolicy approvalPolicy;
    /** 单订单序号、部分成交与撤单状态机。 */
    private final OrderStateMachine orderStateMachine;
    /** 有界成交幂等记录及待处理订单索引。 */
    private final TradeExecutionStore tradeStore;
    /** 在固定双用户锁顺序下执行权威成交的协调器。 */
    private final TradeSettlementCoordinator tradeCoordinator;
    /** 仅保存订单创建前撤单确认的有界缓存。 */
    private final PreCreationEventBuffer preCreationEventBuffer;
    /** 将首次本地撤单请求发送至外部撮合边界。 */
    private final CancelRequestSink cancelRequestSink;
    /** 以订单 ID 索引的并发订单上下文。 */
    private final ConcurrentMap<Long, OrderContext> orders = new ConcurrentHashMap<>();
    /** 以用户和报价资产共同索引的并发风控成交窗口。 */
    private final ConcurrentMap<RiskWindowKey, TradeWindow> tradeWindows =
            new ConcurrentHashMap<>();
    /** 对处理路径进行无锁聚合的运行指标。 */
    private final OrderEngineMetrics metrics = new OrderEngineMetrics();

    /**
     * 使用默认风控、系统时钟和审批配置创建订单引擎。
     *
     * @param ledger 账户账本，不能为空
     * @throws NullPointerException 当账本为 {@code null} 时抛出
     */
    public OrderEngine(AccountLedger ledger) {
        this(ledger, new RiskPipeline(), new SystemClock(), new ApprovalService(1, 128),
                event -> ApprovalDecision.PASS, request -> { });
    }

    /**
     * 使用给定基础设施创建订单引擎。
     *
     * @param ledger 账户账本，不能为空
     * @param riskPipeline 风控流水线，不能为空
     * @param clock 时间来源，不能为空
     * @param approvalService 异步审批服务，不能为空
     * @param approvalPolicy 审批决策策略，不能为空
     * @throws NullPointerException 当任一依赖为 {@code null} 时抛出
     */
    public OrderEngine(AccountLedger ledger, RiskPipeline riskPipeline, Clock clock,
                       ApprovalService approvalService, ApprovalPolicy approvalPolicy) {
        this(ledger, riskPipeline, clock, approvalService, approvalPolicy, request -> { });
    }

    /**
     * 使用给定基础设施和撤单发送边界创建订单引擎。
     *
     * @param ledger 账户账本，不能为空
     * @param riskPipeline 风控流水线，不能为空
     * @param clock 时间来源，不能为空
     * @param approvalService 旧版异步审批服务，不能为空
     * @param approvalPolicy 旧版审批决策策略，不能为空
     * @param cancelRequestSink 强类型撤单请求发送边界，不能为空
     * @throws NullPointerException 当任一依赖为 {@code null} 时抛出
     * @note 旧版审批依赖保留至兼容路径迁移完成；强类型订单不会构造通用 {@link OrderEvent} 审批任务。
     */
    public OrderEngine(AccountLedger ledger, RiskPipeline riskPipeline, Clock clock,
                       ApprovalService approvalService, ApprovalPolicy approvalPolicy,
                       CancelRequestSink cancelRequestSink) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.locks = ledger.lockManager();
        this.riskPipeline = Objects.requireNonNull(riskPipeline, "riskPipeline");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.approvalService = Objects.requireNonNull(approvalService, "approvalService");
        this.approvalPolicy = Objects.requireNonNull(approvalPolicy, "approvalPolicy");
        this.cancelRequestSink = Objects.requireNonNull(cancelRequestSink, "cancelRequestSink");
        this.orderStateMachine = new OrderStateMachine(DEFAULT_MAX_PENDING_ORDER_EVENTS);
        this.tradeStore = new TradeExecutionStore();
        this.preCreationEventBuffer = new PreCreationEventBuffer();
        this.tradeCoordinator = new TradeSettlementCoordinator(
                ledger, orderStateMachine, tradeStore, locks, metrics, orders::get,
                tradeWindows, clock);
    }

    /**
     * 提交一个强类型订单，完成创建风控、资产冻结和安全发布。
     *
     * @param submission 不可变订单提交，不能为空
     * @return 已冻结并发布的新上下文，或精确重复提交对应的原上下文
     * @throws NullPointerException 当提交为 {@code null} 时抛出
     * @throws OrderMetadataMismatchException 当同一订单标识已绑定不同提交载荷时抛出
     * @throws com.cex.core.account.InsufficientBalanceException 当应冻结资产的可用余额不足时抛出
     * @note BUY 冻结报价资产，SELL 冻结基础资产；上下文仅在风控分类和冻结均成功后发布。
     * @note 创建前撤单确认在用户锁内转入订单序列；成交重试严格在释放该单用户锁后执行，避免反向获取双用户锁。
     */
    public OrderContext submit(OrderSubmission submission) {
        Objects.requireNonNull(submission, "submission");
        ReentrantLock userLock = locks.lockForUser(submission.userId());
        OrderContext context;
        boolean[] created = new boolean[1];
        boolean[] approvalRequired = new boolean[1];
        userLock.lock();
        try {
            context = orders.compute(submission.orderId(), (orderId, existing) -> {
                if (existing != null) {
                    try {
                        existing.validateSubmission(submission);
                    } catch (OrderMetadataMismatchException mismatch) {
                        metrics.metadataConflict();
                        throw mismatch;
                    }
                    return existing;
                }

                OrderContext candidate = OrderContext.fromSubmission(submission);
                RiskDecision decision = evaluateTypedInitialRiskLocked(submission);
                ledger.freezeLocked(
                        submission.userId(), reservedAsset(submission), submission.reservedAmount());
                candidate.classifyInitialRiskLocked(decision);
                metrics.freeze();
                if (decision == RiskDecision.HOLD) {
                    metrics.riskHold();
                    approvalRequired[0] = true;
                }
                created[0] = true;
                return candidate;
            });
            transferPreCreationConfirmationsLocked(context);
        } finally {
            userLock.unlock();
        }

        if (created[0] && approvalRequired[0]) {
            // 审批队列背压与回调均在用户资金锁外发生，避免外部逻辑延长冻结临界区。
            approvalService.submit(submission, approvalPolicy, this::onApproval);
            metrics.approvalScheduled();
        }
        drainFromOrders(submission.orderId());
        return context;
    }

    /**
     * 接收外部权威双边成交并按双方订单序号推进。
     *
     * @param execution 不可变权威成交，不能为空
     * @return 仍待创建/审批/序号空洞、已结算、已拒绝或终态精确重复结果
     * @throws NullPointerException 当成交为 {@code null} 时抛出
     * @throws com.cex.core.trade.TradeMetadataMismatchException 当相同成交标识载荷不一致时抛出
     * @throws com.cex.core.trade.PendingCapacityExceededException 当成交存储固定容量不足时抛出
     * @note 成交首先只登记在 {@link TradeExecutionStore}；创建前不进入撤单确认缓存，也不获取用户锁。
     * @note 双边终结后以双方订单为起点持续处理新暴露头部，且所有协调器调用均发生在单用户锁之外。
     */
    public TradeResult onTrade(TradeExecution execution) {
        Objects.requireNonNull(execution, "execution");
        TradeExecutionRecord record = tradeStore.register(execution);
        if (record.state().isTerminal()) {
            return TradeResult.DUPLICATE;
        }

        TradeResult result = attemptStoredTrade(record);
        if (isBilateralTerminalProgress(result)) {
            drainFromOrders(execution.buyOrderId(), execution.sellOrderId());
        }
        return result;
    }

    /**
     * 幂等登记本地撤单请求并发送到外部边界。
     *
     * @param request 不可变撤单请求，不能为空
     * @return 首次提交、相同请求重复或订单已终态
     * @throws NullPointerException 当请求为 {@code null} 时抛出
     * @throws IllegalArgumentException 当订单不存在或不同请求标识已绑定该订单时抛出
     * @note 状态在用户锁内先进入 {@link OrderStatus#PENDING_CANCEL}；仅一个线程可占用发送中状态，外部发送和后续序号排空均在锁外执行。
     * @note sink 抛出时交付状态回滚为已登记，相同请求 ID 可重试；sink 成功后精确重复不再发送。
     */
    public CancelRequestResult requestCancel(CancelRequest request) {
        Objects.requireNonNull(request, "request");
        OrderContext context = requireOrder(request.orderId());
        CancelRequestResult result;
        boolean deliveryClaimed;
        ReentrantLock userLock = locks.lockForUser(context.userId());
        userLock.lock();
        try {
            if (context.status() == OrderStatus.FILLED
                    || context.status() == OrderStatus.CANCELED) {
                return CancelRequestResult.ALREADY_TERMINAL;
            }
            if (context.cancelRequestId() != 0L) {
                if (context.cancelRequestId() != request.cancelRequestId()) {
                    throw new IllegalArgumentException(
                            "different cancel request already registered for orderId="
                                    + request.orderId());
                }
                result = CancelRequestResult.DUPLICATE;
            } else {
                orderStateMachine.requestCancelLocked(context, request);
                result = CancelRequestResult.SUBMITTED;
            }
            deliveryClaimed = context.tryStartCancelRequestDeliveryLocked(
                    request.cancelRequestId());
        } finally {
            userLock.unlock();
        }

        if (!deliveryClaimed) {
            return result;
        }
        try {
            cancelRequestSink.submit(request);
        } catch (RuntimeException | Error deliveryFailure) {
            userLock.lock();
            try {
                context.failCancelRequestDeliveryLocked(request.cancelRequestId());
            } finally {
                userLock.unlock();
            }
            throw deliveryFailure;
        }
        userLock.lock();
        try {
            context.completeCancelRequestDeliveryLocked(request.cancelRequestId());
        } finally {
            userLock.unlock();
        }
        drainFromOrders(request.orderId());
        return result;
    }

    /**
     * 接收外部权威撤单确认，或在订单创建前有界缓存。
     *
     * @param confirmation 不可变撤单确认，不能为空
     * @throws NullPointerException 当确认为 {@code null} 时抛出
     * @throws TradeSequenceConflictException 当同一未消费序号已有不同权威载荷时抛出
     * @throws IllegalStateException 当创建前或订单内未来事件容量已满时抛出
     * @note 创建后确认在所属用户锁内登记；解冻和订单取消使用两阶段准备并在同一用户锁临界区提交。
     */
    public void onCancelConfirmed(CancelConfirmation confirmation) {
        Objects.requireNonNull(confirmation, "confirmation");
        OrderContext context = orders.get(confirmation.orderId());
        if (context == null) {
            preCreationEventBuffer.register(confirmation);
            context = orders.get(confirmation.orderId());
            if (context == null) {
                return;
            }
            transferPreCreationConfirmations(context);
        } else {
            registerConfirmation(context, confirmation);
        }
        drainFromOrders(confirmation.orderId());
    }

    /**
     * 接收强类型审批结果并恢复暂挂订单或发起幂等风险撤单。
     *
     * @param result 不可变审批结果，不能为空
     * @throws NullPointerException 当结果为 {@code null} 时抛出
     * @note PASS 在用户锁内恢复订单后于锁外排空缓存成交；REJECT 只进入 {@link OrderStatus#PENDING_CANCEL} 并发送稳定请求，绝不直接解冻。
     * @note 风险撤单发送失败后，重复的 REJECT 回调复用同一请求对象并保留 Task 6 的同 ID 单飞重试语义。
     */
    public void onApproval(ApprovalResult result) {
        Objects.requireNonNull(result, "result");
        OrderContext context = orders.get(result.orderId());
        if (context == null) {
            return;
        }
        ReentrantLock userLock = locks.lockForUser(context.userId());
        if (result.decision() == ApprovalDecision.PASS) {
            boolean approved;
            userLock.lock();
            try {
                approved = context.approveRiskHoldLocked();
            } finally {
                userLock.unlock();
            }
            if (approved) {
                metrics.approvalPass();
                drainFromOrders(result.orderId());
            }
            return;
        }

        CancelRequest riskCancelRequest;
        userLock.lock();
        try {
            if (context.status() == OrderStatus.RISK_HOLD) {
                riskCancelRequest = context.riskCancelRequestLocked();
            } else if (context.status() == OrderStatus.PENDING_CANCEL
                    && context.hasRiskCancelRequestLocked()) {
                riskCancelRequest = context.riskCancelRequestLocked();
            } else {
                return;
            }
        } finally {
            userLock.unlock();
        }
        if (riskCancelRequest != null) {
            CancelRequestResult cancelResult = requestCancel(riskCancelRequest);
            if (cancelResult == CancelRequestResult.SUBMITTED) {
                metrics.approvalReject();
            }
        }
    }

    /**
     * 查询指定成交的幂等记录。
     *
     * @param tradeId 成交标识
     * @return 已登记成交记录；尚未接收时为 {@code null}
     */
    public TradeExecutionRecord trade(long tradeId) {
        return tradeStore.record(tradeId);
    }

    /**
     * 返回当前仍待双边终结的成交数。
     *
     * @return 已预留或已发布的挂起成交记录数
     */
    public int pendingTradeCount() {
        return tradeStore.pendingCount();
    }

    /**
     * 在提交用户锁内使用强类型报价名义金额执行最小创建风控。
     *
     * @param submission 当前尚未发布的订单提交
     * @return 风控通过或暂挂结论
     * @note 窗口按用户与报价资产组合隔离；创建风险使用上游提供的正数名义金额，不根据基础资产冻结量反推。
     */
    private RiskDecision evaluateTypedInitialRiskLocked(OrderSubmission submission) {
        RiskWindowKey key = new RiskWindowKey(
                submission.userId(), submission.pair().quoteAsset());
        TradeWindow window = tradeWindows.computeIfAbsent(
                key, ignored -> new TradeWindow(RISK_WINDOW_MILLIS));
        long now = clock.currentTimeMillis();
        return riskPipeline.evaluate(new RiskContext(
                submission.orderId(), submission.userId(), submission.pair().quoteAsset(),
                submission.riskQuoteAmount(), now, window.currentSum(now)));
    }

    /**
     * 将强类型审批结果适配为旧版事实事件。
     *
     * @param submission 旧版上下文生成的强类型审批提交
     * @param approval 强类型审批结果
     * @return 保留旧入口用户和金额元数据的审批事实事件
     * @deprecated 仅供 {@link #process(OrderEvent)} 迁移期兼容；任务 8 删除
     */
    @Deprecated(since = "typed-approval", forRemoval = true)
    private static OrderEvent legacyApprovalEvent(
            OrderSubmission submission, ApprovalResult approval) {
        OrderEventType type = approval.decision() == ApprovalDecision.PASS
                ? OrderEventType.APPROVAL_PASSED
                : OrderEventType.APPROVAL_REJECTED;
        return new OrderEvent(
                submission.orderId(),
                submission.userId(),
                submission.reservedAmount(),
                approval.decidedAtMillis(),
                type);
    }

    /**
     * 返回订单提交声明的冻结资产。
     *
     * @param submission 强类型订单提交
     * @return BUY 的报价资产或 SELL 的基础资产
     */
    private static AssetId reservedAsset(OrderSubmission submission) {
        return submission.side() == OrderSide.BUY
                ? submission.pair().quoteAsset()
                : submission.pair().baseAsset();
    }

    /**
     * 返回活动上下文对应的冻结资产。
     *
     * @param context 强类型订单上下文
     * @return BUY 的报价资产或 SELL 的基础资产
     */
    private static AssetId reservedAsset(OrderContext context) {
        return context.side() == OrderSide.BUY
                ? context.pair().quoteAsset()
                : context.pair().baseAsset();
    }

    /**
     * 在获取用户锁后转移该订单全部创建前撤单确认。
     *
     * @param context 已冻结并发布的强类型订单
     * @note 删除与订单事件登记受同一用户锁保护；晚到登记方会重新读取已发布订单并执行第二次转移。
     */
    private void transferPreCreationConfirmations(OrderContext context) {
        ReentrantLock userLock = locks.lockForUser(context.userId());
        userLock.lock();
        try {
            transferPreCreationConfirmationsLocked(context);
        } finally {
            userLock.unlock();
        }
    }

    /**
     * 将创建前确认按序号登记到订单未来事件映射。
     *
     * @param context 已持所属用户锁的订单上下文
     */
    private void transferPreCreationConfirmationsLocked(OrderContext context) {
        for (CancelConfirmation confirmation
                : preCreationEventBuffer.removeAll(context.orderId())) {
            orderStateMachine.registerEventLocked(context, confirmation);
        }
    }

    /**
     * 在活动订单的用户锁内登记单个撤单确认。
     *
     * @param context 已发布订单上下文
     * @param confirmation 待登记确认
     */
    private void registerConfirmation(
            OrderContext context, CancelConfirmation confirmation) {
        ReentrantLock userLock = locks.lockForUser(context.userId());
        userLock.lock();
        try {
            orderStateMachine.registerEventLocked(context, confirmation);
        } finally {
            userLock.unlock();
        }
    }

    /**
     * 从给定订单开始持续重试成交与撤单头部，直到本调用没有新序号进展。
     *
     * @param initialOrderIds 至少一个已校验的订单标识
     * @note 队列只因成交终结或撤单提交而扩展；两者都严格推进序号，因此无空洞跳过或无进展自旋。
     * @note 成交协调发生在未持单用户锁时；协调器内部再按升序条带索引获取双方锁。
     */
    private void drainFromOrders(long... initialOrderIds) {
        ArrayDeque<Long> orderIds = new ArrayDeque<>();
        for (long orderId : initialOrderIds) {
            orderIds.addLast(orderId);
        }
        while (!orderIds.isEmpty()) {
            long orderId = orderIds.removeFirst();
            retryIndexedTrades(orderId, orderIds);
            if (applyReadyCancel(orderId)) {
                orderIds.addLast(orderId);
            }
        }
    }

    /**
     * 在不持有单用户锁时尝试指定订单索引下的每笔挂起成交。
     *
     * @param orderId 当前排空订单标识
     * @param followUpOrderIds 成交终结后追加双方订单的工作队列
     */
    private void retryIndexedTrades(
            long orderId, ArrayDeque<Long> followUpOrderIds) {
        for (long tradeId : tradeStore.pendingTradeIds(orderId)) {
            TradeExecutionRecord record = tradeStore.record(tradeId);
            if (record == null || record.state() != TradeExecutionState.PENDING) {
                continue;
            }
            TradeResult result = attemptStoredTrade(record);
            if (isBilateralTerminalProgress(result)) {
                TradeExecution execution = record.execution();
                followUpOrderIds.addLast(execution.buyOrderId());
                followUpOrderIds.addLast(execution.sellOrderId());
            }
        }
    }

    /**
     * 在订单创建且均未风控暂挂时委托唯一双边协调器处理记录。
     *
     * @param record 已在有界成交存储发布的挂起记录
     * @return 挂起、结算、拒绝或并发终态重复结果
     * @note 暂挂检查只阻止协调器把 {@code RISK_HOLD} 误判为确定性非法状态；不执行任何撮合或资金计算。
     */
    private TradeResult attemptStoredTrade(TradeExecutionRecord record) {
        TradeExecution execution = record.execution();
        OrderContext buyer = orders.get(execution.buyOrderId());
        OrderContext seller = orders.get(execution.sellOrderId());
        if (buyer == null || seller == null
                || buyer.status() == OrderStatus.RISK_HOLD
                || seller.status() == OrderStatus.RISK_HOLD) {
            return TradeResult.PENDING;
        }
        return tradeCoordinator.accept(execution);
    }

    /**
     * 判断一次协调结果是否推进了双方权威序号并需要继续排空。
     *
     * @param result 协调器处理结果
     * @return 新结算或新拒绝时为 {@code true}
     */
    private static boolean isBilateralTerminalProgress(TradeResult result) {
        return result == TradeResult.SETTLED || result == TradeResult.REJECTED;
    }

    /**
     * 若下一权威事件是已匹配请求的撤单确认，则原子准备并提交解冻和订单取消。
     *
     * @param orderId 待检查订单标识
     * @return 本次成功消费一个撤单确认时为 {@code true}
     * @note 订单变更与余额变更都在任何写入前准备完成；提交期间持续持有同一用户锁且不调用双边协调器。
     */
    private boolean applyReadyCancel(long orderId) {
        OrderContext context = orders.get(orderId);
        if (context == null) {
            return false;
        }
        ReentrantLock userLock = locks.lockForUser(context.userId());
        userLock.lock();
        try {
            SequencedOrderEvent next = orderStateMachine.nextEventLocked(context);
            if (!(next instanceof CancelConfirmation confirmation)
                    || context.cancelRequestId() == 0L) {
                return false;
            }

            OrderStatus oldStatus = context.status();
            OrderCancelMutation orderMutation =
                    orderStateMachine.prepareCancelLocked(context, confirmation);
            BalanceMutation balanceMutation = orderMutation.releaseAmount() == 0L
                    ? null
                    : ledger.prepareUnfreezeLocked(
                            context.userId(), reservedAsset(context), orderMutation.releaseAmount());

            if (balanceMutation != null) {
                ledger.commitBalanceLocked(balanceMutation);
            }
            orderStateMachine.commitCancelLocked(context, orderMutation);
            if (balanceMutation != null) {
                metrics.unfreeze();
            }
            if (oldStatus != context.status()) {
                metrics.stateTransition();
            }
            return true;
        } finally {
            userLock.unlock();
        }
    }

    /**
     * 获取必须已经发布的订单上下文。
     *
     * @param orderId 订单标识
     * @return 已发布订单上下文
     * @throws IllegalArgumentException 当订单不存在时抛出
     */
    private OrderContext requireOrder(long orderId) {
        OrderContext context = orders.get(orderId);
        if (context == null) {
            throw new IllegalArgumentException("order not found for orderId=" + orderId);
        }
        return context;
    }

    /**
     * 接收一个订单事件并将订单收敛到当前可推导状态。
     *
     * @param event 待处理事件，不能为空，可重复或乱序到达
     * @throws OrderMetadataMismatchException 当同一订单 ID 的用户或金额不一致时
     * @note 先以原子位图缓存事实，再按用户锁执行状态机；因此乱序事件会滞后收敛，重复事件不会重复冻结、结算或解冻。
     */
    public void process(OrderEvent event) {
        Objects.requireNonNull(event, "event");
        metrics.processedEvent();
        // 首事件可以是乱序终态；上下文先固化订单元数据，后续事件必须保持一致。
        OrderContext context = orders.compute(event.orderId(), (id, existing) -> {
            if (existing == null) {
                return OrderContext.fromFirstEvent(event);
            }
            try {
                existing.validateMetadata(event);
            } catch (OrderMetadataMismatchException mismatch) {
                metrics.metadataConflict();
                throw mismatch;
            }
            return existing;
        });

        boolean createdBefore = context.hasFact(OrderFact.CREATED_SEEN);
        // Fact Bit 使用 CAS 无锁登记，重复事件仍继续 reconcile 以补偿已登记但未执行完的副作用。
        FactRegistrationResult registration = context.registerFact(event.type());
        if (registration == FactRegistrationResult.DUPLICATE) {
            metrics.duplicateEvent();
        } else {
            metrics.acceptedFact();
            if (event.type() == OrderEventType.APPROVAL_PASSED) {
                metrics.approvalPass();
            } else if (event.type() == OrderEventType.APPROVAL_REJECTED) {
                metrics.approvalReject();
            }
        }
        if (event.type() != OrderEventType.ORDER_CREATED && !createdBefore) {
            // 创建前到达的终态或审批事实仅计为乱序并保存在位图中，不在入口直接操作资金。
            metrics.outOfOrderEvent();
        }

        ReconcileResult result;
        ReentrantLock lock = locks.lockForUser(context.userId());
        // 同一用户的订单状态、账户余额和风控窗口在同一条带锁内串行收敛。
        lock.lock();
        try {
            result = reconcileLocked(context);
        } finally {
            lock.unlock();
        }
        if (result.approvalSubmission != null) {
            // 审批投递移到资金锁外，避免队列背压或回调重入延长临界区。
            approvalService.submit(
                    result.approvalSubmission,
                    approvalPolicy,
                    approval -> process(legacyApprovalEvent(result.approvalSubmission, approval)));
        }
    }

    /**
     * 在用户锁内根据已缓存事实协调资金副作用和订单状态。
     *
     * @param context 待协调订单上下文
     * @return 需要在锁外投递的审批结果
     * @note 创建前只缓存乱序事实；终态冲突遵从先已提交的资金副作用，避免已结算订单被错误解冻，保障资产守恒。
     */
    private ReconcileResult reconcileLocked(OrderContext context) {
        boolean filled = context.hasFact(OrderFact.FILLED_SEEN);
        boolean cancelled = context.hasFact(OrderFact.CANCELLED_SEEN);
        boolean approved = context.hasFact(OrderFact.APPROVED_SEEN);
        boolean rejected = context.hasFact(OrderFact.REJECTED_SEEN);
        // 冲突仅计数一次，实际终态由已提交的资金副作用决定。
        if (filled && cancelled && context.markTerminalConflictLocked()) {
            metrics.conflictingTerminalEvent();
        }
        if (approved && rejected && context.markApprovalConflictLocked()) {
            metrics.approvalConflict();
        }
        if (!context.hasFact(OrderFact.CREATED_SEEN)) {
            // 乱序事实继续滞留在 Fact Bit，等待 CREATE 后再次进入本方法执行后置补偿。
            return ReconcileResult.NONE;
        }
        // CREATE 齐备后先幂等冻结，后续终态只能在这笔冻结资金上结算或解冻。
        applyFreezeLocked(context);

        if (rejected) {
            // 审批拒绝优先于通过；若尚未结算，只允许解冻一次并收敛为取消。
            if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
                applyUnfreezeLocked(context);
                transitionLocked(context, OrderStatus.CANCELED);
            }
            return ReconcileResult.NONE;
        }
        if (cancelled && !filled) {
            // 单独撤单事实只能释放未结算冻结资金，不得覆盖已经发生的成交结算。
            if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
                applyUnfreezeLocked(context);
                transitionLocked(context, OrderStatus.CANCELED);
            }
            return ReconcileResult.NONE;
        }

        if (context.status() == OrderStatus.INIT) {
            // 初始风控只执行一次；重复事件不会让已接纳订单因窗口变化重新进入挂起。
            if (approved) {
                transitionLocked(context, OrderStatus.NEW);
            } else {
                ReconcileResult riskResult = evaluateInitialRiskLocked(context);
                if (riskResult.approvalSubmission != null
                        || context.status() == OrderStatus.RISK_HOLD) {
                    return riskResult;
                }
            }
        }

        if (context.status() == OrderStatus.RISK_HOLD) {
            if (!approved) {
                // 挂起期间的成交事实保持缓存，禁止在审批通过前提前结算。
                return ReconcileResult.NONE;
            }
            // 审批通过后恢复 NEW，并在同次 reconcile 中继续消费可能已缓存的成交事实。
            transitionLocked(context, OrderStatus.NEW);
        }

        if (filled) {
            // 成交与撤单冲突时以已提交的互斥资金 Effect 为准，严禁同时结算和解冻。
            if (!context.hasEffect(OrderEffect.UNFREEZE_APPLIED)) {
                applySettleLocked(context);
            }
            if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
                return ReconcileResult.NONE;
            }
            recordRiskTradeLocked(context);
            transitionLocked(context, OrderStatus.FILLED);
            return ReconcileResult.NONE;
        }
        return ReconcileResult.NONE;
    }

    /**
     * 在用户锁内执行初始风控，并在需要时仅投递一次审批任务。
     *
     * @param context 已冻结且尚未完成初始状态迁移的订单
     * @return 需要异步处理的审批事件，或无审批结果
     * @note 风控窗口和审批 Effect Bit 都在用户锁内更新，防止并发重复投递；审批回调以新事实形式重入状态机。
     */
    private ReconcileResult evaluateInitialRiskLocked(OrderContext context) {
        TradeWindow window = tradeWindows.computeIfAbsent(
                new RiskWindowKey(context.userId(), context.pair().quoteAsset()),
                ignored -> new TradeWindow(RISK_WINDOW_MILLIS));
        long now = clock.currentTimeMillis();
        RiskContext riskContext = new RiskContext(
                context.orderId(), context.userId(), context.pair().quoteAsset(),
                context.riskQuoteAmount(), now, window.currentSum(now));
        if (riskPipeline.evaluate(riskContext) == RiskDecision.HOLD) {
            transitionLocked(context, OrderStatus.RISK_HOLD);
            metrics.riskHold();
            if (context.applyEffectLocked(OrderEffect.APPROVAL_SCHEDULED, () -> { })) {
                metrics.approvalScheduled();
                return new ReconcileResult(context.approvalSubmission(now));
            }
            return ReconcileResult.NONE;
        }
        transitionLocked(context, OrderStatus.NEW);
        return ReconcileResult.NONE;
    }

    /**
     * 在用户锁内为订单金额执行一次冻结。
     *
     * @param context 待冻结订单上下文
     * @note Effect Bit 在账本成功冻结后才提交，失败时可安全重试；冻结、结算和解冻必须遵循同一用户锁以维持资产守恒。
     */
    private void applyFreezeLocked(OrderContext context) {
        if (context.hasEffect(OrderEffect.FREEZE_APPLIED)) {
            return;
        }
        context.applyEffectLocked(OrderEffect.FREEZE_APPLIED,
                () -> ledger.freezeLocked(context.userId(), context.amount()));
        metrics.freeze();
    }

    /**
     * 在用户锁内将已冻结订单金额执行一次结算。
     *
     * @param context 待结算订单上下文
     * @note 仅在未解冻时结算，幂等 Effect Bit 防止重放成交事件重复扣减资产。
     */
    private void applySettleLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.SETTLE_APPLIED,
                () -> ledger.settleLocked(context.userId(), context.amount()))) {
            metrics.settle();
        }
    }

    /**
     * 在用户锁内将未结算订单金额执行一次解冻。
     *
     * @param context 待解冻订单上下文
     * @note 仅对未结算订单解冻；终态冲突分支依赖该前提以避免已结算资金被二次返还。
     */
    private void applyUnfreezeLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.UNFREEZE_APPLIED,
                () -> ledger.unfreezeLocked(context.userId(), context.amount()))) {
            metrics.unfreeze();
        }
    }

    /**
     * 在用户锁内将成交金额计入一次风控窗口。
     *
     * @param context 已结算订单上下文
     * @note 幂等标记使重复成交事件不会放大风险敞口；记录失败前不得视为风控事实已提交。
     */
    private void recordRiskTradeLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.RISK_RECORDED, () -> {
            TradeWindow window = tradeWindows.computeIfAbsent(
                    new RiskWindowKey(context.userId(), context.pair().quoteAsset()),
                    ignored -> new TradeWindow(RISK_WINDOW_MILLIS));
            window.record(clock.currentTimeMillis(), context.amount());
        })) {
            metrics.riskRecorded();
        }
    }

    /**
     * 在用户锁内执行有变化才计数的状态迁移。
     *
     * @param context 待迁移订单上下文
     * @param newStatus 目标订单状态
     */
    private void transitionLocked(OrderContext context, OrderStatus newStatus) {
        OrderStatus oldStatus = context.status();
        if (oldStatus != newStatus) {
            context.setLegacyStatusLocked(newStatus);
            metrics.stateTransition();
        }
    }

    /**
     * 查询指定订单的当前上下文。
     *
     * @param orderId 订单 ID
     * @return 已存在订单的上下文；尚未处理时为 {@code null}
     */
    public OrderContext order(long orderId) {
        return orders.get(orderId);
    }

    /**
     * 返回订单引擎运行指标。
     *
     * @return 线程安全的指标聚合器
     */
    public OrderEngineMetrics metrics() {
        return metrics;
    }

    /**
     * 返回当前风控流水线。
     *
     * @return 引擎使用的风控流水线
     */
    public RiskPipeline riskPipeline() {
        return riskPipeline;
    }

    /**
     * 查询用户在指定报价资产下的成交风控窗口。
     *
     * @param userId 用户 ID
     * @param quoteAsset 报价资产标识
     * @return 已建立的风控窗口；尚无成交记录时为 {@code null}
     * @throws NullPointerException 当报价资产为 {@code null} 时抛出
     */
    public TradeWindow tradeWindow(long userId, AssetId quoteAsset) {
        return tradeWindows.get(new RiskWindowKey(userId, quoteAsset));
    }

    /**
     * 等待已投递审批任务完成或超时。
     *
     * @param timeout 最长等待时长
     * @param unit 超时单位，不能为空
     * @throws InterruptedException 当等待线程被中断时
     */
    public void awaitApprovals(long timeout, TimeUnit unit) throws InterruptedException {
        approvalService.awaitQuiescence(timeout, unit);
    }

    /**
     * 关闭异步审批服务并释放其资源。
     */
    @Override
    public void close() {
        approvalService.close();
    }

    /**
     * 用户锁内协调产生的锁外后续动作。
     * 核心能力是将审批投递移出资金锁；实例不可变且线程安全。
     * 限制：当前仅承载一个审批提交。
     */
    private static final class ReconcileResult {
        /** 不需要锁外后续动作的共享结果。 */
        private static final ReconcileResult NONE = new ReconcileResult(null);
        /** 需要投递的强类型审批提交；无投递需求时为 {@code null}。 */
        private final OrderSubmission approvalSubmission;

        /**
         * 创建用户锁外后续动作结果。
         *
         * @param approvalSubmission 待投递审批的强类型提交；无后续动作时为 {@code null}
         */
        private ReconcileResult(OrderSubmission approvalSubmission) {
            this.approvalSubmission = approvalSubmission;
        }
    }
}
