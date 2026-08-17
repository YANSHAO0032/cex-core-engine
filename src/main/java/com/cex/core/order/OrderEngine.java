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
import com.cex.core.trade.TradeMetadataMismatchException;
import com.cex.core.trade.TradeResult;
import com.cex.core.trade.TradeSettlementCoordinator;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 强类型订单与双边成交的并发入口门面。
 *
 * <p>核心能力：冻结方向对应资产，按双方权威序号委托唯一成交协调器，并将撤单确认与剩余资金原子收敛。</p>
 * <p>线程安全：订单索引与事件存储并发发布；单订单变更由用户条带锁保护，双边成交由协调器按升序条带获取双方锁。</p>
 * <p>使用限制：不实现撮合、价格计算或持久化，只消费外部权威强类型输入。</p>
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
     * @param approvalService 异步审批服务，不能为空
     * @param approvalPolicy 审批决策策略，不能为空
     * @param cancelRequestSink 强类型撤单请求发送边界，不能为空
     * @throws NullPointerException 当任一依赖为 {@code null} 时抛出
     * @note 审批服务仅接收 {@link OrderSubmission} 并回流强类型 {@link ApprovalResult}。
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
                    existing.validateSubmission(submission);
                    return existing;
                }

                OrderContext candidate = OrderContext.fromSubmission(submission);
                RiskDecision decision = evaluateTypedInitialRiskLocked(submission);
                ledger.freezeLocked(
                        submission.userId(), reservedAsset(submission), submission.reservedAmount());
                candidate.classifyInitialRiskLocked(decision);
                if (decision == RiskDecision.HOLD) {
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
        boolean knownTrade = tradeStore.record(execution.tradeId()) != null;
        try {
            TradeExecutionRecord record = tradeStore.register(execution);
            if (knownTrade) {
                metrics.duplicateTrade();
            }
            if (record.state().isTerminal()) {
                return TradeResult.DUPLICATE;
            }

            TradeResult result = attemptStoredTrade(record);
            if (result == TradeResult.PENDING && hasVisibleSequenceGap(execution)) {
                metrics.sequenceGap();
            }
            if (isBilateralTerminalProgress(result)) {
                drainFromOrders(execution.buyOrderId(), execution.sellOrderId());
            }
            return result;
        } catch (TradeMetadataMismatchException conflict) {
            metrics.tradeMetadataConflict();
            throw conflict;
        } finally {
            metrics.pendingTradeCount(tradeStore.pendingCount());
        }
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
        CancelRequestAttempt attempt;
        ReentrantLock userLock = locks.lockForUser(context.userId());
        userLock.lock();
        try {
            attempt = prepareCancelRequestLocked(context, request);
        } finally {
            userLock.unlock();
        }
        return deliverCancelRequest(context, request, attempt);
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
                drainFromOrders(result.orderId());
            }
            return;
        }

        CancelRequest riskCancelRequest;
        CancelRequestAttempt attempt;
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
            attempt = prepareCancelRequestLocked(context, riskCancelRequest);
        } finally {
            userLock.unlock();
        }
        deliverCancelRequest(context, riskCancelRequest, attempt);
    }

    /**
     * 在已持用户锁时登记撤单并尝试取得唯一发送权。
     *
     * @param context 撤单所属订单上下文
     * @param request 待登记的稳定撤单请求
     * @return 撤单登记结果及本线程是否取得发送权
     * @throws IllegalArgumentException 当不同撤单请求标识已绑定该订单时抛出
     * @note 状态校验、{@link OrderStatus#PENDING_CANCEL} 登记与发送权竞争共享同一线性化临界区；调用方必须已持所属用户锁。
     */
    private CancelRequestAttempt prepareCancelRequestLocked(
            OrderContext context, CancelRequest request) {
        if (context.status() == OrderStatus.FILLED
                || context.status() == OrderStatus.CANCELED) {
            return new CancelRequestAttempt(CancelRequestResult.ALREADY_TERMINAL, false);
        }
        CancelRequestResult result;
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
            metrics.pendingCancel();
        }
        boolean deliveryClaimed = context.tryStartCancelRequestDeliveryLocked(
                request.cancelRequestId());
        return new CancelRequestAttempt(result, deliveryClaimed);
    }

    /**
     * 在用户锁外发送已取得发送权的撤单请求并收敛交付状态。
     *
     * @param context 撤单所属订单上下文
     * @param request 已登记的稳定撤单请求
     * @param attempt 锁内登记与发送权竞争结果
     * @return 首次提交、相同请求重复或订单已终态
     * @note 外部 sink 始终在用户锁外调用；发送失败回滚为可重试状态，成功后再排空该订单的权威事件。
     */
    private CancelRequestResult deliverCancelRequest(
            OrderContext context, CancelRequest request, CancelRequestAttempt attempt) {
        if (!attempt.deliveryClaimed) {
            return attempt.result;
        }
        ReentrantLock userLock = locks.lockForUser(context.userId());
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
        return attempt.result;
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
            recordSequenceRegistration(
                    orderStateMachine.registerEventLocked(context, confirmation));
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
            recordSequenceRegistration(
                    orderStateMachine.registerEventLocked(context, confirmation));
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
        metrics.pendingTradeCount(tradeStore.pendingCount());
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
     * 判断双方订单已发布时，当前成交是否位于任一下一权威序号之后。
     *
     * @param execution 待诊断的权威成交
     * @return 任一侧存在可见序号空洞时为 {@code true}
     * @note 该方法只用于弱一致指标，不参与成交正确性裁决；实际序号校验仍在双方用户锁内完成。
     */
    private boolean hasVisibleSequenceGap(TradeExecution execution) {
        OrderContext buyer = orders.get(execution.buyOrderId());
        OrderContext seller = orders.get(execution.sellOrderId());
        return buyer != null && seller != null
                && (isFutureGap(
                        execution.buyOrderSequence(), buyer.lastAppliedSequence())
                || isFutureGap(
                        execution.sellOrderSequence(), seller.lastAppliedSequence()));
    }

    /**
     * 判断候选序号是否至少超前最后序号两个位置。
     *
     * @param candidate 候选权威序号
     * @param lastApplied 最后已提交序号
     * @return 两者之间至少缺少一个序号时为 {@code true}
     */
    private static boolean isFutureGap(long candidate, long lastApplied) {
        return candidate > lastApplied && candidate - lastApplied > 1L;
    }

    /**
     * 将撤单确认登记结果转换为序号与过期指标。
     *
     * @param result 状态机返回的登记结果
     */
    private void recordSequenceRegistration(SequenceRegistrationResult result) {
        if (result == SequenceRegistrationResult.BUFFERED) {
            metrics.sequenceGap();
        } else if (result == SequenceRegistrationResult.STALE) {
            metrics.staleCancelConfirmation();
        }
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

            boolean staleAfterFill = context.status() == OrderStatus.FILLED;
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
            if (staleAfterFill) {
                metrics.staleCancelConfirmation();
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
     * 用户锁内撤单登记与发送权竞争的不可变结果。
     * 实例仅跨越一次锁外发送调用，不对外发布。
     */
    private static final class CancelRequestAttempt {
        /** 撤单首次登记、重复或已终态结果。 */
        private final CancelRequestResult result;
        /** 当前线程是否取得该撤单请求的唯一发送权。 */
        private final boolean deliveryClaimed;

        /**
         * 创建撤单登记与发送权竞争结果。
         *
         * @param result 撤单登记业务结果
         * @param deliveryClaimed 当前线程是否取得唯一发送权
         */
        private CancelRequestAttempt(
                CancelRequestResult result, boolean deliveryClaimed) {
            this.result = result;
            this.deliveryClaimed = deliveryClaimed;
        }
    }
}
