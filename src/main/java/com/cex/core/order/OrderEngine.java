package com.cex.core.order;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalPolicy;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.Clock;
import com.cex.core.risk.RiskContext;
import com.cex.core.risk.RiskDecision;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.risk.SystemClock;
import com.cex.core.risk.TradeWindow;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于事实缓存的并发订单状态机与资金协调器。
 * 核心能力是处理乱序、重复和冲突事件，在按用户分片锁内保证冻结、结算、解冻和风控记账的幂等性；可安全并发调用。
 * 限制：只按用户串行化资金变更，调用方必须在关闭后停止提交事件，且账户余额校验由账本负责。
 */
public final class OrderEngine implements AutoCloseable {
    /** 风控成交窗口长度，单位为毫秒。 */
    private static final long RISK_WINDOW_MILLIS = 10_000L;
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
    /** 以订单 ID 索引的并发订单上下文。 */
    private final ConcurrentMap<Long, OrderContext> orders = new ConcurrentHashMap<>();
    /** 以用户 ID 索引的并发风控成交窗口。 */
    private final ConcurrentMap<Long, TradeWindow> tradeWindows = new ConcurrentHashMap<>();
    /** 对处理路径进行无锁聚合的运行指标。 */
    private final OrderEngineMetrics metrics = new OrderEngineMetrics();

    /**
     * 使用默认风控、系统时钟和审批配置创建订单引擎。
     *
     * @param ledger 账户账本，不能为空
     */
    public OrderEngine(AccountLedger ledger) {
        this(ledger, new RiskPipeline(), new SystemClock(), new ApprovalService(1, 128), event -> ApprovalDecision.PASS);
    }

    /**
     * 使用给定基础设施创建订单引擎。
     *
     * @param ledger 账户账本，不能为空
     * @param riskPipeline 风控流水线，不能为空
     * @param clock 时间来源，不能为空
     * @param approvalService 异步审批服务，不能为空
     * @param approvalPolicy 审批决策策略，不能为空
     */
    public OrderEngine(AccountLedger ledger, RiskPipeline riskPipeline, Clock clock,
                       ApprovalService approvalService, ApprovalPolicy approvalPolicy) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.locks = ledger.lockManager();
        this.riskPipeline = Objects.requireNonNull(riskPipeline, "riskPipeline");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.approvalService = Objects.requireNonNull(approvalService, "approvalService");
        this.approvalPolicy = Objects.requireNonNull(approvalPolicy, "approvalPolicy");
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
        if (result.approvalEvent != null) {
            // 审批投递移到资金锁外，避免队列背压或回调重入延长临界区。
            approvalService.submit(result.approvalEvent, approvalPolicy, this::process);
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
                if (riskResult.approvalEvent != null || context.status() == OrderStatus.RISK_HOLD) {
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
                context.userId(), id -> new TradeWindow(RISK_WINDOW_MILLIS));
        long now = clock.currentTimeMillis();
        RiskContext riskContext = new RiskContext(
                context.orderId(), context.userId(), context.amount(), now, window.currentSum(now));
        if (riskPipeline.evaluate(riskContext) == RiskDecision.HOLD) {
            transitionLocked(context, OrderStatus.RISK_HOLD);
            metrics.riskHold();
            if (context.applyEffectLocked(OrderEffect.APPROVAL_SCHEDULED, () -> { })) {
                metrics.approvalScheduled();
                return new ReconcileResult(new OrderEvent(
                        context.orderId(), context.userId(), context.amount(), now,
                        OrderEventType.ORDER_CREATED));
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
            TradeWindow window = tradeWindows.computeIfAbsent(context.userId(), id -> new TradeWindow(RISK_WINDOW_MILLIS));
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
            context.setStatusLocked(newStatus);
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
     * 查询用户的成交风控窗口。
     *
     * @param userId 用户 ID
     * @return 已建立的风控窗口；尚无成交记录时为 {@code null}
     */
    public TradeWindow tradeWindow(long userId) {
        return tradeWindows.get(userId);
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
     * 限制：当前仅承载一个审批事件。
     */
    private static final class ReconcileResult {
        /** 不需要锁外后续动作的共享结果。 */
        private static final ReconcileResult NONE = new ReconcileResult(null);
        /** 需要投递的审批事件；无投递需求时为 {@code null}。 */
        private final OrderEvent approvalEvent;

        /**
         * 创建用户锁外后续动作结果。
         *
         * @param approvalEvent 待投递审批的源事件；无后续动作时为 {@code null}
         */
        private ReconcileResult(OrderEvent approvalEvent) {
            this.approvalEvent = approvalEvent;
        }
    }
}
