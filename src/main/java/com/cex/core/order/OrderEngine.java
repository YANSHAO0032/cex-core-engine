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

public final class OrderEngine implements AutoCloseable {
    private static final long RISK_WINDOW_MILLIS = 10_000L;
    private final AccountLedger ledger;
    private final StripedLockManager locks;
    private final RiskPipeline riskPipeline;
    private final Clock clock;
    private final ApprovalService approvalService;
    private final ApprovalPolicy approvalPolicy;
    private final ConcurrentMap<Long, OrderContext> orders = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, TradeWindow> tradeWindows = new ConcurrentHashMap<>();
    private final OrderEngineMetrics metrics = new OrderEngineMetrics();

    public OrderEngine(AccountLedger ledger) {
        this(ledger, new RiskPipeline(), new SystemClock(), new ApprovalService(1, 128), event -> ApprovalDecision.PASS);
    }

    public OrderEngine(AccountLedger ledger, RiskPipeline riskPipeline, Clock clock,
                       ApprovalService approvalService, ApprovalPolicy approvalPolicy) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.locks = ledger.lockManager();
        this.riskPipeline = Objects.requireNonNull(riskPipeline, "riskPipeline");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.approvalService = Objects.requireNonNull(approvalService, "approvalService");
        this.approvalPolicy = Objects.requireNonNull(approvalPolicy, "approvalPolicy");
    }

    public void process(OrderEvent event) {
        Objects.requireNonNull(event, "event");
        metrics.processedEvent();
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
            metrics.outOfOrderEvent();
        }

        ReconcileResult result;
        ReentrantLock lock = locks.lockForUser(context.userId());
        lock.lock();
        try {
            result = reconcileLocked(context);
        } finally {
            lock.unlock();
        }
        if (result.approvalEvent != null) {
            approvalService.submit(result.approvalEvent, approvalPolicy, this::process);
        }
    }

    private ReconcileResult reconcileLocked(OrderContext context) {
        boolean filled = context.hasFact(OrderFact.FILLED_SEEN);
        boolean cancelled = context.hasFact(OrderFact.CANCELLED_SEEN);
        boolean approved = context.hasFact(OrderFact.APPROVED_SEEN);
        boolean rejected = context.hasFact(OrderFact.REJECTED_SEEN);
        if (filled && cancelled && context.markTerminalConflictLocked()) {
            metrics.conflictingTerminalEvent();
        }
        if (approved && rejected && context.markApprovalConflictLocked()) {
            metrics.approvalConflict();
        }
        if (!context.hasFact(OrderFact.CREATED_SEEN)) {
            return ReconcileResult.NONE;
        }
        applyFreezeLocked(context);

        if (filled) {
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
        if (rejected || cancelled) {
            if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
                applyUnfreezeLocked(context);
                transitionLocked(context, OrderStatus.CANCELED);
            }
            return ReconcileResult.NONE;
        }
        if (approved && !rejected) {
            transitionLocked(context, OrderStatus.NEW);
            return ReconcileResult.NONE;
        }
        if (context.status() == OrderStatus.INIT || context.status() == OrderStatus.NEW) {
            TradeWindow window = tradeWindows.computeIfAbsent(context.userId(), id -> new TradeWindow(RISK_WINDOW_MILLIS));
            long now = clock.currentTimeMillis();
            RiskContext riskContext = new RiskContext(context.orderId(), context.userId(), context.amount(),
                    now, window.currentSum(now));
            if (riskPipeline.evaluate(riskContext) == RiskDecision.HOLD) {
                transitionLocked(context, OrderStatus.RISK_HOLD);
                metrics.riskHold();
                if (context.applyEffectLocked(OrderEffect.APPROVAL_SCHEDULED, () -> { })) {
                    metrics.approvalScheduled();
                    return new ReconcileResult(new OrderEvent(context.orderId(), context.userId(), context.amount(), now,
                            OrderEventType.ORDER_CREATED));
                }
            } else {
                transitionLocked(context, OrderStatus.NEW);
            }
        }
        return ReconcileResult.NONE;
    }

    private void applyFreezeLocked(OrderContext context) {
        if (context.hasEffect(OrderEffect.FREEZE_APPLIED)) {
            return;
        }
        context.applyEffectLocked(OrderEffect.FREEZE_APPLIED,
                () -> ledger.freezeLocked(context.userId(), context.amount()));
        metrics.freeze();
    }

    private void applySettleLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.SETTLE_APPLIED,
                () -> ledger.settleLocked(context.userId(), context.amount()))) {
            metrics.settle();
        }
    }

    private void applyUnfreezeLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.UNFREEZE_APPLIED,
                () -> ledger.unfreezeLocked(context.userId(), context.amount()))) {
            metrics.unfreeze();
        }
    }

    private void recordRiskTradeLocked(OrderContext context) {
        if (context.applyEffectLocked(OrderEffect.RISK_RECORDED, () -> {
            TradeWindow window = tradeWindows.computeIfAbsent(context.userId(), id -> new TradeWindow(RISK_WINDOW_MILLIS));
            window.record(clock.currentTimeMillis(), context.amount());
        })) {
            metrics.riskRecorded();
        }
    }

    private void transitionLocked(OrderContext context, OrderStatus newStatus) {
        OrderStatus oldStatus = context.status();
        if (oldStatus != newStatus) {
            context.setStatusLocked(newStatus);
            metrics.stateTransition();
        }
    }

    public OrderContext order(long orderId) {
        return orders.get(orderId);
    }

    public OrderEngineMetrics metrics() {
        return metrics;
    }

    public RiskPipeline riskPipeline() {
        return riskPipeline;
    }

    public TradeWindow tradeWindow(long userId) {
        return tradeWindows.get(userId);
    }

    public void awaitApprovals(long timeout, TimeUnit unit) throws InterruptedException {
        approvalService.awaitQuiescence(timeout, unit);
    }

    @Override
    public void close() {
        approvalService.close();
    }

    private static final class ReconcileResult {
        private static final ReconcileResult NONE = new ReconcileResult(null);
        private final OrderEvent approvalEvent;

        private ReconcileResult(OrderEvent approvalEvent) {
            this.approvalEvent = approvalEvent;
        }
    }
}
