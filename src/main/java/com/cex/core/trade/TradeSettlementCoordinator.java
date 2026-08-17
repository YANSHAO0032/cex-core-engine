package com.cex.core.trade;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.TradeLedgerMutation;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.InvalidTradeExecutionException;
import com.cex.core.order.OrderContext;
import com.cex.core.order.OrderEngineMetrics;
import com.cex.core.order.OrderEventRegistrationMutation;
import com.cex.core.order.OrderFillMutation;
import com.cex.core.order.OrderSequenceMutation;
import com.cex.core.order.OrderSide;
import com.cex.core.order.OrderStateMachine;
import com.cex.core.order.OrderStatus;
import com.cex.core.order.OrderTerminalStateException;
import com.cex.core.order.SequencedOrderEvent;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradeOrderReference;
import com.cex.core.order.TradeSequenceConflictException;
import com.cex.core.risk.Clock;
import com.cex.core.risk.RiskWindowKey;
import com.cex.core.risk.SystemClock;
import com.cex.core.risk.TradeWindow;
import com.cex.core.risk.TradeWindowMutation;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/**
 * 在固定双用户锁顺序下原子协调成交记录、双方订单和多资产账本。
 *
 * <p>核心能力：在双方权威序号同时就绪后，先准备全部订单与资金变更，再以无算术提交完成双边结算。</p>
 * <p>线程安全：不同成交可并行处理；涉及两个用户时始终按升序条带索引加锁，同条带只获取一次。</p>
 * <p>使用限制：组件仅提供进程内原子性，不负责撮合、价格计算、订单创建或持久化恢复。</p>
 *
 * @note 成交登记和容量背压发生在用户锁外；余额与订单提交期间持续持有双方条带锁。
 * @note 基础资产与报价资产由成交交易对决定，买方余款释放合并进唯一一次账本准备，禁止拆分同一报价余额桶。
 * @note 终态成交通过保留记录实现幂等，精确重复不会再次修改余额、订单、序号或结算指标。
 */
public final class TradeSettlementCoordinator {
    /** 风控成交窗口长度，固定为 10 秒。 */
    private static final long RISK_WINDOW_MILLIS = 10_000L;
    /** 多资产账户账本。 */
    private final AccountLedger ledger;
    /** 单订单状态和权威序号状态机。 */
    private final OrderStateMachine orderStateMachine;
    /** 有界成交幂等存储。 */
    private final TradeExecutionStore tradeStore;
    /** 双用户条带锁管理器。 */
    private final StripedLockManager locks;
    /** 结算与拒绝累计指标。 */
    private final OrderEngineMetrics metrics;
    /** 由订单标识解析当前上下文的只读入口。 */
    private final LongFunction<OrderContext> orderLookup;
    /** 按用户和报价资产隔离的成交风险窗口。 */
    private final ConcurrentMap<RiskWindowKey, TradeWindow> tradeWindows;
    /** 生成风险窗口成交记录时间的时钟。 */
    private final Clock clock;

    /**
     * 创建一个双边成交协调器。
     *
     * @param ledger 多资产账本，不能为空
     * @param orderStateMachine 单订单状态机，不能为空
     * @param tradeStore 有界成交存储，不能为空
     * @param locks 账本共用的条带锁管理器，不能为空
     * @param metrics 订单引擎累计指标，不能为空
     * @param orderLookup 按订单标识返回上下文的查询函数，不能为空；订单尚未创建时返回 {@code null}
     * @throws NullPointerException 当任一依赖为 {@code null} 时抛出
     * @throws IllegalArgumentException 当账本与协调器使用不同条带锁管理器时抛出
     * @note 查询函数必须稳定返回已发布的订单上下文，不得在订单存续期替换同一标识的实例。
     */
    public TradeSettlementCoordinator(
            AccountLedger ledger,
            OrderStateMachine orderStateMachine,
            TradeExecutionStore tradeStore,
            StripedLockManager locks,
            OrderEngineMetrics metrics,
            LongFunction<OrderContext> orderLookup) {
        this(ledger, orderStateMachine, tradeStore, locks, metrics, orderLookup,
                new ConcurrentHashMap<>(), new SystemClock());
    }

    /**
     * 创建共享报价资产风险窗口的双边成交协调器。
     *
     * @param ledger 多资产账本，不能为空
     * @param orderStateMachine 单订单状态机，不能为空
     * @param tradeStore 有界成交存储，不能为空
     * @param locks 账本共用的条带锁管理器，不能为空
     * @param metrics 订单引擎累计指标，不能为空
     * @param orderLookup 按订单标识返回上下文的查询函数，不能为空
     * @param tradeWindows 按用户和报价资产隔离的并发风险窗口映射，不能为空
     * @param clock 风控窗口成交时间来源，不能为空
     * @throws NullPointerException 当任一依赖为 {@code null} 时抛出
     * @throws IllegalArgumentException 当账本与协调器使用不同条带锁管理器时抛出
     * @note 映射与订单引擎共享，使成交提交和后续创建风控读取同一窗口；窗口变更持续受对应用户条带锁保护。
     */
    public TradeSettlementCoordinator(
            AccountLedger ledger,
            OrderStateMachine orderStateMachine,
            TradeExecutionStore tradeStore,
            StripedLockManager locks,
            OrderEngineMetrics metrics,
            LongFunction<OrderContext> orderLookup,
            ConcurrentMap<RiskWindowKey, TradeWindow> tradeWindows,
            Clock clock) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
        this.orderStateMachine = Objects.requireNonNull(orderStateMachine, "orderStateMachine");
        this.tradeStore = Objects.requireNonNull(tradeStore, "tradeStore");
        this.locks = Objects.requireNonNull(locks, "locks");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.orderLookup = Objects.requireNonNull(orderLookup, "orderLookup");
        this.tradeWindows = Objects.requireNonNull(tradeWindows, "tradeWindows");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (ledger.lockManager() != locks) {
            throw new IllegalArgumentException("ledger and coordinator must share the same lock manager");
        }
    }

    /**
     * 接受权威成交并在双方订单同时就绪时尝试原子结算。
     *
     * @param execution 外部撮合提供的不可变成交，不能为空
     * @return 仍待双方就绪、已结算、已确定拒绝或终态精确重复
     * @throws NullPointerException 当成交为 {@code null} 时抛出
     * @throws TradeMetadataMismatchException 当相同成交标识已绑定不同载荷时抛出
     * @throws PendingCapacityExceededException 当新的成交标识超过固定存储容量时抛出
     * @note 成交首先在用户锁外登记；只有记录发布成功后才解析订单并获取条带锁，容量失败不消费订单序号。
     */
    public TradeResult accept(TradeExecution execution) {
        TradeExecutionRecord record = tradeStore.register(
                Objects.requireNonNull(execution, "execution"));
        return processRegistered(record);
    }

    /**
     * 重试一个订单索引下仍挂起的成交，直到本轮没有成交继续进入终态。
     *
     * @param orderId 已创建或即将创建的正数订单标识
     * @throws IllegalArgumentException 当订单标识不为正数时抛出
     * @note 存储快照和记录读取均发生在用户锁外；单笔处理再独立获取其双方条带锁，方法没有协调器级全局锁。
     */
    public void retryPendingForOrder(long orderId) {
        if (orderId <= 0L) {
            throw new IllegalArgumentException("orderId must be positive");
        }
        boolean terminalProgress;
        do {
            terminalProgress = false;
            Collection<Long> pendingTradeIds = tradeStore.pendingTradeIds(orderId);
            for (long tradeId : pendingTradeIds) {
                TradeExecutionRecord record = tradeStore.record(tradeId);
                if (record == null || record.state().isTerminal()) {
                    continue;
                }
                TradeResult result = processRegistered(record);
                if (result == TradeResult.SETTLED || result == TradeResult.REJECTED) {
                    terminalProgress = true;
                }
            }
        } while (terminalProgress);
    }

    /**
     * 处理已完成存储登记的记录，不再次触发容量预留。
     *
     * @param record 已发布的权威成交记录
     * @return 本次处理结果
     */
    private TradeResult processRegistered(TradeExecutionRecord record) {
        if (record.state().isTerminal()) {
            return TradeResult.DUPLICATE;
        }
        TradeExecution execution = record.execution();
        OrderContext initialBuyer = orderLookup.apply(execution.buyOrderId());
        OrderContext initialSeller = orderLookup.apply(execution.sellOrderId());
        if (initialBuyer == null || initialSeller == null) {
            return TradeResult.PENDING;
        }
        if (initialBuyer.userId() == initialSeller.userId()) {
            return rejectWithoutUserLocks(record, "buyer and seller users must differ");
        }
        long riskRecordedAtMillis = clock.currentTimeMillis();
        return withBothUserLocks(
                initialBuyer.userId(),
                initialSeller.userId(),
                () -> processWithBothUserLocks(
                        record, initialBuyer, initialSeller, riskRecordedAtMillis));
    }

    /**
     * 在双方条带锁内重新解析权威记录与订单并尝试准备或提交。
     *
     * @param expectedRecord 锁外解析的记录
     * @param initialBuyer 锁外解析的买方上下文
     * @param initialSeller 锁外解析的卖方上下文
     * @param riskRecordedAtMillis 获取双方用户锁前读取的风险窗口记录时间
     * @return 本次处理结果
     */
    private TradeResult processWithBothUserLocks(
            TradeExecutionRecord expectedRecord,
            OrderContext initialBuyer,
            OrderContext initialSeller,
            long riskRecordedAtMillis) {
        TradeExecution execution = expectedRecord.execution();
        TradeExecutionRecord record = tradeStore.record(execution.tradeId());
        if (record == null || record != expectedRecord) {
            throw new IllegalStateException("registered trade record changed unexpectedly");
        }
        synchronized (record) {
            if (record.state().isTerminal()) {
                return TradeResult.DUPLICATE;
            }
            OrderContext buyer = orderLookup.apply(execution.buyOrderId());
            OrderContext seller = orderLookup.apply(execution.sellOrderId());
            if (buyer == null || seller == null) {
                return TradeResult.PENDING;
            }
            if (buyer != initialBuyer || seller != initialSeller) {
                throw new IllegalStateException("order context changed while awaiting user locks");
            }
            return prepareThenCommitLocked(
                    record, buyer, seller, riskRecordedAtMillis);
        }
    }

    /**
     * 登记双方成交序号引用，并在两边都位于下一序号时准备后统一提交。
     *
     * @param record 当前仍挂起且记录监视器已持有的成交记录
     * @param buyer 已持所属用户锁的买单上下文
     * @param seller 已持所属用户锁的卖单上下文
     * @param riskRecordedAtMillis 获取双方锁前读取的风险窗口记录时间
     * @return 挂起、结算或确定拒绝结果
     */
    private TradeResult prepareThenCommitLocked(
            TradeExecutionRecord record,
            OrderContext buyer,
            OrderContext seller,
            long riskRecordedAtMillis) {
        TradeExecution execution = record.execution();
        if (isStaleForEitherOrder(execution, buyer, seller)) {
            return rejectLocked(record, "trade sequence is already consumed");
        }

        TradeOrderReference buyerReference = new TradeOrderReference(
                execution.tradeId(), execution.buyOrderId(), execution.buyOrderSequence());
        TradeOrderReference sellerReference = new TradeOrderReference(
                execution.tradeId(), execution.sellOrderId(), execution.sellOrderSequence());
        if (!isNextForBothOrders(execution, buyer, seller)) {
            prepareAndCommitBothReferencesLocked(
                    buyer, seller, buyerReference, sellerReference);
            return TradeResult.PENDING;
        }
        validateReferenceSlot(buyer, buyerReference);
        validateReferenceSlot(seller, sellerReference);

        OrderFillMutation buyerMutation;
        OrderFillMutation sellerMutation;
        TradeLedgerMutation ledgerMutation;
        RiskWindowKey buyerWindowKey;
        RiskWindowKey sellerWindowKey;
        TradeWindow buyerWindow;
        TradeWindow sellerWindow;
        boolean publishBuyerWindow;
        boolean publishSellerWindow;
        TradeWindowMutation buyerWindowMutation;
        TradeWindowMutation sellerWindowMutation;
        try {
            validateCounterpartyMetadata(execution, buyer, seller);
            buyerMutation = orderStateMachine.prepareFillLocked(buyer, execution);
            sellerMutation = orderStateMachine.prepareFillLocked(seller, execution);
            ledgerMutation = ledger.prepareTradeLocked(
                    buyer.userId(),
                    seller.userId(),
                    execution.pair().baseAsset(),
                    execution.pair().quoteAsset(),
                    execution.baseQuantity(),
                    execution.quoteQuantity(),
                    buyerMutation.buyerQuoteReleaseAmount());
            buyerWindowKey = new RiskWindowKey(
                    buyer.userId(), execution.pair().quoteAsset());
            sellerWindowKey = new RiskWindowKey(
                    seller.userId(), execution.pair().quoteAsset());
            buyerWindow = tradeWindows.get(buyerWindowKey);
            publishBuyerWindow = buyerWindow == null;
            if (publishBuyerWindow) {
                buyerWindow = new TradeWindow(RISK_WINDOW_MILLIS);
            }
            sellerWindow = tradeWindows.get(sellerWindowKey);
            publishSellerWindow = sellerWindow == null;
            if (publishSellerWindow) {
                sellerWindow = new TradeWindow(RISK_WINDOW_MILLIS);
            }
            buyerWindowMutation = buyerWindow.prepareRecord(
                    riskRecordedAtMillis, execution.quoteQuantity());
            sellerWindowMutation = sellerWindow.prepareRecord(
                    riskRecordedAtMillis, execution.quoteQuantity());
        } catch (TradeSequenceConflictException protocolConflict) {
            throw protocolConflict;
        } catch (IllegalArgumentException
                 | OrderTerminalStateException
                 | ArithmeticException deterministicFailure) {
            prepareAndCommitBothReferencesLocked(
                    buyer, seller, buyerReference, sellerReference);
            return consumeBothReferencesAndRejectLocked(
                    record, buyer, seller, buyerReference, sellerReference, deterministicFailure);
        }

        prepareAndCommitBothReferencesLocked(
                buyer, seller, buyerReference, sellerReference);
        // 新窗口完成双方全部确定性预检后才发布，拒绝路径不得遗留空窗口键。
        if (publishBuyerWindow) {
            tradeWindows.put(buyerWindowKey, buyerWindow);
        }
        if (publishSellerWindow) {
            tradeWindows.put(sellerWindowKey, sellerWindow);
        }
        ledger.commitTradeLocked(ledgerMutation);
        orderStateMachine.commitFillLocked(buyer, buyerMutation);
        orderStateMachine.commitFillLocked(seller, sellerMutation);
        buyerWindow.commitRecord(buyerWindowMutation);
        sellerWindow.commitRecord(sellerWindowMutation);
        tradeStore.markSettled(execution.tradeId(), execution.executedAtMillis());
        if (buyer.status() == OrderStatus.PARTIALLY_FILLED
                || seller.status() == OrderStatus.PARTIALLY_FILLED) {
            metrics.partialFill();
        }
        metrics.settledTrade();
        return TradeResult.SETTLED;
    }

    /**
     * 判断成交是否恰好占用双方订单的下一权威序号。
     *
     * @param execution 待处理成交
     * @param buyer 买方订单
     * @param seller 卖方订单
     * @return 两个成交序号都紧随对应最后已提交序号时为 {@code true}
     */
    private static boolean isNextForBothOrders(
            TradeExecution execution, OrderContext buyer, OrderContext seller) {
        try {
            return execution.buyOrderSequence() == Math.addExact(buyer.lastAppliedSequence(), 1L)
                    && execution.sellOrderSequence()
                    == Math.addExact(seller.lastAppliedSequence(), 1L);
        } catch (ArithmeticException exhaustedSequence) {
            throw new TradeSequenceConflictException("order sequence exhausted");
        }
    }

    /**
     * 在任何引用写入前确认下一序号缓存头为空或等于当前权威成交引用。
     *
     * @param order 目标订单
     * @param reference 当前成交在该订单上的权威引用
     * @throws TradeSequenceConflictException 当下一序号已被不同权威事件占用时抛出
     */
    private void validateReferenceSlot(OrderContext order, TradeOrderReference reference) {
        SequencedOrderEvent head = orderStateMachine.nextEventLocked(order);
        if (head != null && !head.equals(reference)) {
            throw new TradeSequenceConflictException(
                    "different event occupies the next sequence for orderId=" + order.orderId());
        }
    }

    /**
     * 先准备双方成交引用登记，在两侧都不可能失败后统一提交。
     *
     * @param buyer 买方订单
     * @param seller 卖方订单
     * @param buyerReference 买方下一权威引用
     * @param sellerReference 卖方下一权威引用
     * @throws IllegalArgumentException 当任一引用身份不匹配时抛出
     * @throws IllegalStateException 当任一引用存在冲突或超过未来事件容量时抛出
     * @note 两个 prepare 均为只读校验；两个 commit 只消费预计算结果，不执行身份、序号、冲突或容量校验。
     * @note 调用成功后已不存在可预见业务失败，调用方可安全发布尚未进入共享映射的新风险窗口并提交其变更。
     */
    private void prepareAndCommitBothReferencesLocked(
            OrderContext buyer,
            OrderContext seller,
            TradeOrderReference buyerReference,
            TradeOrderReference sellerReference) {
        OrderEventRegistrationMutation buyerRegistration =
                orderStateMachine.prepareEventRegistrationLocked(buyer, buyerReference);
        OrderEventRegistrationMutation sellerRegistration =
                orderStateMachine.prepareEventRegistrationLocked(seller, sellerReference);
        orderStateMachine.commitEventRegistrationLocked(buyerRegistration);
        orderStateMachine.commitEventRegistrationLocked(sellerRegistration);
    }

    /**
     * 判断成交序号是否已被任一订单消费。
     *
     * @param execution 待处理成交
     * @param buyer 买方订单
     * @param seller 卖方订单
     * @return 任一序号不大于对应订单最后已提交序号时为 {@code true}
     */
    private static boolean isStaleForEitherOrder(
            TradeExecution execution, OrderContext buyer, OrderContext seller) {
        return execution.buyOrderSequence() <= buyer.lastAppliedSequence()
                || execution.sellOrderSequence() <= seller.lastAppliedSequence();
    }

    /**
     * 校验上下文与成交声明的双方身份、方向和交易对完全一致。
     *
     * @param execution 权威成交
     * @param buyer 候选买单上下文
     * @param seller 候选卖单上下文
     * @throws InvalidTradeExecutionException 当任一不可变元数据不匹配时抛出
     */
    private static void validateCounterpartyMetadata(
            TradeExecution execution, OrderContext buyer, OrderContext seller) {
        if (buyer.orderId() != execution.buyOrderId()
                || seller.orderId() != execution.sellOrderId()
                || buyer.side() != OrderSide.BUY
                || seller.side() != OrderSide.SELL
                || !buyer.pair().equals(execution.pair())
                || !seller.pair().equals(execution.pair())) {
            throw new InvalidTradeExecutionException("trade counterparty metadata mismatch");
        }
    }

    /**
     * 为确定失败预先准备双方序号消费，再以纯赋值提交并终结记录。
     *
     * @param record 当前挂起成交记录
     * @param buyer 买单上下文
     * @param seller 卖单上下文
     * @param buyerReference 买方下一权威引用
     * @param sellerReference 卖方下一权威引用
     * @param failure 确定性校验失败
     * @return 已拒绝结果
     */
    private TradeResult consumeBothReferencesAndRejectLocked(
            TradeExecutionRecord record,
            OrderContext buyer,
            OrderContext seller,
            TradeOrderReference buyerReference,
            TradeOrderReference sellerReference,
            RuntimeException failure) {
        OrderSequenceMutation buyerSequence =
                orderStateMachine.prepareSequenceLocked(buyer, buyerReference);
        OrderSequenceMutation sellerSequence =
                orderStateMachine.prepareSequenceLocked(seller, sellerReference);
        orderStateMachine.commitSequenceLocked(buyerSequence);
        orderStateMachine.commitSequenceLocked(sellerSequence);
        return rejectLocked(record, rejectionReason(failure));
    }

    /**
     * 在已持记录监视器时将挂起成交终结为拒绝并累计一次指标。
     *
     * @param record 当前挂起成交记录
     * @param reason 非空拒绝原因
     * @return 已拒绝结果
     */
    private TradeResult rejectLocked(TradeExecutionRecord record, String reason) {
        tradeStore.markRejected(
                record.execution().tradeId(), reason, record.execution().executedAtMillis());
        metrics.tradeRejected();
        return TradeResult.REJECTED;
    }

    /**
     * 对禁止的同用户自成交在获取任何用户条带锁之前完成记录级拒绝。
     *
     * @param record 当前候选成交记录
     * @param reason 非空拒绝原因
     * @return 首次拒绝或终态精确重复结果
     */
    private TradeResult rejectWithoutUserLocks(TradeExecutionRecord record, String reason) {
        synchronized (record) {
            if (record.state().isTerminal()) {
                return TradeResult.DUPLICATE;
            }
            return rejectLocked(record, reason);
        }
    }

    /**
     * 将确定性异常转换为稳定且非空的拒绝说明。
     *
     * @param failure 已捕获的确定性校验异常
     * @return 包含异常类型和诊断消息的非空文本
     */
    private static String rejectionReason(RuntimeException failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank()
                ? failure.getClass().getSimpleName()
                : failure.getClass().getSimpleName() + ": " + message;
    }

    /**
     * 按升序条带索引获取两个用户锁并执行操作。
     *
     * @param firstUserId 第一个用户标识
     * @param secondUserId 第二个用户标识
     * @param action 必须在双方锁保护下执行的操作
     * @param <T> 操作结果类型
     * @return 操作返回值
     * @note 两个用户映射至同一条带时只获取和释放一次锁；释放顺序与获取顺序相反。
     */
    private <T> T withBothUserLocks(
            long firstUserId, long secondUserId, Supplier<T> action) {
        int firstStripe = locks.stripeIndexForUser(firstUserId);
        int secondStripe = locks.stripeIndexForUser(secondUserId);
        ReentrantLock lowerLock = locks.lockForStripe(Math.min(firstStripe, secondStripe));
        lowerLock.lock();
        if (firstStripe == secondStripe) {
            try {
                return action.get();
            } finally {
                lowerLock.unlock();
            }
        }

        ReentrantLock higherLock = locks.lockForStripe(Math.max(firstStripe, secondStripe));
        higherLock.lock();
        try {
            return action.get();
        } finally {
            higherLock.unlock();
            lowerLock.unlock();
        }
    }
}
