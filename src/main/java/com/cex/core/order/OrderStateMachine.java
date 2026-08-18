package com.cex.core.order;

import java.util.Objects;

/**
 * 管理单订单权威序号、部分成交和等待撤单状态的无资金状态机。
 *
 * <p>核心能力：登记有界乱序事件，并以准备—提交两阶段生成可原子协调的订单变更。</p>
 * <p>线程安全：实例无可变业务状态；每次调用必须由订单所属用户锁保护。</p>
 * <p>使用限制：不读取或修改账户余额，也不协调成交对手方订单。</p>
 */
public final class OrderStateMachine {
    /** 单订单允许缓存的未来权威事件数量上限，用于限制乱序事件内存。 */
    private final int maxPendingEvents;

    /**
     * 创建具有单订单未来事件上限的状态机。
     *
     * @param maxPendingEvents 每个订单允许缓存的未来事件数，必须严格为正
     * @throws IllegalArgumentException 当容量不为正数时抛出
     */
    public OrderStateMachine(int maxPendingEvents) {
        if (maxPendingEvents <= 0) {
            throw new IllegalArgumentException("maxPendingEvents must be positive");
        }
        this.maxPendingEvents = maxPendingEvents;
    }

    /**
     * 按订单权威序号登记一个单订单事件。
     *
     * @param order 目标订单上下文，不能为空
     * @param event 待登记的不可变事件，不能为空且订单标识必须匹配
     * @return 可立即处理、已缓存、幂等重复或已经过期的登记结果
     * @throws NullPointerException 当订单或事件为 {@code null} 时抛出
     * @throws IllegalArgumentException 当事件属于其他订单时抛出
     * @throws IllegalStateException 当未来事件缓存已满时抛出
     * @throws TradeSequenceConflictException 当相同未消费序号已有不同载荷时抛出
     * @note 调用方须持有订单所属用户锁；缓存满时新事件不会被接受或替换旧事件。
     */
    public SequenceRegistrationResult registerEventLocked(
            OrderContext order, SequencedOrderEvent event) {
        OrderEventRegistrationMutation mutation =
                prepareEventRegistrationLocked(order, event);
        commitEventRegistrationLocked(mutation);
        return mutation.result();
    }

    /**
     * 准备一个单订单权威事件登记变更而不修改订单待处理映射。
     *
     * @param order 目标订单上下文，不能为空
     * @param event 待登记的不可变事件，不能为空且订单标识必须匹配
     * @return 绑定目标订单、事件和预计算登记结果的不透明变更
     * @throws NullPointerException 当订单或事件为 {@code null} 时抛出
     * @throws IllegalArgumentException 当事件属于其他订单时抛出
     * @throws IllegalStateException 当未来事件缓存已满时抛出
     * @throws TradeSequenceConflictException 当相同未消费序号已有不同载荷时抛出
     * @note 调用方须持有订单所属用户锁；本方法只读取并校验身份、序号、冲突和容量，不写入任何订单字段。
     */
    public OrderEventRegistrationMutation prepareEventRegistrationLocked(
            OrderContext order, SequencedOrderEvent event) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(event, "event");
        requireOrderId(order, event.orderId());

        long sequence = event.orderSequence();
        if (sequence <= order.lastAppliedSequence()) {
            return new OrderEventRegistrationMutation(
                    order, event, SequenceRegistrationResult.STALE, false);
        }
        SequencedOrderEvent existing = order.pendingEventLocked(sequence);
        if (existing != null) {
            if (existing.equals(event)) {
                return new OrderEventRegistrationMutation(
                        order, event, SequenceRegistrationResult.DUPLICATE, false);
            }
            throw new TradeSequenceConflictException(
                    "different payload for orderId=" + order.orderId()
                            + ", sequence=" + sequence);
        }

        long nextSequence = nextSequence(order.lastAppliedSequence());
        int futureEventCount = order.pendingEventCountLocked();
        if (order.pendingEventLocked(nextSequence) != null) {
            futureEventCount--;
        }
        if (sequence > nextSequence && futureEventCount >= maxPendingEvents) {
            throw new IllegalStateException(
                    "pending event capacity exceeded for orderId=" + order.orderId());
        }
        SequenceRegistrationResult result = sequence == nextSequence
                ? SequenceRegistrationResult.READY
                : SequenceRegistrationResult.BUFFERED;
        return new OrderEventRegistrationMutation(order, event, result, true);
    }

    /**
     * 提交一个准备好的事件登记变更。
     *
     * @param mutation 已通过 {@link #prepareEventRegistrationLocked(OrderContext, SequencedOrderEvent)}
     *                 完成全部校验的变更
     * @note 调用方须持续持有 mutation 所属订单的用户锁；提交只执行预计算插入或无操作，不进行业务校验和容量计算。
     */
    public void commitEventRegistrationLocked(OrderEventRegistrationMutation mutation) {
        mutation.order().commitEventRegistrationLocked(mutation);
    }

    /**
     * 返回当前恰好占用下一权威序号的已登记事件。
     *
     * @param order 目标订单上下文，不能为空
     * @return 下一事件；序号仍有空洞或未登记时为 {@code null}
     * @throws NullPointerException 当订单为 {@code null} 时抛出
     * @throws TradeSequenceConflictException 当最后序号已达到 {@link Long#MAX_VALUE} 时抛出
     * @note 调用方须持有订单所属用户锁，返回值只在该临界区内保持一致。
     */
    public SequencedOrderEvent nextEventLocked(OrderContext order) {
        Objects.requireNonNull(order, "order");
        return order.pendingEventLocked(nextSequence(order.lastAppliedSequence()));
    }

    /**
     * 返回订单当前已登记但未消费的事件数量。
     *
     * @param order 目标订单上下文，不能为空
     * @return 包含下一事件和未来事件的缓存数量
     * @throws NullPointerException 当订单为 {@code null} 时抛出
     * @note 调用方须持有订单所属用户锁。
     */
    public int pendingEventCountLocked(OrderContext order) {
        return Objects.requireNonNull(order, "order").pendingEventCountLocked();
    }

    /**
     * 准备消费一个已登记且恰好位于下一权威序号的事件。
     *
     * @param order 目标订单上下文，不能为空
     * @param event 必须与缓存头完全相等的已登记事件，不能为空
     * @return 与目标订单绑定的不可变序号变更
     * @throws NullPointerException 当订单或事件为 {@code null} 时抛出
     * @throws IllegalArgumentException 当事件属于其他订单时抛出
     * @throws TradeSequenceConflictException 当事件不是下一序号、缓存头缺失或载荷不一致时抛出
     * @note 本方法只校验并创建变更，不推进序号；调用方须持有目标订单所属用户锁。
     */
    public OrderSequenceMutation prepareSequenceLocked(
            OrderContext order, SequencedOrderEvent event) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(event, "event");
        requireOrderId(order, event.orderId());
        requireNextSequence(order, event.orderSequence());
        SequencedOrderEvent head = order.pendingEventLocked(event.orderSequence());
        if (head == null || !head.equals(event)) {
            throw new TradeSequenceConflictException(
                    "event does not match registered sequence head for orderId="
                            + order.orderId() + ", sequence=" + event.orderSequence());
        }
        return new OrderSequenceMutation(order, event.orderSequence());
    }

    /**
     * 提交准备好的权威序号消费。
     *
     * @param mutation 已通过 {@link #prepareSequenceLocked(OrderContext, SequencedOrderEvent)} 校验的变更
     * @note 本方法只对变更绑定的订单赋值并移除缓存头，不执行算术或校验；调用方须持续持有用户锁。
     */
    public void commitSequenceLocked(OrderSequenceMutation mutation) {
        mutation.order().commitPreparedSequenceLocked(mutation.orderSequence());
    }

    /**
     * 准备一笔权威成交对单订单的全部状态变更。
     *
     * @param order 目标订单上下文，不能为空
     * @param execution 外部撮合提供的权威成交，不能为空
     * @return 已完成元数据、序号和数量校验的不可变成交变更
     * @throws NullPointerException 当订单或成交为 {@code null} 时抛出
     * @throws OrderTerminalStateException 当订单已经成交或取消终结时抛出
     * @throws TradeSequenceConflictException 当成交不是订单下一序号或与缓存事件冲突时抛出
     * @throws InvalidTradeExecutionException 当方向、交易对、数量、冻结额或活动状态不合法时抛出
     * @note 本方法只读订单并计算结果，不修改订单或资金；调用方须持有用户锁。
     */
    public OrderFillMutation prepareFillLocked(OrderContext order, TradeExecution execution) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(execution, "execution");
        rejectTerminalFill(order);
        requireFillableState(order);
        validateExecutionMetadata(order, execution);

        long sequence = executionSequence(order, execution);
        requireNextSequence(order, sequence);
        validateRegisteredTrade(order, execution, sequence);

        try {
            if (execution.baseQuantity() > order.remainingBaseQuantity()) {
                throw new InvalidTradeExecutionException(
                        "base quantity exceeds remaining order quantity");
            }
            long reserveDebit = order.side() == OrderSide.BUY
                    ? execution.quoteQuantity()
                    : execution.baseQuantity();
            if (reserveDebit > order.remainingReservedAmount()) {
                throw new InvalidTradeExecutionException(
                        "execution exceeds remaining reserved amount");
            }

            long cumulativeBase = Math.addExact(
                    order.cumulativeBaseFilled(), execution.baseQuantity());
            long cumulativeQuote = Math.addExact(
                    order.cumulativeQuoteFilled(), execution.quoteQuantity());
            long remainingBase = Math.subtractExact(
                    order.remainingBaseQuantity(), execution.baseQuantity());
            long reserveAfterDebit = Math.subtractExact(
                    order.remainingReservedAmount(), reserveDebit);
            boolean completelyFilled = remainingBase == 0L;
            if (completelyFilled
                    && order.side() == OrderSide.SELL
                    && reserveAfterDebit != 0L) {
                throw new InvalidTradeExecutionException(
                        "filled sell order would leave reserved base amount");
            }

            long buyerRelease = completelyFilled && order.side() == OrderSide.BUY
                    ? reserveAfterDebit
                    : 0L;
            long remainingReserve = completelyFilled ? 0L : reserveAfterDebit;
            OrderStatus targetStatus = targetFillStatus(order.status(), completelyFilled);
            return new OrderFillMutation(
                    cumulativeBase,
                    cumulativeQuote,
                    remainingBase,
                    remainingReserve,
                    buyerRelease,
                    sequence,
                    targetStatus);
        } catch (ArithmeticException exception) {
            throw new InvalidTradeExecutionException(
                    "trade execution arithmetic overflow", exception);
        }
    }

    /**
     * 提交准备好的单订单成交变更。
     *
     * @param order 准备该变更的订单上下文
     * @param mutation 已通过 {@link #prepareFillLocked(OrderContext, TradeExecution)} 校验的变更
     * @note 本方法只执行预计算赋值和缓存移除，不再计算、校验或修改资金；调用方须持有用户锁。
     */
    public void commitFillLocked(OrderContext order, OrderFillMutation mutation) {
        order.commitFillLocked(mutation);
    }

    /**
     * 准备并立即提交一笔单订单成交变更。
     *
     * @param order 目标订单上下文，不能为空
     * @param execution 外部撮合提供的权威成交，不能为空
     * @throws NullPointerException 当订单或成交为 {@code null} 时抛出
     * @throws OrderTerminalStateException 当订单已经进入终态时抛出
     * @throws TradeSequenceConflictException 当成交不是下一权威序号时抛出
     * @throws InvalidTradeExecutionException 当成交元数据、数量、冻结额或活动状态不合法时抛出
     * @note 只适用于无需双边账本协调的调用；生产结算应分别准备双方后再统一提交。
     */
    public void applyFillLocked(OrderContext order, TradeExecution execution) {
        OrderFillMutation mutation = prepareFillLocked(order, execution);
        commitFillLocked(order, mutation);
    }

    /**
     * 幂等登记本地撤单请求并进入等待确认状态。
     *
     * @param order 目标订单上下文，不能为空
     * @param request 撤单请求，不能为空且订单标识必须匹配
     * @return 首次登记并进入等待状态时为 {@code true}；重复请求或终态订单为 {@code false}
     * @throws NullPointerException 当订单或请求为 {@code null} 时抛出
     * @throws IllegalArgumentException 当请求属于其他订单时抛出
     * @note 本方法不解冻资产；调用方须持有用户锁并对首次返回结果发送外部撤单请求。
     */
    public boolean requestCancelLocked(OrderContext order, CancelRequest request) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(request, "request");
        requireOrderId(order, request.orderId());
        if (order.status() == OrderStatus.FILLED || order.status() == OrderStatus.CANCELED) {
            return false;
        }
        if (order.cancelRequestId() != 0L) {
            return false;
        }
        order.startCancelLocked(request.cancelRequestId());
        return true;
    }

    /**
     * 准备权威撤单确认对应的剩余冻结额释放和订单状态变更。
     *
     * @param order 目标订单上下文，不能为空
     * @param confirmation 外部撮合撤单确认，不能为空
     * @return 已预计算释放额、确认序号和目标状态的不可变变更
     * @throws NullPointerException 当订单或确认为 {@code null} 时抛出
     * @throws IllegalArgumentException 当订单或撤单请求标识不匹配时抛出
     * @throws OrderTerminalStateException 当订单已经取消终结时抛出
     * @throws TradeSequenceConflictException 当确认不是下一序号或与缓存事件冲突时抛出
     * @note 已完全成交订单的确认作为零释放过期确认准备，提交后仍保持 {@code FILLED}。
     */
    public OrderCancelMutation prepareCancelLocked(
            OrderContext order, CancelConfirmation confirmation) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(confirmation, "confirmation");
        requireOrderId(order, confirmation.orderId());
        if (order.cancelRequestId() == 0L
                || order.cancelRequestId() != confirmation.cancelRequestId()) {
            throw new IllegalArgumentException(
                    "cancel request ID mismatch for orderId=" + order.orderId());
        }
        if (order.status() == OrderStatus.CANCELED) {
            throw new OrderTerminalStateException(
                    "order is already canceled: orderId=" + order.orderId());
        }

        long sequence = confirmation.orderSequence();
        requireNextSequence(order, sequence);
        validateRegisteredEvent(order, confirmation, sequence);
        if (order.status() == OrderStatus.FILLED) {
            return new OrderCancelMutation(0L, sequence, OrderStatus.FILLED);
        }
        if (order.status() != OrderStatus.PENDING_CANCEL) {
            throw new IllegalArgumentException(
                    "order is not pending cancel: orderId=" + order.orderId());
        }

        long releaseAmount;
        try {
            releaseAmount = order.side() == OrderSide.BUY
                    ? Math.subtractExact(
                            order.originalReservedAmount(), order.cumulativeQuoteFilled())
                    : Math.subtractExact(
                            order.originalReservedAmount(), order.cumulativeBaseFilled());
        } catch (ArithmeticException exception) {
            throw new IllegalStateException("invalid reserved amount invariant", exception);
        }
        if (releaseAmount < 0L || releaseAmount != order.remainingReservedAmount()) {
            throw new IllegalStateException(
                    "remaining reserved amount invariant violated for orderId=" + order.orderId());
        }
        return new OrderCancelMutation(releaseAmount, sequence, OrderStatus.CANCELED);
    }

    /**
     * 提交准备好的撤单确认变更。
     *
     * @param order 准备该变更的订单上下文
     * @param mutation 已通过 {@link #prepareCancelLocked(OrderContext, CancelConfirmation)} 校验的变更
     * @note 本方法只归零剩余冻结额、推进序号并赋值状态；调用方须先提交解冻且持续持有用户锁。
     */
    public void commitCancelLocked(OrderContext order, OrderCancelMutation mutation) {
        order.commitCancelLocked(mutation);
    }

    /**
     * 校验订单当前状态允许继续应用权威成交。
     *
     * @param order 已持所属用户锁的目标订单
     * @throws InvalidTradeExecutionException 当订单处于风控暂挂或其他不可成交状态时抛出
     */
    private static void requireFillableState(OrderContext order) {
        if (order.status() != OrderStatus.NEW
                && order.status() != OrderStatus.PARTIALLY_FILLED
                && order.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidTradeExecutionException(
                    "order state cannot apply fill: " + order.status());
        }
    }

    /**
     * 拒绝对已终结订单再次应用成交。
     *
     * @param order 已持所属用户锁的目标订单
     * @throws OrderTerminalStateException 当订单已经完全成交或取消时抛出
     */
    private static void rejectTerminalFill(OrderContext order) {
        if (order.status() == OrderStatus.FILLED || order.status() == OrderStatus.CANCELED) {
            throw new OrderTerminalStateException(
                    "order is terminal: orderId=" + order.orderId()
                            + ", status=" + order.status());
        }
    }

    /**
     * 校验成交声明的订单方向、订单标识和交易对与目标订单一致。
     *
     * @param order 已持所属用户锁的目标订单
     * @param execution 外部撮合提供的权威双边成交
     * @throws InvalidTradeExecutionException 当成交引用错误订单或交易对时抛出
     */
    private static void validateExecutionMetadata(
            OrderContext order, TradeExecution execution) {
        long executionOrderId = order.side() == OrderSide.BUY
                ? execution.buyOrderId()
                : execution.sellOrderId();
        if (executionOrderId != order.orderId() || !execution.pair().equals(order.pair())) {
            throw new InvalidTradeExecutionException(
                    "trade metadata mismatch for orderId=" + order.orderId());
        }
    }

    /**
     * 按订单方向提取成交对应的单订单权威序号。
     *
     * @param order 决定读取买方或卖方序号的目标订单
     * @param execution 包含双边序号的权威成交
     * @return 买单的买方序号或卖单的卖方序号
     */
    private static long executionSequence(OrderContext order, TradeExecution execution) {
        return order.side() == OrderSide.BUY
                ? execution.buyOrderSequence()
                : execution.sellOrderSequence();
    }

    /**
     * 校验指定序号已缓存的权威事件与当前成交引用一致。
     *
     * @param order 已持所属用户锁的目标订单
     * @param execution 当前权威成交
     * @param sequence 当前成交在目标订单上的权威序号
     * @throws TradeSequenceConflictException 当同一未消费序号已由其他事件占用时抛出
     * @note 允许序号尚未缓存；若已缓存，只接受相同 tradeId、orderId 和序号的成交引用。
     */
    private static void validateRegisteredTrade(
            OrderContext order, TradeExecution execution, long sequence) {
        SequencedOrderEvent registered = order.pendingEventLocked(sequence);
        if (registered == null) {
            return;
        }
        TradeOrderReference expected = new TradeOrderReference(
                execution.tradeId(), order.orderId(), sequence);
        if (!registered.equals(expected)) {
            throw new TradeSequenceConflictException(
                    "registered event does not match trade for orderId=" + order.orderId()
                            + ", sequence=" + sequence);
        }
    }

    /**
     * 校验指定序号已缓存事件与当前权威事件载荷一致。
     *
     * @param order 已持所属用户锁的目标订单
     * @param event 当前待处理权威事件
     * @param sequence 当前事件的订单权威序号
     * @throws TradeSequenceConflictException 当同一未消费序号已有不同载荷时抛出
     * @note 允许序号尚未缓存；已缓存时必须保持精确幂等，禁止覆盖乱序事件。
     */
    private static void validateRegisteredEvent(
            OrderContext order, SequencedOrderEvent event, long sequence) {
        SequencedOrderEvent registered = order.pendingEventLocked(sequence);
        if (registered != null && !registered.equals(event)) {
            throw new TradeSequenceConflictException(
                    "registered event payload conflict for orderId=" + order.orderId()
                            + ", sequence=" + sequence);
        }
    }

    /**
     * 校验权威事件声明的订单标识属于目标订单。
     *
     * @param order 目标订单上下文
     * @param eventOrderId 权威事件声明的订单标识
     * @throws IllegalArgumentException 当事件订单标识与目标订单不一致时抛出
     */
    private static void requireOrderId(OrderContext order, long eventOrderId) {
        if (order.orderId() != eventOrderId) {
            throw new IllegalArgumentException(
                    "event order ID mismatch: expected=" + order.orderId()
                            + ", actual=" + eventOrderId);
        }
    }

    /**
     * 校验候选序号严格等于订单下一权威序号。
     *
     * @param order 已持所属用户锁的目标订单
     * @param sequence 待消费的候选权威序号
     * @throws TradeSequenceConflictException 当候选序号造成跳号、重复消费或序号耗尽时抛出
     * @note 状态推进禁止跳过乱序缓存空洞，确保成交与撤单按单订单权威顺序提交。
     */
    private static void requireNextSequence(OrderContext order, long sequence) {
        long expected = nextSequence(order.lastAppliedSequence());
        if (sequence != expected) {
            throw new TradeSequenceConflictException(
                    "expected sequence=" + expected + ", actual=" + sequence
                            + ", orderId=" + order.orderId());
        }
    }

    /**
     * 计算最后已提交序号之后的下一权威序号。
     *
     * @param lastAppliedSequence 最后成功提交的订单权威序号
     * @return 精确加一后的下一权威序号
     * @throws TradeSequenceConflictException 当序号已达到长整型上限时抛出
     */
    private static long nextSequence(long lastAppliedSequence) {
        try {
            return Math.addExact(lastAppliedSequence, 1L);
        } catch (ArithmeticException exception) {
            throw new TradeSequenceConflictException("order sequence exhausted");
        }
    }

    /**
     * 根据剩余数量和撤单等待状态确定成交后的订单状态。
     *
     * @param currentStatus 成交提交前的订单状态
     * @param completelyFilled 是否已无剩余基础资产数量
     * @return 完全成交、继续等待撤单或部分成交状态
     * @note 等待撤单期间到达的较低序号成交保持 {@code PENDING_CANCEL}，直到权威撤单确认消费下一序号。
     */
    private static OrderStatus targetFillStatus(
            OrderStatus currentStatus, boolean completelyFilled) {
        if (completelyFilled) {
            return OrderStatus.FILLED;
        }
        return currentStatus == OrderStatus.PENDING_CANCEL
                ? OrderStatus.PENDING_CANCEL
                : OrderStatus.PARTIALLY_FILLED;
    }
}
