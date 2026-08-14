package com.cex.core.engine.order;

import com.cex.core.engine.event.EventType;
import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.ledger.LedgerService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 支持乱序事件和幂等处理的订单状态机。
 *
 * <p>状态机按 orderId 锁分片串行化同一订单事件；订单尚未创建时，滞后事件进入
 * OrderEventBuffer，创建事件到达后按到达顺序补偿重放。内部聚合使用 primitive 字段，
 * applyFast 路径避免热路径快照对象分配。</p>
 */
public final class OrderStateMachine {

    /** 默认订单锁分片数量，必须为 2 的幂以便快速路由。 */
    private static final int DEFAULT_STRIPE_COUNT = 1 << 10;

    /** 按订单标识保存当前订单聚合。 */
    private final ConcurrentHashMap<Long, OrderAggregate> orders =
            new ConcurrentHashMap<>();
    /** 保存订单创建前到达的滞后事件。 */
    private final OrderEventBuffer eventBuffer;
    /** 订单并发控制锁分片。 */
    private final ReentrantLock[] stripes;
    /** 订单锁分片掩码。 */
    private final int stripeMask;
    /** 可选成交结算账本；为空时保留仅状态机的兼容模式。 */
    private final LedgerService ledgerService;

    /** 使用默认锁分片和默认乱序事件缓存创建状态机。 */
    public OrderStateMachine() {
        this(new OrderEventBuffer(), DEFAULT_STRIPE_COUNT, null);
    }

    /**
     * 创建带成交结算能力的订单状态机。
     *
     * @param ledgerService 成交时执行买方冻结到卖方可用转移的账本
     */
    public OrderStateMachine(LedgerService ledgerService) {
        this(new OrderEventBuffer(), DEFAULT_STRIPE_COUNT, ledgerService);
    }

    /**
     * 创建订单状态机。
     *
     * @param eventBuffer 订单创建前的乱序事件缓存
     * @param stripeCount 订单锁分片数量，必须为正数且为 2 的幂
     * @throws NullPointerException eventBuffer 为空时抛出
     * @throws IllegalArgumentException stripeCount 不满足分片约束时抛出
     */
    public OrderStateMachine(OrderEventBuffer eventBuffer, int stripeCount) {
        this(eventBuffer, stripeCount, null);
    }

    /** 创建可配置乱序缓存、锁分片和成交账本的状态机。 */
    public OrderStateMachine(OrderEventBuffer eventBuffer,
                             int stripeCount,
                             LedgerService ledgerService) {
        if (eventBuffer == null) {
            throw new NullPointerException("eventBuffer");
        }
        if (stripeCount < 1 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        this.eventBuffer = eventBuffer;
        this.ledgerService = ledgerService;
        this.stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            this.stripes[i] = new ReentrantLock(false);
        }
        this.stripeMask = stripeCount - 1;
    }

    /**
     * 应用订单事件并返回订单快照。
     *
     * @param event 待处理的订单事件
     * @return 事件处理结果及处理后的订单快照；未知订单滞后事件暂存时快照为 null
     * @note 同一 orderId 使用分片锁串行处理；eventId 记录在 primitive set 中，重复事件不会重复成交或扣减。
     * @note 乱序事件先写入 OrderEventBuffer，ORDER_CREATED 到达后执行后置补偿重放。
     */
    public EventApplyResult apply(OrderEvent event) {
        if (event == null) {
            throw new NullPointerException("event");
        }
        long orderId = event.getOrderId();
        ReentrantLock lock = lockFor(orderId);
        lock.lock();
        try {
            EventApplyStatus status = applyLocked(
                    event.getEventId(),
                    event.getOrderId(),
                    event.getType(),
                    event.getUserId(),
                    event.getSymbol(),
                     event.getPrice(),
                     event.getQuantity(),
                     event.getFillQuantity(),
                     event.getTradeId(),
                     event.getBuyerUserId(),
                     event.getSellerUserId(),
                     event.getSettlementAmount());
            OrderAggregate aggregate = orders.get(orderId);
            return new EventApplyResult(status,
                    aggregate == null ? null : aggregate.snapshot());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 以低对象分配路径应用订单事件。
     *
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param type 订单事件类型
     * @param userId 创建事件对应的用户标识
     * @param symbol 创建事件对应的交易对
     * @param price 创建事件对应的订单价格，使用最小价格单位
     * @param quantity 创建事件对应的订单数量，使用最小数量单位
     * @param fillQuantity 成交事件对应的成交数量，使用最小数量单位
     * @return 事件处理结果类别
     * @note 仅在同一订单锁内更新 primitive 聚合，不创建 Order 或 EventApplyResult 快照，适合高吞吐单消费者。
     * @note 乱序事件仍写入缓存，幂等 eventId 和状态分支与 apply 完全一致；调用方不得重复补偿同一事件。
     */
    public EventApplyStatus applyFast(long eventId,
                                      long orderId,
                                      EventType type,
                                      long userId,
                                      String symbol,
                                      long price,
                                      long quantity,
                                      long fillQuantity) {
        return applyFast(eventId, orderId, type, userId, symbol, price, quantity,
                fillQuantity, 0L, 0L, 0L, 0L);
    }

    /** 以低分配路径应用带成交结算事实的订单事件。 */
    public EventApplyStatus applyFast(long eventId,
                                      long orderId,
                                      EventType type,
                                      long userId,
                                      String symbol,
                                      long price,
                                      long quantity,
                                      long fillQuantity,
                                      long tradeId,
                                      long buyerUserId,
                                      long sellerUserId,
                                      long settlementAmount) {
        if (type == null) {
            throw new NullPointerException("type");
        }
        ReentrantLock lock = lockFor(orderId);
        lock.lock();
        try {
            return applyLocked(eventId, orderId, type, userId, symbol, price,
                    quantity, fillQuantity, tradeId, buyerUserId,
                    sellerUserId, settlementAmount);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取指定订单的不可变快照。
     *
     * @param orderId 订单标识
     * @return 当前订单快照，不存在时返回 null
     * @note 快照在读取时创建，不参与 applyFast 热路径。
     */
    public Order get(long orderId) {
        OrderAggregate aggregate = orders.get(orderId);
        return aggregate == null ? null : aggregate.snapshot();
    }

    /**
     * 获取指定订单的乱序待处理事件数量。
     *
     * @param orderId 订单标识
     * @return 当前待补偿事件数量
     */
    public int pendingEventCount(long orderId) {
        return eventBuffer.size(orderId);
    }

    /**
     * 在订单分片锁内应用事件事实。
     *
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param type 订单事件类型
     * @param userId 创建事件对应的用户标识
     * @param symbol 创建事件对应的交易对
     * @param price 创建事件对应的价格
     * @param quantity 创建事件对应的原始数量
     * @param fillQuantity 本次成交数量
     * @return 事件处理结果类别
     * @note 调用方必须已持有对应 orderId 分片锁；未知订单的非创建事件进入缓存，创建后按到达顺序 replay。
     */
    private EventApplyStatus applyLocked(long eventId,
                                         long orderId,
                                         EventType type,
                                         long userId,
                                         String symbol,
                                         long price,
                                         long quantity,
                                         long fillQuantity,
                                         long tradeId,
                                         long buyerUserId,
                                         long sellerUserId,
                                         long settlementAmount) {
        OrderAggregate aggregate = orders.get(orderId);
        if (aggregate == null) {
            if (type != EventType.ORDER_CREATED) {
                // 未知订单事件必须保留原始事实，等待 CREATE 到达后补偿执行。
                OrderEvent event = toEvent(eventId, orderId, type, userId, symbol,
                        price, quantity, fillQuantity, tradeId, buyerUserId,
                        sellerUserId, settlementAmount);
                boolean buffered = eventBuffer.add(event);
                return buffered ? EventApplyStatus.BUFFERED : EventApplyStatus.DUPLICATE;
            }

            aggregate = new OrderAggregate(orderId, userId, symbol, price, quantity);
            aggregate.processedEventIds.add(eventId);
            orders.put(orderId, aggregate);

            for (OrderEvent pending : eventBuffer.drain(orderId)) {
                EventApplyStatus replayStatus = applyKnownEvent(aggregate,
                        pending.getEventId(), pending.getType(),
                        pending.getFillQuantity(), pending.getTradeId(),
                        pending.getBuyerUserId(), pending.getSellerUserId(),
                        pending.getSettlementAmount());
                if (replayStatus == EventApplyStatus.SETTLEMENT_REJECTED) {
                    eventBuffer.add(pending);
                    return replayStatus;
                }
            }
            return EventApplyStatus.APPLIED;
        }

        // CREATE 重复事件只记录 eventId，不覆盖已存在聚合，避免重置成交事实。
        if (type == EventType.ORDER_CREATED) {
            return aggregate.processedEventIds.add(eventId)
                    ? EventApplyStatus.IGNORED : EventApplyStatus.DUPLICATE;
        }

        // 幂等检查必须在状态分支前执行，防止重复 MATCH 造成数量累计两次。
        if (aggregate.processedEventIds.contains(eventId)) {
            return EventApplyStatus.DUPLICATE;
        }

        EventApplyStatus result = applyKnownEvent(aggregate, eventId, type, fillQuantity, tradeId,
                buyerUserId, sellerUserId, settlementAmount);
        if (result != EventApplyStatus.SETTLEMENT_REJECTED) {
            eventBuffer.remove(orderId, eventId);
            if (type == EventType.ORDER_CANCELLED && result == EventApplyStatus.APPLIED) {
                eventBuffer.drain(orderId);
            }
        }
        return result;
    }

    /**
     * 应用已知订单聚合上的单个事件。
     *
     * @param aggregate 已创建的订单聚合
     * @param eventId 事件幂等标识
     * @param type 订单事件类型
     * @param fillQuantity 成交数量，非成交事件忽略
     * @return 事件应用结果
     * @note 成交结算成功后才写入 eventId 幂等集合，资金不足时允许上游重试；终态订单仍记录并忽略迟到事件。
     */
    private EventApplyStatus applyKnownEvent(OrderAggregate aggregate,
                                             long eventId,
                                             EventType type,
                                             long fillQuantity,
                                             long tradeId,
                                             long buyerUserId,
                                             long sellerUserId,
                                             long settlementAmount) {
        if (aggregate.processedEventIds.contains(eventId)) {
            return EventApplyStatus.DUPLICATE;
        }

        if (type == EventType.ORDER_CANCELLED) {
            // FILLED/CANCELLED 为终态，撤单只能影响未完成订单。
            if (aggregate.state == OrderState.FILLED
                    || aggregate.state == OrderState.CANCELLED) {
                aggregate.processedEventIds.add(eventId);
                return EventApplyStatus.IGNORED;
            }
            aggregate.processedEventIds.add(eventId);
            aggregate.state = OrderState.CANCELLED;
            return EventApplyStatus.APPLIED;
        }

        if (type == EventType.RISK_HOLD) {
            // 风控冻结暂停后续成交，直到外部审批流程产生新的业务指令。
            if (aggregate.state == OrderState.FILLED
                    || aggregate.state == OrderState.CANCELLED
                    || aggregate.state == OrderState.RISK_HOLD) {
                aggregate.processedEventIds.add(eventId);
                return EventApplyStatus.IGNORED;
            }
            aggregate.processedEventIds.add(eventId);
            aggregate.stateBeforeRiskHold = aggregate.state;
            aggregate.state = OrderState.RISK_HOLD;
            return EventApplyStatus.APPLIED;
        }

        if (type == EventType.RISK_RELEASED) {
            if (aggregate.state != OrderState.RISK_HOLD) {
                aggregate.processedEventIds.add(eventId);
                return EventApplyStatus.IGNORED;
            }
            aggregate.processedEventIds.add(eventId);
            aggregate.state = aggregate.stateBeforeRiskHold;
            return EventApplyStatus.APPLIED;
        }

        if (type == EventType.MATCH_FILLED) {
            // 风控冻结、撤单和完成订单禁止继续累计成交数量。
            if (aggregate.state == OrderState.FILLED
                    || aggregate.state == OrderState.CANCELLED
                    || aggregate.state == OrderState.RISK_HOLD
                    || fillQuantity <= 0L) {
                aggregate.processedEventIds.add(eventId);
                return EventApplyStatus.IGNORED;
            }

            long remaining = aggregate.quantity - aggregate.filledQuantity;
            long appliedQuantity = Math.min(remaining, fillQuantity);
            if (appliedQuantity <= 0L) {
                aggregate.processedEventIds.add(eventId);
                return EventApplyStatus.IGNORED;
            }

            if (ledgerService != null) {
                if (tradeId <= 0L || buyerUserId <= 0L || sellerUserId <= 0L
                        || settlementAmount <= 0L
                        || buyerUserId != aggregate.userId) {
                    return EventApplyStatus.SETTLEMENT_REJECTED;
                }
                try {
                    if (!ledgerService.settleTrade(tradeId, buyerUserId,
                            sellerUserId, settlementAmount)) {
                        return EventApplyStatus.SETTLEMENT_REJECTED;
                    }
                } catch (IllegalArgumentException invalidSettlement) {
                    return EventApplyStatus.SETTLEMENT_REJECTED;
                }
            }
            aggregate.processedEventIds.add(eventId);
            aggregate.filledQuantity += appliedQuantity;
            aggregate.state = aggregate.filledQuantity == aggregate.quantity
                    ? OrderState.FILLED : OrderState.PARTIAL_FILLED;
            return EventApplyStatus.APPLIED;
        }

        aggregate.processedEventIds.add(eventId);
        return EventApplyStatus.IGNORED;
    }

    /**
     * 将 primitive 事件字段转换为乱序缓存所需的不可变事件对象。
     *
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param type 事件类型
     * @param userId 用户标识
     * @param symbol 交易对标识
     * @param price 订单价格
     * @param quantity 订单数量
     * @param fillQuantity 成交数量
     * @return 可写入乱序事件缓存的事件对象
     */
    private static OrderEvent toEvent(long eventId,
                                      long orderId,
                                      EventType type,
                                      long userId,
                                      String symbol,
                                       long price,
                                       long quantity,
                                       long fillQuantity,
                                       long tradeId,
                                       long buyerUserId,
                                       long sellerUserId,
                                       long settlementAmount) {
        if (type == EventType.ORDER_CREATED) {
            return OrderEvent.created(eventId, orderId, userId, symbol, price, quantity);
        }
        if (type == EventType.ORDER_CANCELLED) {
            return OrderEvent.cancelled(eventId, orderId);
        }
        if (type == EventType.RISK_HOLD) {
            return OrderEvent.riskHold(eventId, orderId);
        }
        if (type == EventType.RISK_RELEASED) {
            return OrderEvent.riskReleased(eventId, orderId);
        }
        if (type == EventType.MATCH_FILLED && tradeId > 0L) {
            return OrderEvent.matchFilled(eventId, orderId, fillQuantity, tradeId,
                    buyerUserId, sellerUserId, settlementAmount);
        }
        return OrderEvent.matchFilled(eventId, orderId, fillQuantity);
    }

    /**
     * 根据订单标识选择状态机锁分片。
     *
     * @param orderId 订单标识
     * @return 对应订单分片锁
     */
    private ReentrantLock lockFor(long orderId) {
        int hash = Long.hashCode(orderId);
        hash ^= hash >>> 16;
        return stripes[hash & stripeMask];
    }

    /** 状态机内部可变订单聚合，仅在对应 orderId 分片锁内访问。 */
    private static final class OrderAggregate {

        /** 订单业务标识。 */
        private final long orderId;
        /** 下单用户标识。 */
        private final long userId;
        /** 交易对标识。 */
        private final String symbol;
        /** 订单价格，使用最小价格单位。 */
        private final long price;
        /** 订单原始数量，使用最小数量单位。 */
        private final long quantity;
        /** primitive 事件幂等集合，避免 Long 装箱导致 GC 压力。 */
        private final PrimitiveLongSet processedEventIds = new PrimitiveLongSet();
        /** 已成交数量，使用最小数量单位。 */
        private long filledQuantity;
        /** 当前订单状态。 */
        private OrderState state = OrderState.CREATED;
        /** 进入 RISK_HOLD 前的活跃状态，用于审批通过后恢复。 */
        private OrderState stateBeforeRiskHold = OrderState.CREATED;

        /**
         * 创建可变订单内部聚合。
         *
         * @param orderId 订单标识
         * @param userId 下单用户标识
         * @param symbol 交易对标识
         * @param price 订单价格
         * @param quantity 订单原始数量
         */
        private OrderAggregate(long orderId,
                               long userId,
                               String symbol,
                               long price,
                               long quantity) {
            this.orderId = orderId;
            this.userId = userId;
            this.symbol = symbol;
            this.price = price;
            this.quantity = quantity;
        }

        /**
         * 复制当前内部聚合为不可变订单快照。
         *
         * @return 当前订单不可变快照
         */
        private Order snapshot() {
            return new Order(orderId, userId, symbol, price, quantity,
                    filledQuantity, state);
        }
    }
}
