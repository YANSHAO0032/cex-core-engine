package com.cex.core.engine.event;

/**
 * 不可变订单事件信封。
 *
 * <p>创建字段仅用于 ORDER_CREATED，成交数量仅用于 MATCH_FILLED；事件对象适合跨线程
 * 发布，禁止在入队后修改。</p>
 */
public final class OrderEvent {

    /** 全局或业务域内唯一的事件标识，用于幂等去重。 */
    private final long eventId;
    /** 事件所属订单标识，用于状态机分片和乱序缓存。 */
    private final long orderId;
    /** 外部订单事件类型。 */
    private final EventType type;
    /** 创建事件对应的用户账户标识。 */
    private final long userId;
    /** 创建事件对应的交易对标识。 */
    private final String symbol;
    /** 订单价格，使用最小价格单位表示。 */
    private final long price;
    /** 订单原始数量，使用交易标的最小数量单位表示。 */
    private final long quantity;
    /** 本次撮合成交数量，使用交易标的最小数量单位表示。 */
    private final long fillQuantity;
    /** 结算成交幂等标识；未携带结算信息的兼容事件为 0。 */
    private final long tradeId;
    /** 结算买方账户；未携带结算信息的兼容事件为 0。 */
    private final long buyerUserId;
    /** 结算卖方账户；未携带结算信息的兼容事件为 0。 */
    private final long sellerUserId;
    /** 结算金额；未携带结算信息的兼容事件为 0。 */
    private final long settlementAmount;

    /**
     * 创建不可变事件对象。
     *
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param type 订单事件类型
     * @param userId 创建事件对应的用户标识
     * @param symbol 交易对标识
     * @param price 订单价格，使用最小价格单位
     * @param quantity 订单数量，使用最小数量单位
     * @param fillQuantity 成交数量，使用最小数量单位
     */
    private OrderEvent(long eventId,
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
        this.eventId = eventId;
        this.orderId = orderId;
        this.type = type;
        this.userId = userId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.fillQuantity = fillQuantity;
        this.tradeId = tradeId;
        this.buyerUserId = buyerUserId;
        this.sellerUserId = sellerUserId;
        this.settlementAmount = settlementAmount;
    }

    /**
     * 构造订单创建事件。
     *
     * @param eventId 创建事件幂等标识
     * @param orderId 订单标识
     * @param userId 用户账户标识
     * @param symbol 交易对标识
     * @param price 订单价格，使用最小价格单位
     * @param quantity 订单数量，使用最小数量单位
     * @return 创建类型订单事件
     */
    public static OrderEvent created(long eventId,
                                     long orderId,
                                     long userId,
                                     String symbol,
                                     long price,
                                     long quantity) {
        return new OrderEvent(eventId, orderId, EventType.ORDER_CREATED,
                userId, symbol, price, quantity, 0L,
                0L, 0L, 0L, 0L);
    }

    /**
     * 构造订单撤单事件。
     *
     * @param eventId 撤单事件幂等标识
     * @param orderId 待撤订单标识
     * @return 撤单类型订单事件
     */
    public static OrderEvent cancelled(long eventId, long orderId) {
        return new OrderEvent(eventId, orderId, EventType.ORDER_CANCELLED,
                0L, null, 0L, 0L, 0L,
                0L, 0L, 0L, 0L);
    }

    /**
     * 构造成交事件。
     *
     * @param eventId 成交事件幂等标识
     * @param orderId 成交所属订单标识
     * @param fillQuantity 本次成交数量，使用最小数量单位
     * @return 成交类型订单事件
     */
    public static OrderEvent matchFilled(long eventId, long orderId, long fillQuantity) {
        return new OrderEvent(eventId, orderId, EventType.MATCH_FILLED,
                0L, null, 0L, 0L, fillQuantity,
                0L, 0L, 0L, 0L);
    }

    /**
     * 构造带真实买卖双方结算信息的成交事件。
     *
     * @param eventId 状态机事件幂等标识
     * @param orderId 成交所属订单
     * @param fillQuantity 本次成交数量
     * @param tradeId 资金结算幂等标识
     * @param buyerUserId 买方账户，必须与订单用户一致
     * @param sellerUserId 卖方账户
     * @param settlementAmount 买方冻结并转给卖方的成交金额
     * @return 带资金结算事实的成交事件
     */
    public static OrderEvent matchFilled(long eventId,
                                         long orderId,
                                         long fillQuantity,
                                         long tradeId,
                                         long buyerUserId,
                                         long sellerUserId,
                                         long settlementAmount) {
        return new OrderEvent(eventId, orderId, EventType.MATCH_FILLED,
                0L, null, 0L, 0L, fillQuantity,
                tradeId, buyerUserId, sellerUserId, settlementAmount);
    }

    /**
     * 构造风控冻结事件。
     *
     * @param eventId 风控事件幂等标识，必须与成交事件标识区分
     * @param orderId 需要进入风控冻结的订单标识
     * @return 风控冻结类型订单事件
     */
    public static OrderEvent riskHold(long eventId, long orderId) {
        return new OrderEvent(eventId, orderId, EventType.RISK_HOLD,
                0L, null, 0L, 0L, 0L,
                0L, 0L, 0L, 0L);
    }

    /**
     * 构造风控审批通过后的放行事件。
     *
     * @param eventId 审批事件幂等标识
     * @param orderId 需要从风控挂起恢复的订单标识
     * @return 风控放行类型订单事件
     */
    public static OrderEvent riskReleased(long eventId, long orderId) {
        return new OrderEvent(eventId, orderId, EventType.RISK_RELEASED,
                0L, null, 0L, 0L, 0L,
                0L, 0L, 0L, 0L);
    }

    /**
     * 获取事件标识。
     *
     * @return 事件幂等标识
     */
    public long getEventId() {
        return eventId;
    }

    /**
     * 获取订单标识。
     *
     * @return 订单标识
     */
    public long getOrderId() {
        return orderId;
    }

    /**
     * 获取事件类型。
     *
     * @return 订单事件类型
     */
    public EventType getType() {
        return type;
    }

    /**
     * 获取用户标识。
     *
     * @return 用户账户标识
     */
    public long getUserId() {
        return userId;
    }

    /**
     * 获取交易对。
     *
     * @return 交易对标识
     */
    public String getSymbol() {
        return symbol;
    }

    /**
     * 获取订单价格。
     *
     * @return 最小价格单位表示的订单价格
     */
    public long getPrice() {
        return price;
    }

    /**
     * 获取订单数量。
     *
     * @return 最小数量单位表示的订单数量
     */
    public long getQuantity() {
        return quantity;
    }

    /**
     * 获取成交数量。
     *
     * @return 最小数量单位表示的本次成交数量
     */
    public long getFillQuantity() {
        return fillQuantity;
    }

    /** 获取成交结算幂等标识；兼容非结算事件时返回 0。 */
    public long getTradeId() {
        return tradeId;
    }

    /** 获取成交买方账户；兼容非结算事件时返回 0。 */
    public long getBuyerUserId() {
        return buyerUserId;
    }

    /** 获取成交卖方账户；兼容非结算事件时返回 0。 */
    public long getSellerUserId() {
        return sellerUserId;
    }

    /** 获取成交结算金额；兼容非结算事件时返回 0。 */
    public long getSettlementAmount() {
        return settlementAmount;
    }
}
