package com.cex.core.engine.order;

/**
 * 订单不可变领域快照。
 *
 * <p>保存订单静态信息和当前成交状态，不包含状态转换、资金扣减或并发控制逻辑。</p>
 */
public final class Order {

    /** 订单业务标识。 */
    private final long orderId;
    /** 下单用户标识。 */
    private final long userId;
    /** 交易对标识。 */
    private final String symbol;
    /** 订单价格，使用最小价格单位表示。 */
    private final long price;
    /** 订单原始数量，使用最小数量单位表示。 */
    private final long quantity;
    /** 已成交数量，使用最小数量单位表示。 */
    private final long filledQuantity;
    /** 订单当前生命周期状态。 */
    private final OrderState state;

    /**
     * 创建订单不可变快照。
     *
     * @param orderId 订单业务标识
     * @param userId 下单用户标识
     * @param symbol 交易对标识
     * @param price 订单价格，使用最小价格单位
     * @param quantity 订单原始数量，使用最小数量单位
     * @param filledQuantity 已成交数量，使用最小数量单位
     * @param state 当前订单状态
     */
    public Order(long orderId,
                 long userId,
                 String symbol,
                 long price,
                 long quantity,
                 long filledQuantity,
                 OrderState state) {
        this.orderId = orderId;
        this.userId = userId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.filledQuantity = filledQuantity;
        this.state = state;
    }

    /**
     * 获取订单标识。
     *
     * @return 订单业务标识
     */
    public long getOrderId() {
        return orderId;
    }

    /**
     * 获取用户标识。
     *
     * @return 下单用户标识
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
     * 获取订单原始数量。
     *
     * @return 最小数量单位表示的订单数量
     */
    public long getQuantity() {
        return quantity;
    }

    /**
     * 获取已成交数量。
     *
     * @return 最小数量单位表示的已成交数量
     */
    public long getFilledQuantity() {
        return filledQuantity;
    }

    /**
     * 获取订单当前状态。
     *
     * @return 订单生命周期状态
     */
    public OrderState getState() {
        return state;
    }
}
