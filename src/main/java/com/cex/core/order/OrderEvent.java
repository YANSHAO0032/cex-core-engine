package com.cex.core.order;

import com.cex.core.util.MoneyMath;
import java.util.Objects;

/**
 * 进入订单引擎的不可变业务事件。
 * 核心能力是携带订单身份、用户、金额、发生时间和触发类型；不可变设计使其线程安全。
 * 限制：不保证事件顺序，且金额必须使用底层货币最小单位并为正数。
 */
public final class OrderEvent {

    /** 事件所属订单的全局唯一标识。 */
    private final long orderId;
    /** 下单用户的唯一标识，用于账户锁和资金归属。 */
    private final long userId;
    /** 订单金额，单位为货币最小单位且必须为正。 */
    private final long amount;
    /** 事件产生时间的毫秒时间戳，必须非负。 */
    private final long eventTimeMillis;
    /** 驱动订单状态机的业务事件类型。 */
    private final OrderEventType type;

    /**
     * 创建并校验订单事件。
     *
     * @param orderId 订单唯一标识，必须为正
     * @param userId 用户唯一标识，必须为正
     * @param amount 订单金额，单位为货币最小单位且必须为正
     * @param eventTimeMillis 事件发生的非负毫秒时间戳
     * @param type 业务事件类型，不能为空
     */
    public OrderEvent(long orderId, long userId, long amount, long eventTimeMillis, OrderEventType type) {
        this.orderId = requirePositive(orderId, "orderId");
        this.userId = requirePositive(userId, "userId");
        this.amount = MoneyMath.requirePositive(amount);
        this.eventTimeMillis = MoneyMath.requireNonNegative(eventTimeMillis);
        this.type = Objects.requireNonNull(type, "type");
    }

    /**
     * 返回订单唯一标识。
     *
     * @return 订单 ID
     */
    public long orderId() {
        return orderId;
    }

    /**
     * 返回用户唯一标识。
     *
     * @return 用户 ID
     */
    public long userId() {
        return userId;
    }

    /**
     * 返回订单金额。
     *
     * @return 以货币最小单位表示的正金额
     */
    public long amount() {
        return amount;
    }

    /**
     * 返回事件发生时间。
     *
     * @return 非负毫秒时间戳
     */
    public long eventTimeMillis() {
        return eventTimeMillis;
    }

    /**
     * 返回业务事件类型。
     *
     * @return 订单状态机事件类型
     */
    public OrderEventType type() {
        return type;
    }

    /**
     * 校验标识值为正数。
     *
     * @param value 待校验数值
     * @param fieldName 用于异常信息的字段名
     * @return 已确认的正数值
     * @throws IllegalArgumentException 当数值不为正时
     */
    private static long requirePositive(long value, String fieldName) {
        if (value <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return value;
    }
}
