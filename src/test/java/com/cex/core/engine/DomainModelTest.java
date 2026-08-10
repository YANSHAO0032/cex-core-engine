package com.cex.core.engine;

import com.cex.core.engine.account.Account;
import com.cex.core.engine.order.Order;
import com.cex.core.engine.order.OrderState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 领域模型结构测试工具。
 *
 * <p>仅验证字段读取和枚举值，不执行资金结算、状态流转或外部副作用；测试类无共享可变状态。</p>
 */
class DomainModelTest {

    /** 验证账户领域对象的用户标识和余额字段。 */
    @Test
    void accountExposesOnlyItsDeclaredFields() {
        Account account = new Account(7L, 1_000L, 200L);

        assertEquals(7L, account.getUserId());
        assertEquals(1_000L, account.getAvailable());
        assertEquals(200L, account.getFrozen());
    }

    /** 验证订单领域对象的静态信息和当前状态字段。 */
    @Test
    void orderExposesOnlyItsDeclaredFields() {
        Order order = new Order(11L, 7L, "BTC-USDT", 50_000L,
                10L, 3L, OrderState.PARTIAL_FILLED);

        assertEquals(11L, order.getOrderId());
        assertEquals(7L, order.getUserId());
        assertEquals("BTC-USDT", order.getSymbol());
        assertEquals(50_000L, order.getPrice());
        assertEquals(10L, order.getQuantity());
        assertEquals(3L, order.getFilledQuantity());
        assertEquals(OrderState.PARTIAL_FILLED, order.getState());
    }

    /** 验证订单状态枚举完整覆盖核心生命周期值。 */
    @Test
    void orderStateContainsTheRequiredValues() {
        assertArrayEquals(new OrderState[]{
                OrderState.INIT,
                OrderState.CREATED,
                OrderState.PARTIAL_FILLED,
                OrderState.FILLED,
                OrderState.CANCELLED,
                OrderState.RISK_HOLD
        }, OrderState.values());
    }
}
