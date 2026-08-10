package com.cex.core.engine.risk;

import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.order.OrderState;
import com.cex.core.engine.order.OrderStateMachine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 内存风控滑动窗口测试工具。
 *
 * <p>验证 10 秒窗口过期、成交幂等、阈值冻结和窗口金额回收逻辑。</p>
 */
class RiskEngineTest {

    /** 验证过期逻辑只移除 Deque 队头旧成交。 */
    @Test
    void expiresOnlyTheOldPrefixOfTheDeque() {
        SlidingWindow window = new SlidingWindow(10_000L);

        window.record(1L, 60L, 1_000L);
        window.record(2L, 50L, 5_000L);
        assertEquals(110L, window.getTotalAmount());

        assertEquals(1, window.expireOldTransactions(11_001L));
        assertEquals(50L, window.getTotalAmount());
        assertEquals(1, window.size());
    }

    /** 验证重复 tradeId 不会重复累计成交金额。 */
    @Test
    void duplicateTradeIsNotCountedTwice() {
        RiskEngine riskEngine = new RiskEngine(100L, 10_000L, 16);

        RiskDecision first = riskEngine.recordTrade(7L, 10L, 100L, 60L, 1_000L);
        RiskDecision duplicate = riskEngine.recordTrade(7L, 10L, 100L, 60L, 2_000L);

        assertFalse(first.isRiskHold());
        assertFalse(duplicate.isNewTransaction());
        assertEquals(60L, duplicate.getWindowAmount());
    }

    /** 验证窗口金额超过阈值后订单进入 RISK_HOLD。 */
    @Test
    void movesOrderToRiskHoldAfterThresholdIsExceeded() {
        OrderStateMachine stateMachine = new OrderStateMachine();
        stateMachine.apply(OrderEvent.created(1L, 10L, 7L, "BTC-USDT", 50_000L, 10L));
        RiskEngine riskEngine = new RiskEngine(100L);

        RiskDecision decision = riskEngine.recordTradeAndApply(
                stateMachine, 9001L, 7L, 10L, 100L, 101L, 1_000L);

        assertTrue(decision.isRiskHold());
        assertEquals(RiskState.RISK_HOLD, decision.getState());
        assertEquals(OrderState.RISK_HOLD, stateMachine.get(10L).getState());
    }

    /** 验证过期成交清理后用户窗口金额能够回到零。 */
    @Test
    void expiredAmountCanBringUserBackBelowThreshold() {
        RiskEngine riskEngine = new RiskEngine(100L, 10_000L, 16);
        riskEngine.recordTrade(7L, 10L, 1L, 101L, 1_000L);

        assertEquals(1, riskEngine.expireUser(7L, 11_001L));
        assertEquals(0L, riskEngine.currentWindowAmount(7L));
    }
}
