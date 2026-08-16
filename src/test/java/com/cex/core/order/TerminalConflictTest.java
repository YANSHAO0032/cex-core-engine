package com.cex.core.order;

import com.cex.core.account.AccountLedger;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.RiskPipeline;
import com.cex.core.risk.ManualClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证成交与取消冲突时，已提交的资金终态不会被反向补偿。
 *
 * <p>核心能力：覆盖成交与取消互斥终态裁决，并校验结算或解冻副作仅执行一次。</p>
 * <p>线程安全：引擎依靠原子副作位和分片锁保护终态资金操作，本类校验迟到事件不能翻转已提交结果。</p>
 * <p>使用限制：仅验证单订单终态事件顺序，不覆盖多订单批量交易或外部清算系统。</p>
 */
class TerminalConflictTest {
    /** 场景：成交完成后迟到取消不得解冻已结算资金。 */
    @Test
    void lateCancelAfterFillDoesNotUnfreeze() {
        Fixture f = new Fixture();
        try {
            f.engine.process(e(OrderEventType.ORDER_CREATED));
            f.engine.process(e(OrderEventType.MATCH_FILLED));
            f.engine.process(e(OrderEventType.ORDER_CANCELLED));
            assertEquals(OrderStatus.FILLED, f.engine.order(1L).status());
            assertEquals(1L, f.engine.metrics().settleCount());
            assertEquals(0L, f.engine.metrics().unfreezeCount());
            assertEquals(1L, f.engine.metrics().conflictingTerminalEvents());
        } finally { f.close(); }
    }

    /** 场景：取消完成后迟到成交不得对已解冻资金再次结算。 */
    @Test
    void lateFillAfterCancelDoesNotSettle() {
        Fixture f = new Fixture();
        try {
            f.engine.process(e(OrderEventType.ORDER_CREATED));
            f.engine.process(e(OrderEventType.ORDER_CANCELLED));
            f.engine.process(e(OrderEventType.MATCH_FILLED));
            assertEquals(OrderStatus.CANCELED, f.engine.order(1L).status());
            assertEquals(0L, f.engine.metrics().settleCount());
            assertEquals(1L, f.engine.metrics().unfreezeCount());
            assertEquals(1L, f.engine.metrics().conflictingTerminalEvents());
        } finally { f.close(); }
    }

    /**
     * 构造固定元数据的测试事件。
     *
     * @param type 事件类型
     * @return 测试订单事件
     */
    private static OrderEvent e(OrderEventType type) {
        return new OrderEvent(1L, 1L, 100L, 1L, type);
    }

    /** 终态冲突测试的账本、审批服务和引擎夹具。 */
    private static final class Fixture implements AutoCloseable {
        /** 使用的账户账本。 */
        private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
        /** 使用的异步审批服务。 */
        private final ApprovalService approvals = new ApprovalService(1, 8);
        /** 被测订单引擎。 */
        private final OrderEngine engine;
        /** 创建并初始化测试账户和引擎。 */
        private Fixture() {
            ledger.createAccount(1L, 1000L);
            engine = new OrderEngine(ledger, new RiskPipeline(), new ManualClock(1L), approvals,
                    event -> ApprovalDecision.PASS);
        }
        /** 关闭夹具创建的引擎资源。 */
        @Override public void close() { engine.close(); }
    }
}
