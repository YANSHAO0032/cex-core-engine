package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalResult;
import org.junit.jupiter.api.Test;

/**
 * 验证强类型交易输入的值语义与边界校验。
 *
 * <p>核心能力：覆盖资产、订单、成交、撤单和审批输入的关键正负边界。</p>
 * <p>线程安全：所有被测类型均为不可变值对象，测试不共享可变状态。</p>
 * <p>使用限制：仅验证输入模型，不覆盖订单状态迁移、账本或成交结算。</p>
 */
class OrderInputValidationTest {

    /** 用于交易对和订单测试的基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 用于交易对和订单测试的报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");

    /** 场景：提交订单应保留预留金额、风控名义金额和权威序号。 */
    @Test
    void submissionCarriesReserveAndRiskNotional() {
        OrderSubmission submission = new OrderSubmission(
                11L, 21L, OrderSide.BUY, new TradingPair(BTC, USDT),
                5L, 500L, 450L, 1L, 100L);

        assertEquals(500L, submission.reservedAmount());
        assertEquals(450L, submission.riskQuoteAmount());
        assertEquals(1L, submission.orderSequence());
    }

    /** 场景：成交的双订单、数量和序号均应满足权威输入约束。 */
    @Test
    void executionRequiresDifferentOrdersPositiveQuantitiesAndSequences() {
        assertThrows(IllegalArgumentException.class, () -> new TradeExecution(
                1L, 10L, 10L, new TradingPair(BTC, USDT),
                1L, 100L, 2L, 2L, 100L));
        assertThrows(IllegalArgumentException.class, () -> new TradeExecution(
                1L, 10L, 20L, new TradingPair(BTC, USDT),
                0L, 100L, 2L, 2L, 100L));
    }

    /** 场景：资产代码和交易对必须采用规范且可区分的资产标识。 */
    @Test
    void assetCodesAndTradingPairsAreCanonical() {
        assertEquals("BTC", BTC.value());
        assertThrows(IllegalArgumentException.class, () -> new AssetId("btc"));
        assertThrows(IllegalArgumentException.class, () -> new TradingPair(BTC, BTC));
    }

    /** 场景：订单提交的全部数值标识和金额必须为正，时间戳必须非负。 */
    @Test
    void submissionValidatesRequiredValues() {
        TradingPair pair = new TradingPair(BTC, USDT);

        assertThrows(IllegalArgumentException.class, () -> new OrderSubmission(
                0L, 21L, OrderSide.BUY, pair, 5L, 500L, 450L, 1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair, 5L, 500L, 450L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new OrderSubmission(
                11L, 21L, OrderSide.BUY, pair, 5L, 500L, 450L, 1L, -1L));
    }

    /** 场景：成交的全部标识、金额和序号必须为正，执行时间可以为零。 */
    @Test
    void executionValidatesAllInputValues() {
        TradingPair pair = new TradingPair(BTC, USDT);

        assertThrows(IllegalArgumentException.class, () -> new TradeExecution(
                0L, 10L, 20L, pair, 1L, 100L, 2L, 2L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new TradeExecution(
                1L, 10L, 20L, pair, 1L, 100L, 2L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new TradeExecution(
                1L, 10L, 20L, pair, 1L, 100L, 2L, 2L, -1L));
    }

    /** 场景：成交引用和撤单确认属于带权威序号的单订单事件。 */
    @Test
    void sequencedEventsExposeTheirOrderIdentity() {
        SequencedOrderEvent tradeReference = new TradeOrderReference(31L, 11L, 2L);
        SequencedOrderEvent confirmation = new CancelConfirmation(41L, 11L, 3L, 10L);

        assertEquals(11L, tradeReference.orderId());
        assertEquals(2L, tradeReference.orderSequence());
        assertEquals(11L, confirmation.orderId());
        assertEquals(3L, confirmation.orderSequence());
    }

    /** 场景：撤单请求和确认的标识与时间必须符合幂等撤单边界。 */
    @Test
    void cancelInputsValidateIdentifiersSequencesAndTimestamps() {
        assertThrows(IllegalArgumentException.class, () -> new CancelRequest(0L, 11L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new CancelRequest(41L, 11L, -1L));
        assertThrows(IllegalArgumentException.class, () -> new CancelConfirmation(41L, 11L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new CancelConfirmation(41L, 11L, 3L, -1L));
    }

    /** 场景：撤单网关应将提交的请求传递给外部接收方。 */
    @Test
    void cancelRequestSinkSubmitsRequestToReceiver() {
        CancelRequest[] received = new CancelRequest[1];
        CancelRequestSink sink = request -> received[0] = request;
        CancelRequest request = new CancelRequest(41L, 11L, 10L);

        sink.submit(request);

        assertEquals(request, received[0]);
    }

    /** 场景：审批结果的订单标识为正，结论非空，决定时间非负。 */
    @Test
    void approvalResultValidatesItsTypedCallbackPayload() {
        ApprovalResult result = new ApprovalResult(11L, ApprovalDecision.PASS, 0L);

        assertEquals(ApprovalDecision.PASS, result.decision());
        assertThrows(IllegalArgumentException.class, () -> new ApprovalResult(0L, ApprovalDecision.PASS, 0L));
        assertThrows(IllegalArgumentException.class, () -> new ApprovalResult(11L, ApprovalDecision.PASS, -1L));
    }
}
