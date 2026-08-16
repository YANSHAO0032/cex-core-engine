package com.cex.core.risk;

import java.util.Objects;

/**
 * 风控管道的轻量评估门面。
 * 核心能力：向订单流程提供统一风险判定入口；线程安全：委托给支持并发读写的 {@link RiskPipeline}；使用限制：上下文应反映当前订单与已结算窗口快照。
 */
public final class RiskEngine {
    /** 支撑统一评估入口的可动态替换规则管道。 */
    private final RiskPipeline pipeline;

    /**
     * 创建风控引擎门面。
     *
     * @param pipeline 要委托的风险规则管道
     * @throws NullPointerException 当管道为 {@code null} 时抛出
     */
    public RiskEngine(RiskPipeline pipeline) {
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    }

    /** 按当前规则管道评估订单风险。
     * @param context 当前订单的风险评估快照
     * @return 风险通过或挂起决定
     */
    public RiskDecision evaluate(RiskContext context) {
        return pipeline.evaluate(context);
    }

    /** 获取底层规则管道。
     * @return 当前风控管道实例
     */
    public RiskPipeline pipeline() {
        return pipeline;
    }
}
