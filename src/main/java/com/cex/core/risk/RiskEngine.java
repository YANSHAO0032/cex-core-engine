package com.cex.core.risk;

import java.util.Objects;

/** Small facade for evaluating the dynamically replaceable risk pipeline. */
public final class RiskEngine {
    private final RiskPipeline pipeline;

    public RiskEngine(RiskPipeline pipeline) {
        this.pipeline = Objects.requireNonNull(pipeline, "pipeline");
    }

    public RiskDecision evaluate(RiskContext context) {
        return pipeline.evaluate(context);
    }

    public RiskPipeline pipeline() {
        return pipeline;
    }
}
