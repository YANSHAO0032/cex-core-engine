package com.cex.core.engine.risk;

/** Supplies a simulated manual or senior-risk decision for an approval task. */
@FunctionalInterface
public interface ApprovalDecisionProvider {

    /**
     * Decide how to handle a risk-held order.
     *
     * @param task pending approval task
     * @return approval decision; null is treated as task failure
     */
    ApprovalDecision decide(ApprovalTask task);
}
