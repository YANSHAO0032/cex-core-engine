package com.cex.core.engine.risk;

/** Decision returned by the in-memory approval workflow. */
public enum ApprovalDecision {
    /** Release the risk-held order and debit frozen funds. */
    APPROVED,
    /** Cancel the risk-held order and return frozen funds to available balance. */
    REJECTED
}
