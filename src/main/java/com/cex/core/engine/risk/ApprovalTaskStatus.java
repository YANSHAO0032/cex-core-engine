package com.cex.core.engine.risk;

/** Lifecycle status for an in-memory approval task. */
public enum ApprovalTaskStatus {
    /** The task has been accepted but not processed by the worker. */
    PENDING,
    /** Approval passed and the side effects were applied. */
    APPROVED,
    /** Approval rejected and the side effects were applied. */
    REJECTED,
    /** Approval could not be completed safely. */
    FAILED
}
