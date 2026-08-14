package com.cex.core.engine.risk;

import java.util.concurrent.atomic.AtomicReference;

/** In-memory approval task created after an order enters RISK_HOLD. */
public final class ApprovalTask {

    private final long taskId;
    private final long approvalEventId;
    private final long userId;
    private final long orderId;
    private final long amount;
    private final long createdAtMillis;
    private final AtomicReference<ApprovalTaskStatus> status =
            new AtomicReference<>(ApprovalTaskStatus.PENDING);
    private volatile ApprovalDecision decision;
    private volatile String failureReason;

    ApprovalTask(long taskId,
                 long approvalEventId,
                 long userId,
                 long orderId,
                 long amount,
                 long createdAtMillis) {
        this.taskId = taskId;
        this.approvalEventId = approvalEventId;
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
        this.createdAtMillis = createdAtMillis;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getApprovalEventId() {
        return approvalEventId;
    }

    public long getUserId() {
        return userId;
    }

    public long getOrderId() {
        return orderId;
    }

    public long getAmount() {
        return amount;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public ApprovalTaskStatus getStatus() {
        return status.get();
    }

    public ApprovalDecision getDecision() {
        return decision;
    }

    public String getFailureReason() {
        return failureReason;
    }

    boolean isPending() {
        return status.get() == ApprovalTaskStatus.PENDING;
    }

    void markCompleted(ApprovalDecision completedDecision) {
        this.decision = completedDecision;
        if (completedDecision == ApprovalDecision.APPROVED) {
            status.compareAndSet(ApprovalTaskStatus.PENDING,
                    ApprovalTaskStatus.APPROVED);
        } else if (completedDecision == ApprovalDecision.REJECTED) {
            status.compareAndSet(ApprovalTaskStatus.PENDING,
                    ApprovalTaskStatus.REJECTED);
        }
    }

    void markFailed(String reason) {
        this.failureReason = reason;
        status.compareAndSet(ApprovalTaskStatus.PENDING, ApprovalTaskStatus.FAILED);
    }
}
