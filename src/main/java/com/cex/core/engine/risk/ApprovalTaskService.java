package com.cex.core.engine.risk;

import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.ledger.LedgerService;
import com.cex.core.engine.order.OrderStateMachine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Pure in-memory asynchronous approval workflow for risk-held orders. */
public final class ApprovalTaskService implements AutoCloseable {

    private final OrderStateMachine stateMachine;
    private final LedgerService ledgerService;
    private final ApprovalDecisionProvider decisionProvider;
    private final ConcurrentHashMap<Long, ApprovalTask> tasks =
            new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<ApprovalTask> queue =
            new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    private volatile Thread workerThread;

    public ApprovalTaskService(OrderStateMachine stateMachine,
                               LedgerService ledgerService,
                               ApprovalDecisionProvider decisionProvider) {
        if (stateMachine == null) {
            throw new NullPointerException("stateMachine");
        }
        if (ledgerService == null) {
            throw new NullPointerException("ledgerService");
        }
        if (decisionProvider == null) {
            throw new NullPointerException("decisionProvider");
        }
        this.stateMachine = stateMachine;
        this.ledgerService = ledgerService;
        this.decisionProvider = decisionProvider;
    }

    /** Start the single async approval worker. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread thread = new Thread(this::runWorker, "cex-approval-task-worker");
        thread.setDaemon(true);
        workerThread = thread;
        thread.start();
    }

    /**
     * Submit a task idempotently. A duplicate taskId returns the existing task
     * and does not enqueue another approval side effect.
     */
    public ApprovalTask submit(long taskId,
                               long approvalEventId,
                               long userId,
                               long orderId,
                               long amount,
                               long createdAtMillis) {
        if (amount <= 0L) {
            throw new IllegalArgumentException("amount must be positive");
        }
        ApprovalTask task = new ApprovalTask(taskId, approvalEventId, userId,
                orderId, amount, createdAtMillis);
        ApprovalTask existing = tasks.putIfAbsent(taskId, task);
        if (existing != null) {
            return existing;
        }
        queue.offer(task);
        return task;
    }

    public ApprovalTask get(long taskId) {
        return tasks.get(taskId);
    }

    public int pendingQueueSize() {
        return queue.size();
    }

    public Throwable getWorkerFailure() {
        return workerFailure.get();
    }

    public boolean awaitStatus(long taskId,
                               ApprovalTaskStatus expected,
                               long timeoutMillis) {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            ApprovalTask task = tasks.get(taskId);
            if (task != null && task.getStatus() == expected) {
                return true;
            }
            Thread.onSpinWait();
        }
        ApprovalTask task = tasks.get(taskId);
        return task != null && task.getStatus() == expected;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread thread = workerThread;
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
            try {
                thread.join(5_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runWorker() {
        try {
            while (running.get() || !queue.isEmpty()) {
                ApprovalTask task = queue.poll(100L, TimeUnit.MILLISECONDS);
                if (task != null) {
                    process(task);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
        }
    }

    private void process(ApprovalTask task) {
        if (!task.isPending()) {
            return;
        }
        try {
            ApprovalDecision decision = decisionProvider.decide(task);
            if (decision == null) {
                task.markFailed("approval decision is null");
                return;
            }
            if (decision == ApprovalDecision.APPROVED) {
                approve(task);
            } else {
                reject(task);
            }
        } catch (Throwable failure) {
            workerFailure.compareAndSet(null, failure);
            task.markFailed(failure.getClass().getSimpleName() + ": "
                    + failure.getMessage());
        }
    }

    private void approve(ApprovalTask task) {
        if (!ledgerService.tradeDebit(task.getUserId(), task.getAmount())) {
            task.markFailed("insufficient frozen balance for approval debit");
            return;
        }
        stateMachine.apply(OrderEvent.riskReleased(task.getApprovalEventId(),
                task.getOrderId()));
        task.markCompleted(ApprovalDecision.APPROVED);
    }

    private void reject(ApprovalTask task) {
        if (!ledgerService.unfreeze(task.getUserId(), task.getAmount())) {
            task.markFailed("insufficient frozen balance for approval release");
            return;
        }
        stateMachine.apply(OrderEvent.cancelled(task.getApprovalEventId(),
                task.getOrderId()));
        task.markCompleted(ApprovalDecision.REJECTED);
    }
}
