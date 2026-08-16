package com.cex.core.risk;

import com.cex.core.order.OrderEvent;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class ApprovalService implements AutoCloseable {
    private final ThreadPoolExecutor executor;
    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();

    public ApprovalService(int workerCount, int queueCapacity) {
        if (workerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("workerCount and queueCapacity must be positive");
        }
        executor = new ThreadPoolExecutor(workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void submit(OrderEvent source, ApprovalPolicy policy, Consumer<OrderEvent> sink) {
        Objects.requireNonNull(source, "source");
        if (executor.isShutdown()) {
            throw new IllegalStateException("approval service is shut down");
        }
        submitted.incrementAndGet();
        executor.execute(() -> {
            try {
                new ApprovalTask(source, policy, sink).run();
            } finally {
                completed.incrementAndGet();
            }
        });
    }

    public long submittedCount() {
        return submitted.get();
    }

    public void awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (completed.get() < submitted.get() || executor.getActiveCount() != 0 || !executor.getQueue().isEmpty()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("approval executor did not quiesce");
            }
            Thread.yield();
        }
    }

    public int queueSize() {
        return executor.getQueue().size();
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
