package com.cex.core.risk;

import com.cex.core.order.OrderEvent;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 以有界异步队列执行人工审批任务的服务。
 * 核心能力：调度审批、统计提交与完成数量并等待静止；线程安全：依赖线程池和原子计数器支持并发调用；使用限制：关闭后不得提交任务。
 */
public final class ApprovalService implements AutoCloseable {
    /** 执行审批任务的固定线程池及其有界等待队列。 */
    private final ThreadPoolExecutor executor;
    /** 已被服务接收的审批任务总数。 */
    private final AtomicLong submitted = new AtomicLong();
    /** 已执行完毕（含失败清理）的审批任务总数。 */
    private final AtomicLong completed = new AtomicLong();

    /**
     * 创建审批执行服务。
     *
     * @param workerCount 审批工作线程数，必须为正数
     * @param queueCapacity 审批等待队列容量，必须为正数
     * @throws IllegalArgumentException 当线程数或队列容量不为正数时抛出
     */
    public ApprovalService(int workerCount, int queueCapacity) {
        if (workerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("workerCount and queueCapacity must be positive");
        }
        // 有界队列满时由提交线程执行，避免无界积压与丢失审批事件。
        executor = new ThreadPoolExecutor(workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 异步提交一项审批，并将审批结果事件回流到统一订单入口。
     *
     * @param source 触发审批的原始订单事件
     * @param policy 审批决策策略
     * @param sink 接收审批结果事件的回流端
     * @throws NullPointerException 当任一必需参数为 {@code null} 时抛出
     * @throws IllegalStateException 当服务已关闭时抛出
     * @note 任务计数在提交前递增、在 finally 中完成，支持并发静止判定与审批结果回流。
     */
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

    /** 获取已提交的审批任务数量。
     * @return 已提交任务总数
     */
    public long submittedCount() {
        return submitted.get();
    }

    /**
     * 等待全部已提交审批任务完成且执行器队列清空。
     * @param timeout 最长等待时长
     * @param unit 等待时长单位
     * @throws InterruptedException 当当前线程在等待期间被中断时抛出
     * @throws IllegalStateException 当超时后执行器仍未静止时抛出
     * @note 结合原子完成计数、活动线程数和有界队列长度判定静止，不主动关闭执行器。
     */
    public void awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (completed.get() < submitted.get() || executor.getActiveCount() != 0 || !executor.getQueue().isEmpty()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("approval executor did not quiesce");
            }
            Thread.yield();
        }
    }

    /** 获取当前等待审批的任务数量。
     * @return 有界审批队列中的任务数
     */
    public int queueSize() {
        return executor.getQueue().size();
    }

    /**
     * 停止接收新审批任务。
     *
     * @note 使用 shutdown 保留已入队任务，保证其审批结果仍可回流。
     */
    @Override
    public void close() {
        executor.shutdown();
    }
}
