package com.cex.core.risk;

import com.cex.core.order.OrderSubmission;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    /** 仅供同包测试协调任务接收与关闭竞态的观察器。 */
    private final SubmissionObserver submissionObserver;
    /** 协调任务接收线性化边界与执行器关闭时机的生命周期监视器。 */
    private final Object lifecycleMonitor = new Object();
    /** 已进入接收边界但尚未从执行器提交调用返回的线程数。 */
    private int submissionsInFlight;
    /** 是否已停止接收新审批任务，默认 false 表示仍接收。 */
    private boolean closed;

    /**
     * 创建审批执行服务。
     *
     * @param workerCount 审批工作线程数，必须为正数
     * @param queueCapacity 审批等待队列容量，必须为正数
     * @throws IllegalArgumentException 当线程数或队列容量不为正数时抛出
     */
    public ApprovalService(int workerCount, int queueCapacity) {
        this(workerCount, queueCapacity, () -> { });
    }

    /**
     * 使用接收边界观察器创建审批执行服务。
     *
     * @param workerCount 审批工作线程数，必须为正数
     * @param queueCapacity 审批等待队列容量，必须为正数
     * @param submissionObserver 仅测试使用的任务接收边界观察器
     * @throws IllegalArgumentException 当线程数或队列容量不为正数时抛出
     * @throws NullPointerException 当观察器为 {@code null} 时抛出
     */
    ApprovalService(int workerCount, int queueCapacity, SubmissionObserver submissionObserver) {
        if (workerCount <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("workerCount and queueCapacity must be positive");
        }
        this.submissionObserver = Objects.requireNonNull(
                submissionObserver, "submissionObserver");
        // 有界队列满时由提交线程执行，避免无界积压与丢失审批事件。
        executor = new ThreadPoolExecutor(workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 异步提交一项审批，并将强类型审批结果回流到订单入口。
     *
     * @param submission 触发审批的强类型订单提交
     * @param policy 审批决策策略
     * @param sink 接收强类型审批结果的回流端
     * @throws NullPointerException 当任一必需参数为 {@code null} 时抛出
     * @throws IllegalStateException 当服务已关闭时抛出
     * @note 关闭检查与执行器接收之间使用短生命周期预约建立线性化边界；任务计数仅覆盖已接收任务，并在执行 finally 中完成。
     */
    public void submit(
            OrderSubmission submission,
            ApprovalPolicy policy,
            Consumer<ApprovalResult> sink) {
        Objects.requireNonNull(submission, "submission");
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(sink, "sink");
        beginSubmission();
        boolean counted = false;
        AtomicBoolean taskStarted = new AtomicBoolean();
        try {
            submissionObserver.afterAcceptance();
            submitted.incrementAndGet();
            counted = true;
            executor.execute(() -> {
                taskStarted.set(true);
                try {
                    new ApprovalTask(submission, policy, sink).run();
                } finally {
                    completed.incrementAndGet();
                }
            });
        } catch (RuntimeException | Error submissionFailure) {
            if (counted && !taskStarted.get()) {
                submitted.decrementAndGet();
            }
            throw submissionFailure;
        } finally {
            finishSubmission();
        }
    }

    /** 获取已提交的审批任务数量。
     * @return 已提交任务总数
     * @note 使用原子读取返回弱一致累计值，不阻塞并发提交或审批执行。
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
     * @note 结合已越过接收边界的提交预约、原子完成计数、活动线程数和有界队列长度判定静止，不主动关闭执行器。
     * @note 等待循环不持有生命周期监视器，审批回流线程可安全重入 {@link #close()}。
     */
    public void awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (hasSubmissionsInFlight()
                || completed.get() < submitted.get()
                || executor.getActiveCount() != 0
                || !executor.getQueue().isEmpty()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("approval executor did not quiesce");
            }
            Thread.yield();
        }
    }

    /** 获取当前等待审批的任务数量。
     * @return 有界审批队列中的任务数
     * @note 线程池队列提供并发安全的瞬时大小，该值不包含正在运行或 CallerRuns 回流执行的任务。
     */
    public int queueSize() {
        return executor.getQueue().size();
    }

    /**
     * 停止接收新审批任务。
     *
     * @note 先在线性化边界停止接收；若仍有提交正在调用 execute，由最后一个提交者执行 shutdown，保留所有已接收任务并避免回流端重入死锁。
     */
    @Override
    public void close() {
        boolean shutdownExecutor;
        synchronized (lifecycleMonitor) {
            if (closed) {
                return;
            }
            closed = true;
            shutdownExecutor = submissionsInFlight == 0;
        }
        if (shutdownExecutor) {
            executor.shutdown();
        }
    }

    /**
     * 在线性化边界接收一个提交并预约执行器仍保持开放。
     *
     * @throws IllegalStateException 当服务已停止接收新任务时抛出
     */
    private void beginSubmission() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("approval service is shut down");
            }
            submissionsInFlight++;
        }
    }

    /**
     * 释放一个提交预约，并在关闭已发生时由最后一个提交者关闭执行器。
     *
     * @note 不等待任务执行完成；CallerRunsPolicy 回流端即使重入 close 也不会等待自身预约。
     */
    private void finishSubmission() {
        boolean shutdownExecutor;
        synchronized (lifecycleMonitor) {
            submissionsInFlight--;
            shutdownExecutor = closed && submissionsInFlight == 0;
        }
        if (shutdownExecutor) {
            executor.shutdown();
        }
    }

    /**
     * 判断是否仍有已被服务接收但尚未从执行器提交调用返回的任务。
     *
     * @return 存在已接收的执行器边界内提交时为 {@code true}
     */
    private boolean hasSubmissionsInFlight() {
        synchronized (lifecycleMonitor) {
            return submissionsInFlight != 0;
        }
    }

    /**
     * 观察审批提交已越过接收线性化边界的测试回调。
     *
     * <p>核心能力：为确定性并发测试暴露接收与执行器提交之间的窄窗口。</p>
     * <p>线程安全：实现可能在任意提交线程中被同步调用，必须自行保证线程安全。</p>
     * <p>使用限制：仅供同包测试协调交错，生产构造器始终使用无操作实现。</p>
     */
    @FunctionalInterface
    interface SubmissionObserver {
        /** 观察任务已通过关闭检查但尚未交给执行器。 */
        void afterAcceptance();
    }
}
