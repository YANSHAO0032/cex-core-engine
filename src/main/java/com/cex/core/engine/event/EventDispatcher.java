package com.cex.core.engine.event;

import com.cex.core.engine.order.OrderStateMachine;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 多生产者单消费者订单事件分发器。
 *
 * <p>内部使用有界 MPSC RingBuffer 模拟消息队列，并将事件交给一个状态机消费者；
 * 生产线程安全，消费者线程由本类独占，调用方必须在关闭前停止生产。</p>
 */
public final class EventDispatcher implements AutoCloseable {

    /** 兼容对象事件路径使用的有界事件环形缓冲区。 */
    private final MpscRingBuffer<OrderEvent> ringBuffer;
    /** 负责应用订单事件的状态机。 */
    private final OrderStateMachine stateMachine;
    /** 消费线程运行标识，默认 false 表示尚未启动。 */
    private final AtomicBoolean running = new AtomicBoolean();
    /** 消费线程发生的异常，供调用方在测试或监控中读取。 */
    private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    /** 已成功交给状态机处理的事件数量。 */
    private final LongAdder processedEvents = new LongAdder();
    /** 单消费者线程引用。 */
    private volatile Thread consumerThread;

    /**
     * 创建对象事件分发器。
     *
     * @param stateMachine 订单状态机
     * @param capacity RingBuffer 容量，必须是大于等于 2 的 2 的幂
     * @throws NullPointerException stateMachine 为空时抛出
     * @throws IllegalArgumentException capacity 不符合 RingBuffer 约束时抛出
     */
    public EventDispatcher(OrderStateMachine stateMachine, int capacity) {
        if (stateMachine == null) {
            throw new NullPointerException("stateMachine");
        }
        this.stateMachine = stateMachine;
        this.ringBuffer = new MpscRingBuffer<>(capacity);
    }

    /**
     * 启动单个消费者线程。
     *
     * @note 使用 CAS 保证重复启动不会创建第二个消费者；消费者线程安全地批量消费事件。
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread thread = new Thread(this::consumeLoop, "cex-event-consumer");
        thread.setDaemon(true);
        consumerThread = thread;
        thread.start();
    }

    /**
     * 尝试非阻塞发布订单事件。
     *
     * @param event 待发布的订单事件
     * @return 发布成功返回 true，RingBuffer 满时返回 false
     * @note 多生产者通过 RingBuffer 内部 CAS 竞争序号；调用方不得重复发布同一业务事件。
     */
    public boolean publish(OrderEvent event) {
        return ringBuffer.offer(event);
    }

    /**
     * 以自旋背压方式发布订单事件。
     *
     * @param event 待发布的订单事件
     * @throws InterruptedException 发布线程被中断时抛出，事件可能尚未入队
     * @note RingBuffer 满时持续自旋，必须确保消费者已启动，避免生产线程永久等待。
     */
    public void publishBlocking(OrderEvent event) throws InterruptedException {
        while (!ringBuffer.offer(event)) {
            if (Thread.interrupted()) {
                throw new InterruptedException("event publisher interrupted");
            }
            Thread.onSpinWait();
        }
    }

    /**
     * 在当前线程批量处理已就绪事件。
     *
     * @param limit 本次最多处理的事件数量
     * @return 实际交给状态机处理的事件数量
     * @note 该方法与后台消费者共享单消费者约束，不应与后台消费线程并行调用。
     */
    public int drain(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        int processed = 0;
        while (processed < limit) {
            OrderEvent event = ringBuffer.poll();
            if (event == null) {
                break;
            }
            stateMachine.apply(event);
            processedEvents.increment();
            processed++;
        }
        return processed;
    }

    /**
     * 获取已处理事件数量。
     *
     * @return 已成功交给状态机的事件数量
     */
    public long processedEventCount() {
        return processedEvents.sum();
    }

    /**
     * 获取消费者线程捕获的异常。
     *
     * @return 消费异常；未发生异常时返回 null
     */
    public Throwable getConsumerFailure() {
        return consumerFailure.get();
    }

    /**
     * 判断消费者线程是否处于运行状态。
     *
     * @return 正在运行返回 true，否则返回 false
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 停止消费者并等待其退出。
     *
     * @note 停止前会让消费者继续排空已入队事件；调用方应先停止生产，避免关闭期间持续入队。
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread thread = consumerThread;
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
            try {
                thread.join(5_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 执行消费者主循环，并在退出前排空已发布事件。
     *
     * @note 消费异常只记录一次并终止消费者，生产方需通过 getConsumerFailure 检查故障。
     */
    private void consumeLoop() {
        try {
            while (running.get() || !ringBuffer.isEmpty()) {
                if (drain(256) == 0) {
                    Thread.onSpinWait();
                }
            }
        } catch (Throwable failure) {
            consumerFailure.compareAndSet(null, failure);
        } finally {
            running.set(false);
        }
    }
}
