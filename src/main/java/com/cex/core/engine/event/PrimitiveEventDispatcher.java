package com.cex.core.engine.event;

import com.cex.core.engine.order.OrderStateMachine;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 基于 primitive 事件环形缓冲区的低分配单消费者分发器。
 *
 * <p>生产者传递 primitive 字段，消费者调用 OrderStateMachine.applyFast；固定数组复用槽位，
 * 用于减少 GC 并适配 -Xmx256m 高吞吐场景。</p>
 */
public final class PrimitiveEventDispatcher implements AutoCloseable {

    /** 缓存 enum ordinal 到枚举对象的映射，避免热路径重复创建 values 数组。 */
    private static final EventType[] EVENT_TYPES = EventType.values();

    /** primitive 字段事件环形缓冲区。 */
    private final PrimitiveEventRingBuffer ringBuffer;
    /** 负责应用事件事实的订单状态机。 */
    private final OrderStateMachine stateMachine;
    /** 消费线程运行标识，默认 false。 */
    private final AtomicBoolean running = new AtomicBoolean();
    /** 消费线程捕获的异常。 */
    private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    /** 已处理事件计数器。 */
    private final LongAdder processedEvents = new LongAdder();
    /** 复用的消费者回调对象，避免每次 drain 创建回调。 */
    private final PrimitiveEventRingBuffer.EventConsumer consumer = this::dispatch;
    /** 单消费者线程引用。 */
    private volatile Thread consumerThread;

    /**
     * 创建 primitive 事件分发器。
     *
     * @param stateMachine 订单状态机
     * @param capacity primitive RingBuffer 容量，必须为大于等于 2 的 2 的幂
     * @throws NullPointerException stateMachine 为空时抛出
     * @throws IllegalArgumentException capacity 不满足约束时抛出
     */
    public PrimitiveEventDispatcher(OrderStateMachine stateMachine, int capacity) {
        if (stateMachine == null) {
            throw new NullPointerException("stateMachine");
        }
        this.stateMachine = stateMachine;
        this.ringBuffer = new PrimitiveEventRingBuffer(capacity);
    }

    /**
     * 启动唯一消费者线程。
     *
     * @note AtomicBoolean CAS 防止重复启动；所有事件由该消费者按 RingBuffer 顺序提交状态机。
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread thread = new Thread(this::consumeLoop, "cex-primitive-event-consumer");
        thread.setDaemon(true);
        consumerThread = thread;
        thread.start();
    }

    /**
     * 非阻塞发布 primitive 订单事件。
     *
     * @param type 订单事件类型
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param userId 创建事件对应的用户标识
     * @param symbol 创建事件对应的交易对
     * @param price 订单价格，使用最小价格单位
     * @param quantity 订单数量，使用最小数量单位
     * @param fillQuantity 成交数量，使用最小数量单位
     * @return 入队成功返回 true，缓冲区满时返回 false
     * @note 事件字段写入固定 primitive 槽位，不创建 OrderEvent 对象。
     */
    public boolean publish(EventType type,
                           long eventId,
                           long orderId,
                           long userId,
                           String symbol,
                           long price,
                           long quantity,
                           long fillQuantity) {
        return publish(type, eventId, orderId, userId, symbol, price, quantity,
                fillQuantity, 0L, 0L, 0L, 0L);
    }

    /** 发布带买卖双方结算事实的 primitive 订单事件。 */
    public boolean publish(EventType type,
                           long eventId,
                           long orderId,
                           long userId,
                           String symbol,
                           long price,
                           long quantity,
                           long fillQuantity,
                           long tradeId,
                           long buyerUserId,
                           long sellerUserId,
                           long settlementAmount) {
        if (type == null) {
            throw new NullPointerException("type");
        }
        return ringBuffer.offer(type.ordinal(), eventId, orderId, userId, symbol,
                price, quantity, fillQuantity, tradeId, buyerUserId,
                sellerUserId, settlementAmount);
    }

    /**
     * 以自旋背压方式发布 primitive 订单事件。
     *
     * @param type 订单事件类型
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param userId 创建事件对应的用户标识
     * @param symbol 创建事件对应的交易对
     * @param price 订单价格，使用最小价格单位
     * @param quantity 订单数量，使用最小数量单位
     * @param fillQuantity 成交数量，使用最小数量单位
     * @throws InterruptedException 发布线程被中断时抛出
     * @note RingBuffer 满载时自旋等待；调用方需确保消费者线程已经启动。
     */
    public void publishBlocking(EventType type,
                                long eventId,
                                long orderId,
                                long userId,
                                String symbol,
                                long price,
                                 long quantity,
                                 long fillQuantity) throws InterruptedException {
        publishBlocking(type, eventId, orderId, userId, symbol, price, quantity,
                fillQuantity, 0L, 0L, 0L, 0L);
    }

    /** 以自旋背压方式发布带结算事实的 primitive 事件。 */
    public void publishBlocking(EventType type,
                                long eventId,
                                long orderId,
                                long userId,
                                String symbol,
                                long price,
                                long quantity,
                                long fillQuantity,
                                long tradeId,
                                long buyerUserId,
                                long sellerUserId,
                                long settlementAmount) throws InterruptedException {
        while (!publish(type, eventId, orderId, userId, symbol, price,
                quantity, fillQuantity, tradeId, buyerUserId, sellerUserId,
                settlementAmount)) {
            if (Thread.interrupted()) {
                throw new InterruptedException("event publisher interrupted");
            }
            Thread.onSpinWait();
        }
    }

    /**
     * 获取已处理事件数量。
     *
     * @return 已调用状态机的事件数量
     */
    public long processedEventCount() {
        return processedEvents.sum();
    }

    /**
     * 获取消费者异常。
     *
     * @return 消费线程异常，未发生异常时返回 null
     */
    public Throwable getConsumerFailure() {
        return consumerFailure.get();
    }

    /**
     * 停止消费者并等待其退出。
     *
     * @note 关闭前消费者会排空已经发布的槽位；调用方应先停止生产线程。
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
     * 批量消费 primitive 事件。
     *
     * @note 消费异常记录到 consumerFailure 并停止线程，便于 benchmark 和监控发现故障。
     */
    private void consumeLoop() {
        try {
            while (running.get() || !ringBuffer.isEmpty()) {
                if (ringBuffer.drain(consumer, 256) == 0) {
                    Thread.onSpinWait();
                }
            }
        } catch (Throwable failure) {
            consumerFailure.compareAndSet(null, failure);
        } finally {
            running.set(false);
        }
    }

    /**
     * 将 primitive 事件字段提交给低分配状态机路径。
     *
     * @param type 事件类型 ordinal
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param userId 用户标识
     * @param symbol 交易对
     * @param price 订单价格
     * @param quantity 订单数量
     * @param fillQuantity 成交数量
     */
    private void dispatch(int type,
                          long eventId,
                          long orderId,
                          long userId,
                          String symbol,
                          long price,
                          long quantity,
                          long fillQuantity,
                          long tradeId,
                          long buyerUserId,
                          long sellerUserId,
                          long settlementAmount) {
        stateMachine.applyFast(eventId, orderId, EVENT_TYPES[type], userId,
                symbol, price, quantity, fillQuantity, tradeId, buyerUserId,
                sellerUserId, settlementAmount);
        processedEvents.increment();
    }
}
