package com.cex.core.engine.event;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 热路径 primitive 字段 MPSC 事件环形缓冲区。
 *
 * <p>固定数组和连续 payload 在所有事件间复用；生产者不创建事件信封，单消费者通过回调
 * 接收 primitive 字段，减少 GC 并改善 CPU cache locality。</p>
 */
public final class PrimitiveEventRingBuffer {

    /** RingBuffer 固定容量。 */
    private final int capacity;
    /** 容量掩码，用位运算替代取模。 */
    private final int mask;
    /** 事件类型 ordinal 数组。 */
    private final int[] types;
    /** 每槽位连续保存事件和成交结算字段的 primitive payload。 */
    private final long[] payload;
    /** 交易对引用数组，槽位消费后立即清空以便对象回收。 */
    private final String[] symbols;
    /** 槽位发布序号，提供生产者写入完成的可见性。 */
    private final AtomicLongArray publishedSequences;
    /** 多生产者 CAS 预留序号。 */
    private final AtomicLong producerSequence = new AtomicLong();
    /** 单消费者当前读取序号。 */
    private final AtomicLong consumerSequence = new AtomicLong();

    /**
     * 创建 primitive 字段环形缓冲区。
     *
     * @param capacity 缓冲区容量，必须为大于等于 2 的 2 的幂
     * @throws IllegalArgumentException capacity 不满足约束时抛出
     */
    public PrimitiveEventRingBuffer(int capacity) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("capacity must be a power of two >= 2");
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.types = new int[capacity];
        this.payload = new long[capacity * 10];
        this.symbols = new String[capacity];
        this.publishedSequences = new AtomicLongArray(capacity);
        for (int i = 0; i < capacity; i++) {
            this.publishedSequences.set(i, -1L);
        }
    }

    /**
     * 尝试将 primitive 事件字段写入环形缓冲区。
     *
     * @param type 事件类型 ordinal
     * @param eventId 事件幂等标识
     * @param orderId 订单标识
     * @param userId 用户标识
     * @param symbol 交易对引用
     * @param price 订单价格
     * @param quantity 订单数量
     * @param fillQuantity 成交数量
     * @return 入队成功返回 true，缓冲区满时返回 false
     * @note CAS 预留序号后由单一生产者写入该槽位，发布序号写入必须最后执行。
     */
    public boolean offer(int type,
                         long eventId,
                         long orderId,
                         long userId,
                         String symbol,
                         long price,
                         long quantity,
                         long fillQuantity) {
        return offer(type, eventId, orderId, userId, symbol, price, quantity,
                fillQuantity, 0L, 0L, 0L, 0L);
    }

    /** 将带成交结算事实的 primitive 事件写入环形缓冲区。 */
    public boolean offer(int type,
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
        long sequence;
        for (;;) {
            long producer = producerSequence.get();
            long consumer = consumerSequence.get();
            if (producer - consumer >= capacity) {
                return false;
            }
            // CAS 竞争唯一序号，确保多生产者不会写入同一槽位。
            if (producerSequence.compareAndSet(producer, producer + 1L)) {
                sequence = producer;
                break;
            }
        }

        int index = (int) sequence & mask;
        int offset = index * 10;
        types[index] = type;
        payload[offset] = eventId;
        payload[offset + 1] = orderId;
        payload[offset + 2] = userId;
        payload[offset + 3] = price;
        payload[offset + 4] = quantity;
        payload[offset + 5] = fillQuantity;
        payload[offset + 6] = tradeId;
        payload[offset + 7] = buyerUserId;
        payload[offset + 8] = sellerUserId;
        payload[offset + 9] = settlementAmount;
        symbols[index] = symbol;
        publishedSequences.lazySet(index, sequence);
        return true;
    }

    /**
     * 消费下一个完整发布的 primitive 事件。
     *
     * @param consumer 接收事件字段的单消费者回调
     * @return 成功消费返回 true，当前无完整事件返回 false
     * @note 回调执行完成后才推进 consumerSequence，避免生产者提前覆盖仍在使用的槽位。
     */
    public boolean poll(EventConsumer consumer) {
        long sequence = consumerSequence.get();
        int index = (int) sequence & mask;
        if (publishedSequences.get(index) != sequence) {
            return false;
        }

        int offset = index * 10;
        consumer.accept(types[index], payload[offset], payload[offset + 1], payload[offset + 2],
                symbols[index], payload[offset + 3], payload[offset + 4], payload[offset + 5],
                payload[offset + 6], payload[offset + 7], payload[offset + 8], payload[offset + 9]);
        symbols[index] = null;
        consumerSequence.lazySet(sequence + 1L);
        return true;
    }

    /**
     * 批量消费 primitive 事件。
     *
     * @param consumer 接收事件字段的单消费者回调
     * @param limit 本次最多消费数量
     * @return 实际消费事件数量
     */
    public int drain(EventConsumer consumer, int limit) {
        int processed = 0;
        while (processed < limit && poll(consumer)) {
            processed++;
        }
        return processed;
    }

    /**
     * 判断缓冲区是否为空。
     *
     * @return 当前没有未消费事件时返回 true
     */
    public boolean isEmpty() {
        return producerSequence.get() == consumerSequence.get();
    }

    /**
     * 获取当前未消费事件数量。
     *
     * @return 生产序号与消费序号之差
     */
    public long size() {
        return producerSequence.get() - consumerSequence.get();
    }

    /** 单消费者读取完整事件槽位时使用的回调接口。 */
    @FunctionalInterface
    public interface EventConsumer {

        /**
         * 接收一个已经完整发布的 primitive 事件。
         *
         * @param type 事件类型 ordinal
         * @param eventId 事件幂等标识
         * @param orderId 订单标识
         * @param userId 用户标识
         * @param symbol 交易对
         * @param price 订单价格
         * @param quantity 订单数量
         * @param fillQuantity 成交数量
         * @param tradeId 成交结算幂等标识
         * @param buyerUserId 买方账户
         * @param sellerUserId 卖方账户
         * @param settlementAmount 成交结算金额
         */
        void accept(int type,
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
                     long settlementAmount);
    }
}
