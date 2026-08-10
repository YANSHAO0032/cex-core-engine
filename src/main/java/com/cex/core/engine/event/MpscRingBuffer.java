package com.cex.core.engine.event;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * 固定容量的多生产者单消费者事件环形缓冲区。
 *
 * <p>生产者通过 CAS 预留序号，并在写入事件后发布槽位序号；消费者不会读到半写入事件。
 * 缓冲区有界，满载时由 offer 返回 false 施加背压。</p>
 */
public final class MpscRingBuffer<T> {

    /** RingBuffer 固定容量。 */
    private final int capacity;
    /** 容量掩码，容量为 2 的幂时可替代取模。 */
    private final int mask;
    /** 保存事件引用的原子槽位数组。 */
    private final AtomicReferenceArray<T> entries;
    /** 每个槽位对应的已发布序号。 */
    private final AtomicLongArray publishedSequences;
    /** 多生产者 CAS 申请的下一个序号。 */
    private final AtomicLong producerSequence = new AtomicLong();
    /** 单消费者当前待取序号。 */
    private final AtomicLong consumerSequence = new AtomicLong();

    /**
     * 创建固定容量 MPSC RingBuffer。
     *
     * @param capacity 缓冲区容量，必须为大于等于 2 的 2 的幂
     * @throws IllegalArgumentException capacity 不满足约束时抛出
     */
    public MpscRingBuffer(int capacity) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("capacity must be a power of two >= 2");
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.entries = new AtomicReferenceArray<>(capacity);
        this.publishedSequences = new AtomicLongArray(capacity);
        for (int i = 0; i < capacity; i++) {
            this.publishedSequences.set(i, -1L);
        }
    }

    /**
     * 尝试非阻塞入队一个事件对象。
     *
     * @param value 待入队事件对象
     * @return 入队成功返回 true，缓冲区满时返回 false
     * @note 生产者使用 CAS 竞争序号，无全局业务锁；槽位发布序号保证消费者可见性。
     */
    public boolean offer(T value) {
        if (value == null) {
            throw new NullPointerException("value");
        }

        long sequence;
        for (;;) {
            long producer = producerSequence.get();
            long consumer = consumerSequence.get();
            if (producer - consumer >= capacity) {
                return false;
            }
            // CAS 只负责为当前生产者分配唯一序号，避免多个生产者覆盖同一槽位。
            if (producerSequence.compareAndSet(producer, producer + 1L)) {
                sequence = producer;
                break;
            }
        }

        int index = (int) sequence & mask;
        entries.set(index, value);
        publishedSequences.lazySet(index, sequence);
        return true;
    }

    /**
     * 获取下一个已完整发布的事件。
     *
     * @return 下一事件；队列为空或生产者尚未发布完整槽位时返回 null
     * @note 单消费者读取并推进 consumerSequence，发布序号提供生产者写入完成的内存可见性。
     */
    public T poll() {
        long sequence = consumerSequence.get();
        int index = (int) sequence & mask;
        if (publishedSequences.get(index) != sequence) {
            return null;
        }

        T value = entries.getAndSet(index, null);
        consumerSequence.lazySet(sequence + 1L);
        return value;
    }

    /**
     * 判断缓冲区是否为空。
     *
     * @return 生产序号等于消费序号时返回 true
     */
    public boolean isEmpty() {
        return producerSequence.get() == consumerSequence.get();
    }

    /**
     * 获取缓冲区容量。
     *
     * @return 固定槽位数量
     */
    public int capacity() {
        return capacity;
    }

    /**
     * 获取当前未消费事件数量。
     *
     * @return 当前生产序号与消费序号之差
     */
    public long size() {
        return producerSequence.get() - consumerSequence.get();
    }
}
