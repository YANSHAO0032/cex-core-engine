package com.cex.core.engine.benchmark;

import com.cex.core.engine.event.EventDispatcher;
import com.cex.core.engine.event.EventType;
import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.event.PrimitiveEventDispatcher;
import com.cex.core.engine.order.OrderStateMachine;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 纯 JDK 事件流水线前后性能 benchmark 工具。
 *
 * <p>使用 16 个生产者和 1 个消费者处理 50 万事件，对比对象事件路径与 primitive RingBuffer
 * 路径的 TPS、单事件平均耗时和 GC 指标；测试不改变业务状态转换规则。</p>
 */
@Tag("benchmark")
class EventPipelineBenchmarkTest {

    /** benchmark 生产者线程数量。 */
    private static final int PRODUCER_COUNT = 16;
    /** 单次正式测量事件数量。 */
    private static final int EVENT_COUNT = 500_000;
    /** JIT 预热事件数量。 */
    private static final int WARMUP_EVENT_COUNT = 50_000;
    /** 正式测量重复次数，最终取中位数降低调度噪声。 */
    private static final int MEASUREMENT_TRIALS = 5;
    /** 预置订单聚合数量。 */
    private static final int ORDER_COUNT = 10_000;
    /** benchmark 订单标识起始值。 */
    private static final long ORDER_BASE = 100_000L;
    /** 测试事件 RingBuffer 容量。 */
    private static final int RING_CAPACITY = 1 << 16;
    /** benchmark 使用的规范化交易对引用，避免每事件创建字符串。 */
    private static final String SYMBOL = "BTC-USDT";

    /** 对比对象事件路径和 primitive RingBuffer 路径。 */
    @Test
    void comparesObjectEventPathWithPrimitiveRingPath() throws Exception {
        runBaseline(WARMUP_EVENT_COUNT);
        runOptimized(WARMUP_EVENT_COUNT);

        BenchmarkResult before = medianBaseline(EVENT_COUNT);
        BenchmarkResult after = medianOptimized(EVENT_COUNT);

        System.out.printf("%nEvent pipeline benchmark (%d events, %d producers / 1 consumer)%n",
                EVENT_COUNT, PRODUCER_COUNT);
        print("Before", before);
        print("After ", after);
        System.out.printf("Speedup: %.2fx, GC collections delta: %d -> %d%n%n",
                after.tps / before.tps, before.gcCollections, after.gcCollections);

        assertEquals(EVENT_COUNT, before.processedEvents);
        assertEquals(EVENT_COUNT, after.processedEvents);
        assertNull(before.consumerFailure);
        assertNull(after.consumerFailure);
        assertTrue(after.tps > 0.0);
    }

    /**
     * 执行多次对象事件路径并取中位数。
     *
     * @param eventCount 每次测量事件数量
     * @return 对象事件路径中位数结果
     * @throws Exception 生产线程等待或中断时抛出
     */
    private static BenchmarkResult medianBaseline(int eventCount) throws Exception {
        BenchmarkResult[] trials = new BenchmarkResult[MEASUREMENT_TRIALS];
        for (int i = 0; i < MEASUREMENT_TRIALS; i++) {
            trials[i] = runBaselineOnce(eventCount);
        }
        return median(trials);
    }

    /**
     * 执行多次 primitive 事件路径并取中位数。
     *
     * @param eventCount 每次测量事件数量
     * @return primitive 事件路径中位数结果
     * @throws Exception 生产线程等待或中断时抛出
     */
    private static BenchmarkResult medianOptimized(int eventCount) throws Exception {
        BenchmarkResult[] trials = new BenchmarkResult[MEASUREMENT_TRIALS];
        for (int i = 0; i < MEASUREMENT_TRIALS; i++) {
            trials[i] = runOptimizedOnce(eventCount);
        }
        return median(trials);
    }

    /**
     * 执行一次对象事件路径 benchmark。
     *
     * @param eventCount 事件数量
     * @return 单次对象路径测量结果
     * @throws Exception 线程同步或发布中断时抛出
     */
    private static BenchmarkResult runBaseline(int eventCount) throws Exception {
        return runBaselineOnce(eventCount);
    }

    /**
     * 创建对象事件分发器并执行单次测量。
     *
     * @param eventCount 事件数量
     * @return 对象事件路径测量结果
     * @throws Exception 生产线程等待或中断时抛出
     */
    private static BenchmarkResult runBaselineOnce(int eventCount) throws Exception {
        OrderStateMachine stateMachine = seedOrders();
        EventDispatcher dispatcher = new EventDispatcher(stateMachine, RING_CAPACITY);
        dispatcher.start();
        long beforeGcCollections = gcCollections();
        long beforeGcTime = gcTimeMillis();
        long startedAt = System.nanoTime();
        runObjectProducers(dispatcher, eventCount);
        waitFor(dispatcher::processedEventCount, eventCount);
        dispatcher.close();
        long elapsed = System.nanoTime() - startedAt;
        return new BenchmarkResult(eventCount, elapsed,
                gcCollections() - beforeGcCollections,
                gcTimeMillis() - beforeGcTime,
                dispatcher.getConsumerFailure());
    }

    /**
     * 执行一次 primitive 事件路径 benchmark。
     *
     * @param eventCount 事件数量
     * @return 单次 primitive 路径测量结果
     * @throws Exception 线程同步或发布中断时抛出
     */
    private static BenchmarkResult runOptimized(int eventCount) throws Exception {
        return runOptimizedOnce(eventCount);
    }

    /**
     * 创建 primitive 事件分发器并执行单次测量。
     *
     * @param eventCount 事件数量
     * @return primitive 事件路径测量结果
     * @throws Exception 生产线程等待或中断时抛出
     */
    private static BenchmarkResult runOptimizedOnce(int eventCount) throws Exception {
        OrderStateMachine stateMachine = seedOrders();
        PrimitiveEventDispatcher dispatcher =
                new PrimitiveEventDispatcher(stateMachine, RING_CAPACITY);
        dispatcher.start();
        long beforeGcCollections = gcCollections();
        long beforeGcTime = gcTimeMillis();
        long startedAt = System.nanoTime();
        runPrimitiveProducers(dispatcher, eventCount);
        waitFor(dispatcher::processedEventCount, eventCount);
        dispatcher.close();
        long elapsed = System.nanoTime() - startedAt;
        return new BenchmarkResult(eventCount, elapsed,
                gcCollections() - beforeGcCollections,
                gcTimeMillis() - beforeGcTime,
                dispatcher.getConsumerFailure());
    }

    /**
     * 预置可接收成交事件的订单聚合。
     *
     * @return 已创建订单的状态机
     */
    private static OrderStateMachine seedOrders() {
        OrderStateMachine stateMachine = new OrderStateMachine();
        for (int i = 0; i < ORDER_COUNT; i++) {
            long orderId = ORDER_BASE + i;
            stateMachine.apply(OrderEvent.created(
                    orderId * 10L + 1L, orderId, i, SYMBOL, 1L, 1_000_000L));
        }
        return stateMachine;
    }

    /**
     * 启动多生产者并发布对象订单事件。
     *
     * @param dispatcher 对象事件分发器
     * @param eventCount 待发布事件数量
     * @throws Exception 线程同步或生产线程中断时抛出
     * @note 生产者使用有界 RingBuffer 背压；所有线程结束后才返回，避免 benchmark 提前计时结束。
     */
    private static void runObjectProducers(EventDispatcher dispatcher, int eventCount)
            throws Exception {
        List<Thread> producers = new ArrayList<>(PRODUCER_COUNT);
        CountDownLatch ready = new CountDownLatch(PRODUCER_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(PRODUCER_COUNT);
        for (int producerId = 0; producerId < PRODUCER_COUNT; producerId++) {
            final int id = producerId;
            Thread producer = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = id; i < eventCount; i += PRODUCER_COUNT) {
                        long orderId = ORDER_BASE + (i % ORDER_COUNT);
                        dispatcher.publishBlocking(OrderEvent.matchFilled(
                                1_000_000L + i, orderId, 1L));
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "benchmark-object-producer-" + producerId);
            producers.add(producer);
            producer.start();
        }
        assertTrue(ready.await(10L, TimeUnit.SECONDS));
        start.countDown();
        assertTrue(done.await(60L, TimeUnit.SECONDS));
        for (Thread producer : producers) {
            producer.join();
        }
    }

    /**
     * 启动多生产者并发布 primitive 订单事件。
     *
     * @param dispatcher primitive 事件分发器
     * @param eventCount 待发布事件数量
     * @throws Exception 线程同步或生产线程中断时抛出
     * @note 生产者复用固定 primitive 槽位，不创建 OrderEvent 对象，适配 -Xmx256m。
     */
    private static void runPrimitiveProducers(PrimitiveEventDispatcher dispatcher,
                                              int eventCount) throws Exception {
        List<Thread> producers = new ArrayList<>(PRODUCER_COUNT);
        CountDownLatch ready = new CountDownLatch(PRODUCER_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(PRODUCER_COUNT);
        for (int producerId = 0; producerId < PRODUCER_COUNT; producerId++) {
            final int id = producerId;
            Thread producer = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int i = id; i < eventCount; i += PRODUCER_COUNT) {
                        long orderId = ORDER_BASE + (i % ORDER_COUNT);
                        dispatcher.publishBlocking(EventType.MATCH_FILLED,
                                1_000_000L + i, orderId, 0L, SYMBOL,
                                0L, 0L, 1L);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "benchmark-primitive-producer-" + producerId);
            producers.add(producer);
            producer.start();
        }
        assertTrue(ready.await(10L, TimeUnit.SECONDS));
        start.countDown();
        assertTrue(done.await(60L, TimeUnit.SECONDS));
        for (Thread producer : producers) {
            producer.join();
        }
    }

    /**
     * 等待消费者处理达到目标数量。
     *
     * @param processedEvents 返回当前已处理数量的计数函数
     * @param expected 期望处理事件数量
     */
    private static void waitFor(LongSupplier processedEvents, long expected) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        while (processedEvents.getAsLong() < expected && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(expected, processedEvents.getAsLong());
    }

    /**
     * 输出一组 benchmark 指标。
     *
     * @param label 指标路径名称
     * @param result benchmark 结果
     */
    private static void print(String label, BenchmarkResult result) {
        System.out.printf("%s: TPS=%.2f, avg/event=%.2f us, GC collections=%d, "
                        + "GC time=%d ms%n",
                label, result.tps, result.averageMicros,
                result.gcCollections, result.gcTimeMillis);
    }

    /**
     * 计算多次 benchmark 结果的中位数。
     *
     * @param trials 单次测量结果数组
     * @return TPS、平均耗时和 GC 指标的中位数结果
     */
    private static BenchmarkResult median(BenchmarkResult[] trials) {
        double[] tps = new double[trials.length];
        double[] averageMicros = new double[trials.length];
        long[] gcCollections = new long[trials.length];
        long[] gcTimeMillis = new long[trials.length];
        for (int i = 0; i < trials.length; i++) {
            tps[i] = trials[i].tps;
            averageMicros[i] = trials[i].averageMicros;
            gcCollections[i] = trials[i].gcCollections;
            gcTimeMillis[i] = trials[i].gcTimeMillis;
        }
        Arrays.sort(tps);
        Arrays.sort(averageMicros);
        Arrays.sort(gcCollections);
        Arrays.sort(gcTimeMillis);
        return BenchmarkResult.median(EVENT_COUNT,
                tps[tps.length / 2],
                averageMicros[averageMicros.length / 2],
                gcCollections[gcCollections.length / 2],
                gcTimeMillis[gcTimeMillis.length / 2]);
    }

    /**
     * 汇总 JVM 垃圾收集次数。
     *
     * @return 当前 JVM 所有垃圾收集器的收集次数总和
     */
    private static long gcCollections() {
        long total = 0L;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (bean.getCollectionCount() > 0L) {
                total += bean.getCollectionCount();
            }
        }
        return total;
    }

    /**
     * 汇总 JVM 垃圾收集耗时。
     *
     * @return 当前 JVM 所有垃圾收集器的收集耗时，单位为毫秒
     */
    private static long gcTimeMillis() {
        long total = 0L;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (bean.getCollectionTime() > 0L) {
                total += bean.getCollectionTime();
            }
        }
        return total;
    }

    /** 单次或中位数 benchmark 指标快照。 */
    private static final class BenchmarkResult {

        /** 实际处理事件数量。 */
        private final long processedEvents;
        /** 每秒处理事件数。 */
        private final double tps;
        /** 单事件平均耗时，单位为微秒。 */
        private final double averageMicros;
        /** benchmark 区间内垃圾收集次数。 */
        private final long gcCollections;
        /** benchmark 区间内垃圾收集耗时，单位为毫秒。 */
        private final long gcTimeMillis;
        /** 单消费者线程捕获的异常。 */
        private final Throwable consumerFailure;

        /**
         * 根据单次计时结果创建 benchmark 结果。
         *
         * @param eventCount 处理事件数量
         * @param elapsedNanos 总耗时，单位为纳秒
         * @param gcCollections 垃圾收集次数
         * @param gcTimeMillis 垃圾收集耗时
         * @param consumerFailure 消费线程异常
         */
        private BenchmarkResult(long eventCount,
                                long elapsedNanos,
                                long gcCollections,
                                long gcTimeMillis,
                                Throwable consumerFailure) {
            this.processedEvents = eventCount;
            this.tps = eventCount / (elapsedNanos / 1_000_000_000.0);
            this.averageMicros = elapsedNanos / (double) eventCount / 1_000.0;
            this.gcCollections = gcCollections;
            this.gcTimeMillis = gcTimeMillis;
            this.consumerFailure = consumerFailure;
        }

        /**
         * 创建中位数 benchmark 结果。
         *
         * @param eventCount 处理事件数量
         * @param tps 中位数 TPS
         * @param averageMicros 中位数单事件耗时
         * @param gcCollections 中位数 GC 次数
         * @param gcTimeMillis 中位数 GC 耗时
         * @return 汇总后的 benchmark 结果
         */
        private static BenchmarkResult median(long eventCount,
                                              double tps,
                                              double averageMicros,
                                              long gcCollections,
                                              long gcTimeMillis) {
            return new BenchmarkResult(eventCount, tps, averageMicros,
                    gcCollections, gcTimeMillis, null);
        }

        private BenchmarkResult(long processedEvents,
                                double tps,
                                double averageMicros,
                                long gcCollections,
                                long gcTimeMillis,
                                Throwable consumerFailure) {
            this.processedEvents = processedEvents;
            this.tps = tps;
            this.averageMicros = averageMicros;
            this.gcCollections = gcCollections;
            this.gcTimeMillis = gcTimeMillis;
            this.consumerFailure = consumerFailure;
        }
    }
}
