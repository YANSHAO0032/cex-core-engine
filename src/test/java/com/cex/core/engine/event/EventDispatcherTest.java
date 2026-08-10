package com.cex.core.engine.event;

import com.cex.core.engine.order.Order;
import com.cex.core.engine.order.OrderState;
import com.cex.core.engine.order.OrderStateMachine;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件分发器并发测试工具。
 *
 * <p>验证多生产者单消费者、RingBuffer 背压和订单最终状态；测试资源在 finally 中关闭。</p>
 */
class EventDispatcherTest {

    /** 验证多个生产者可以安全发布并由单消费者完成订单事件处理。 */
    @Test
    void supportsMultipleProducersAndOneConsumer() throws Exception {
        OrderStateMachine stateMachine = new OrderStateMachine();
        EventDispatcher dispatcher = new EventDispatcher(stateMachine, 256);
        dispatcher.start();

        int producerCount = 4;
        int ordersPerProducer = 250;
        ExecutorService producers = Executors.newFixedThreadPool(producerCount);
        CountDownLatch ready = new CountDownLatch(producerCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Throwable> failures = new ArrayList<>();
        try {
            for (int producerId = 0; producerId < producerCount; producerId++) {
                final int id = producerId;
                producers.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < ordersPerProducer; i++) {
                            long orderId = id * 10_000L + i;
                            dispatcher.publishBlocking(OrderEvent.created(
                                    orderId * 10L, orderId, id, "BTC-USDT", 50_000L, 1L));
                            dispatcher.publishBlocking(OrderEvent.matchFilled(
                                    orderId * 10L + 1L, orderId, 1L));
                        }
                    } catch (Throwable failure) {
                        synchronized (failures) {
                            failures.add(failure);
                        }
                    }
                });
            }

            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            start.countDown();
            producers.shutdown();
            assertTrue(producers.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(failures.isEmpty(), failures::toString);

            long expected = (long) producerCount * ordersPerProducer * 2L;
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
            while (dispatcher.processedEventCount() < expected
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertEquals(expected, dispatcher.processedEventCount());
            assertNull(dispatcher.getConsumerFailure());

            for (int producerId = 0; producerId < producerCount; producerId++) {
                for (int i = 0; i < ordersPerProducer; i++) {
                    long orderId = producerId * 10_000L + i;
                    Order order = stateMachine.get(orderId);
                    assertEquals(OrderState.FILLED, order.getState());
                    assertEquals(1L, order.getFilledQuantity());
                }
            }
        } finally {
            producers.shutdownNow();
            dispatcher.close();
        }
    }

    /** 验证固定容量 RingBuffer 满载时返回背压信号。 */
    @Test
    void ringBufferAppliesBackpressureWhenFull() {
        MpscRingBuffer<Integer> ringBuffer = new MpscRingBuffer<>(2);

        assertTrue(ringBuffer.offer(1));
        assertTrue(ringBuffer.offer(2));
        assertTrue(!ringBuffer.offer(3));
        assertEquals(1, ringBuffer.poll());
        assertTrue(ringBuffer.offer(3));
        assertEquals(2, ringBuffer.poll());
        assertEquals(3, ringBuffer.poll());
    }
}
