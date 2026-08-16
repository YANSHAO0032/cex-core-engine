package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * 验证订单上下文的可见性、CAS 事实登记和副作用幂等提交。
 *
 * <p>核心能力：覆盖事实位与副作用位的原子竞争、重复识别及未持锁读可见性。</p>
 * <p>线程安全：并发用例通过多线程同步启动验证 Atomic/CAS 操作最多一次成功。</p>
 * <p>使用限制：仅验证单 JVM 内订单上下文的并发语义，不覆盖跨进程或持久化恢复。</p>
 */
class OrderContextTest {

    /**
     * 场景：未持锁读取状态时，状态字段必须具备 volatile 可见性。
     *
     * @throws Exception 反射读取状态字段失败时抛出
     */
    @Test
    void statusFieldIsVolatileForUnlockedReaders() throws Exception {
        int modifiers = OrderContext.class.getDeclaredField("status").getModifiers();
        assertTrue(Modifier.isVolatile(modifiers));
    }

    /** 场景：重复登记同一事实应保留事实位并返回重复结果。 */
    @Test
    void duplicateFactRegistrationIsExposedWithoutClearingTheFact() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(1L, 2L, 3L, 1_000L, OrderEventType.ORDER_CREATED));

        assertEquals(FactRegistrationResult.NEW, context.registerFact(OrderEventType.MATCH_FILLED));
        assertEquals(FactRegistrationResult.DUPLICATE, context.registerFact(OrderEventType.MATCH_FILLED));
        assertTrue(context.hasFact(OrderFact.FILLED_SEEN));
    }

    /**
     * 场景：多线程竞争登记同一事实时，只允许一个首次成功者。
     *
     * @throws Exception 并发任务等待、结果获取或执行器关闭失败时抛出
     */
    @Test
    void concurrentFactRegistrationProducesOneNewFactAndRemainingDuplicates() throws Exception {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(1L, 2L, 3L, 1_000L, OrderEventType.ORDER_CREATED));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        AtomicInteger newCount = new AtomicInteger();
        AtomicInteger duplicateCount = new AtomicInteger();
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                tasks.add(() -> {
                    FactRegistrationResult result = context.registerFact(OrderEventType.ORDER_CANCELLED);
                    if (result == FactRegistrationResult.NEW) {
                        newCount.incrementAndGet();
                    } else {
                        duplicateCount.incrementAndGet();
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = executor.invokeAll(tasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(1, newCount.get());
        assertEquals(31, duplicateCount.get());
        assertTrue(context.hasFact(OrderFact.CANCELLED_SEEN));
    }

    /** 场景：在锁内重复应用副作用时，实际变更只能执行一次。 */
    @Test
    void applyEffectLockedMarksEffectOnlyOnce() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(1L, 2L, 3L, 1_000L, OrderEventType.ORDER_CREATED));
        AtomicInteger mutationRuns = new AtomicInteger();

        assertTrue(context.applyEffectLocked(OrderEffect.FREEZE_APPLIED, mutationRuns::incrementAndGet));
        assertFalse(context.applyEffectLocked(OrderEffect.FREEZE_APPLIED, mutationRuns::incrementAndGet));
        assertEquals(1, mutationRuns.get());
        assertTrue(context.hasEffect(OrderEffect.FREEZE_APPLIED));
    }

    /** 场景：副作用执行失败时，不得提交其幂等标记以支持后续补偿。 */
    @Test
    void applyEffectLockedDoesNotCommitFlagWhenMutationFails() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(1L, 2L, 3L, 1_000L, OrderEventType.ORDER_CREATED));

        assertThrows(IllegalStateException.class,
                () -> context.applyEffectLocked(OrderEffect.SETTLE_APPLIED, () -> {
                    throw new IllegalStateException("boom");
                }));

        assertFalse(context.hasEffect(OrderEffect.SETTLE_APPLIED));
    }
}
