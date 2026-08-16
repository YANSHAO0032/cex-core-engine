package com.cex.core.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OrderContextTest {

    @Test
    void duplicateFactRegistrationIsExposedWithoutClearingTheFact() {
        OrderContext context = OrderContext.fromFirstEvent(
                new OrderEvent(1L, 2L, 3L, 1_000L, OrderEventType.ORDER_CREATED));

        assertEquals(FactRegistrationResult.NEW, context.registerFact(OrderEventType.MATCH_FILLED));
        assertEquals(FactRegistrationResult.DUPLICATE, context.registerFact(OrderEventType.MATCH_FILLED));
        assertTrue(context.hasFact(OrderFact.FILLED_SEEN));
    }

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
