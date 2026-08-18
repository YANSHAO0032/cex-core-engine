package com.cex.core.trade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.order.AssetId;
import com.cex.core.order.TradeExecution;
import com.cex.core.order.TradingPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * 验证成交幂等存储的容量边界、终态索引清理和并发注册语义。
 *
 * <p>线程安全：测试使用起跑闭锁使并发注册重叠，并验证计数不会超过配置上限。</p>
 * <p>使用限制：仅验证进程内存储，不覆盖后续双边结算协调器。</p>
 */
class TradeExecutionStoreTest {
    /** 创建成交存储测试实例。 */
    TradeExecutionStoreTest() {
    }

    /** 成交存储测试使用的固定交易对。 */
    private static final TradingPair BTC_USDT = new TradingPair(new AssetId("BTC"), new AssetId("USDT"));

    /** 场景：相同载荷必须返回原记录，而冲突载荷绝不替换最初记录。 */
    @Test
    void duplicatePayloadReturnsOriginalRecordButConflictFails() {
        TradeExecutionStore store = new TradeExecutionStore(2, 4);
        TradeExecution first = execution(1L, 10L, 20L);
        TradeExecutionRecord accepted = store.register(first);

        assertSame(accepted, store.register(first));
        assertThrows(TradeMetadataMismatchException.class,
                () -> store.register(execution(1L, 10L, 21L)));
        assertSame(accepted, store.record(1L));
        assertSame(first, accepted.execution());
    }

    /** 场景：挂起容量满时拒绝新成交且不会逐出已接纳成交，已有重复仍可识别。 */
    @Test
    void pendingCapacityRejectsNewTradeWithoutEvictingAcceptedTrades() {
        TradeExecutionStore store = new TradeExecutionStore(1, 4);
        TradeExecution accepted = execution(1L, 10L, 20L);
        TradeExecutionRecord record = store.register(accepted);

        assertThrows(PendingCapacityExceededException.class,
                () -> store.register(execution(2L, 11L, 21L)));
        assertSame(record, store.register(accepted));
        assertEquals(1, store.pendingCount());
        assertEquals(1, store.totalCount());
        assertNotNull(store.record(1L));
    }

    /** 场景：总容量满后终态记录仍识别重复，但新 ID 必须受到背压。 */
    @Test
    void totalCapacityRejectsNewTradeIdsButStillRecognizesTerminalDuplicates() {
        TradeExecutionStore store = new TradeExecutionStore(2, 2);
        TradeExecution first = execution(1L, 10L, 20L);
        TradeExecutionRecord firstRecord = store.register(first);
        store.markSettled(1L, 100L);
        store.register(execution(2L, 11L, 21L));

        assertSame(firstRecord, store.register(first));
        assertThrows(PendingCapacityExceededException.class,
                () -> store.register(execution(3L, 12L, 22L)));
        assertEquals(2, store.totalCount());
        assertEquals(1, store.pendingCount());
    }

    /** 场景：终态转换仅生效一次，立即删除双方订单索引并恰好释放一个挂起名额。 */
    @Test
    void terminalTransitionRemovesBothOrderIndexesAndDecrementsPendingExactlyOnce() {
        TradeExecutionStore store = new TradeExecutionStore(2, 4);
        TradeExecutionRecord record = store.register(execution(1L, 10L, 20L));

        store.markRejected(1L, "sequence gap cannot close", 100L);
        store.markSettled(1L, 101L);

        assertEquals(TradeExecutionState.REJECTED, record.state());
        assertEquals("sequence gap cannot close", record.rejectionReason());
        assertEquals(100L, record.completedAtMillis());
        assertTrue(store.pendingTradeIds(10L).isEmpty());
        assertTrue(store.pendingTradeIds(20L).isEmpty());
        assertEquals(0, store.pendingCount());
        assertEquals(1, store.totalCount());
    }

    /**
     * 场景：并发新 ID 注册永不突破任一容量，失败预留不会遗留在计数中。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void concurrentRegistrationsKeepPendingAndTotalCountsBounded() throws Exception {
        TradeExecutionStore store = new TradeExecutionStore(8, 8);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Callable<Boolean>> registrations = new ArrayList<>();
            for (long tradeId = 1L; tradeId <= 64L; tradeId++) {
                long currentTradeId = tradeId;
                registrations.add(() -> {
                    start.await();
                    try {
                        store.register(execution(currentTradeId, currentTradeId + 100L, currentTradeId + 200L));
                        return true;
                    } catch (PendingCapacityExceededException expected) {
                        return false;
                    }
                });
            }
            List<Future<Boolean>> futures = new ArrayList<>();
            for (Callable<Boolean> registration : registrations) {
                futures.add(executor.submit(registration));
            }

            start.countDown();
            int accepted = completedTrueCount(futures);

            assertEquals(8, accepted);
            assertEquals(8, store.pendingCount());
            assertEquals(8, store.totalCount());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：容量为一时并发同载荷登记全部返回同一记录，竞争预留最终完全回滚。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void concurrentExactDuplicatesReturnOriginalRecordWhenCapacityIsFull() throws Exception {
        TradeExecutionStore store = new TradeExecutionStore(1, 1);
        TradeExecution execution = execution(1L, 10L, 20L);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Future<TradeExecutionRecord>> futures = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return store.register(execution);
                }));
            }

            start.countDown();
            TradeExecutionRecord original = getWithin(futures.getFirst());
            for (Future<TradeExecutionRecord> future : futures) {
                assertSame(original, getWithin(future));
            }
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：注册结果必须在线性化点精确区分一个首次登记与其余 31 个并发重复。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void concurrentRegistrationOutcomeIdentifiesOneNewAndThirtyOneDuplicates() throws Exception {
        TradeExecutionStore store = new TradeExecutionStore(1, 1);
        TradeExecution execution = execution(1L, 10L, 20L);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            List<Future<TradeRegistrationOutcome>> futures = new ArrayList<>();
            for (int index = 0; index < 32; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return store.registerWithOutcome(execution);
                }));
            }

            start.countDown();
            TradeExecutionRecord original = null;
            int newRegistrations = 0;
            int duplicates = 0;
            for (Future<TradeRegistrationOutcome> future : futures) {
                TradeRegistrationOutcome outcome = getWithin(future);
                if (original == null) {
                    original = outcome.record();
                }
                assertSame(original, outcome.record());
                if (outcome.duplicate()) {
                    duplicates++;
                } else {
                    newRegistrations++;
                }
            }

            assertNotNull(original);
            assertEquals(1, newRegistrations);
            assertEquals(31, duplicates);
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：迟到重复在首次查询未命中后，必须识别另一线程已经发布并移除登记槽的权威记录。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void exactDuplicateRechecksPublishedRecordAfterWinningRecreatedRegistrationSlot() throws Exception {
        CountDownLatch ownerBeforePublication = new CountDownLatch(1);
        CountDownLatch allowOwnerPublication = new CountDownLatch(1);
        CountDownLatch contenderAfterInitialMiss = new CountDownLatch(1);
        CountDownLatch allowContenderRegistration = new CountDownLatch(1);
        AtomicBoolean pauseOwnerOnce = new AtomicBoolean(true);
        AtomicReference<Thread> contenderThread = new AtomicReference<>();
        TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
            if (stage == TradeExecutionStore.PublicationStage.BEFORE_RECORD_PUBLICATION
                    && pauseOwnerOnce.compareAndSet(true, false)) {
                ownerBeforePublication.countDown();
                await(allowOwnerPublication);
            }
        }, tradeId -> {
            if (Thread.currentThread() == contenderThread.get()) {
                contenderAfterInitialMiss.countDown();
                await(allowContenderRegistration);
            }
        });
        TradeExecution execution = execution(1L, 10L, 20L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TradeRegistrationOutcome> owner = executor.submit(
                    () -> store.registerWithOutcome(execution));
            assertTrue(ownerBeforePublication.await(5L, TimeUnit.SECONDS));
            Future<TradeRegistrationOutcome> contender = executor.submit(() -> {
                contenderThread.set(Thread.currentThread());
                return store.registerWithOutcome(execution);
            });
            assertTrue(contenderAfterInitialMiss.await(5L, TimeUnit.SECONDS));

            allowOwnerPublication.countDown();
            TradeRegistrationOutcome created = getWithin(owner);
            allowContenderRegistration.countDown();
            TradeRegistrationOutcome duplicate = getWithin(contender);

            assertFalse(created.duplicate());
            assertTrue(duplicate.duplicate());
            assertSame(created.record(), duplicate.record());
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
            allowOwnerPublication.countDown();
            allowContenderRegistration.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：权威记录发布后，精确重复必须优先于重建槽中的临时冲突载荷。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void authoritativeRecordWinsOverConflictingRecreatedRegistrationSlot() throws Exception {
        CountDownLatch ownerBeforePublication = new CountDownLatch(1);
        CountDownLatch allowOwnerPublication = new CountDownLatch(1);
        CountDownLatch conflictAfterInitialMiss = new CountDownLatch(1);
        CountDownLatch allowConflictRegistration = new CountDownLatch(1);
        CountDownLatch conflictAcquiredRecreatedSlot = new CountDownLatch(1);
        CountDownLatch allowConflictRecordRecheck = new CountDownLatch(1);
        CountDownLatch duplicateAfterInitialMiss = new CountDownLatch(1);
        CountDownLatch allowDuplicateRegistration = new CountDownLatch(1);
        AtomicBoolean pauseOwnerOnce = new AtomicBoolean(true);
        AtomicReference<Thread> conflictThread = new AtomicReference<>();
        AtomicReference<Thread> duplicateThread = new AtomicReference<>();
        TradeExecutionStore.RegistrationObserver registrationObserver =
                new TradeExecutionStore.RegistrationObserver() {
                    @Override
                    public void afterMiss(long tradeId) {
                        if (Thread.currentThread() == conflictThread.get()) {
                            conflictAfterInitialMiss.countDown();
                            await(allowConflictRegistration);
                        } else if (Thread.currentThread() == duplicateThread.get()) {
                            duplicateAfterInitialMiss.countDown();
                            await(allowDuplicateRegistration);
                        }
                    }

                    @Override
                    public void afterRegistrationAcquired(long tradeId) {
                        if (Thread.currentThread() == conflictThread.get()) {
                            conflictAcquiredRecreatedSlot.countDown();
                            await(allowConflictRecordRecheck);
                        }
                    }
                };
        TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
            if (stage == TradeExecutionStore.PublicationStage.BEFORE_RECORD_PUBLICATION
                    && pauseOwnerOnce.compareAndSet(true, false)) {
                ownerBeforePublication.countDown();
                await(allowOwnerPublication);
            }
        }, registrationObserver);
        TradeExecution authoritative = execution(1L, 10L, 20L);
        TradeExecution conflicting = execution(1L, 10L, 21L);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<TradeRegistrationOutcome> owner = executor.submit(
                    () -> store.registerWithOutcome(authoritative));
            assertTrue(ownerBeforePublication.await(5L, TimeUnit.SECONDS));
            Future<TradeRegistrationOutcome> conflict = executor.submit(() -> {
                conflictThread.set(Thread.currentThread());
                return store.registerWithOutcome(conflicting);
            });
            Future<TradeRegistrationOutcome> duplicate = executor.submit(() -> {
                duplicateThread.set(Thread.currentThread());
                return store.registerWithOutcome(authoritative);
            });
            assertTrue(conflictAfterInitialMiss.await(5L, TimeUnit.SECONDS));
            assertTrue(duplicateAfterInitialMiss.await(5L, TimeUnit.SECONDS));

            allowOwnerPublication.countDown();
            TradeRegistrationOutcome created = getWithin(owner);
            allowConflictRegistration.countDown();
            assertTrue(conflictAcquiredRecreatedSlot.await(5L, TimeUnit.SECONDS));
            allowDuplicateRegistration.countDown();
            TradeRegistrationOutcome exactDuplicate = getWithin(duplicate);
            allowConflictRecordRecheck.countDown();
            ExecutionException conflictFailure = assertThrows(
                    ExecutionException.class, () -> getWithin(conflict));

            assertFalse(created.duplicate());
            assertTrue(exactDuplicate.duplicate());
            assertSame(created.record(), exactDuplicate.record());
            assertTrue(conflictFailure.getCause() instanceof TradeMetadataMismatchException);
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
            allowOwnerPublication.countDown();
            allowConflictRegistration.countDown();
            allowConflictRecordRecheck.countDown();
            allowDuplicateRegistration.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：权威记录尚未发布时，冲突载荷等待活动槽结束后再按已发布记录拒绝。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void unpublishedConcurrentConflictWaitsForAuthoritativePublication() throws Exception {
        CountDownLatch ownerBeforePublication = new CountDownLatch(1);
        CountDownLatch allowOwnerPublication = new CountDownLatch(1);
        CountDownLatch contenderWaitingForOwner = new CountDownLatch(1);
        TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
            if (stage == TradeExecutionStore.PublicationStage.BEFORE_RECORD_PUBLICATION) {
                ownerBeforePublication.countDown();
                await(allowOwnerPublication);
            }
        }, new TradeExecutionStore.RegistrationObserver() {
            @Override
            public void afterMiss(long tradeId) {
            }

            @Override
            public void beforeAwait(long tradeId) {
                contenderWaitingForOwner.countDown();
            }
        });
        TradeExecution authoritative = execution(1L, 10L, 20L);
        TradeExecution conflicting = execution(1L, 10L, 21L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TradeRegistrationOutcome> owner = executor.submit(
                    () -> store.registerWithOutcome(authoritative));
            assertTrue(ownerBeforePublication.await(5L, TimeUnit.SECONDS));
            Future<TradeRegistrationOutcome> contender = executor.submit(
                    () -> store.registerWithOutcome(conflicting));

            assertTrue(contenderWaitingForOwner.await(5L, TimeUnit.SECONDS));
            allowOwnerPublication.countDown();
            TradeRegistrationOutcome created = getWithin(owner);
            ExecutionException conflictFailure = assertThrows(
                    ExecutionException.class, () -> getWithin(contender));

            assertFalse(created.duplicate());
            assertTrue(conflictFailure.getCause() instanceof TradeMetadataMismatchException);
            assertSame(created.record(), store.record(authoritative.tradeId()));
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
            allowOwnerPublication.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：活动槽拥有者回滚后，不同载荷等待者可重新竞争并成为该成交标识的新权威记录。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void conflictingContenderMayRegisterAfterOwnerPublicationFailure() throws Exception {
        CountDownLatch ownerBeforeFailure = new CountDownLatch(1);
        CountDownLatch allowOwnerFailure = new CountDownLatch(1);
        CountDownLatch contenderWaitingForOwner = new CountDownLatch(1);
        AtomicBoolean failOwnerOnce = new AtomicBoolean(true);
        TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
            if (stage == TradeExecutionStore.PublicationStage.BEFORE_RECORD_PUBLICATION
                    && failOwnerOnce.compareAndSet(true, false)) {
                ownerBeforeFailure.countDown();
                await(allowOwnerFailure);
                throw new InjectedPublicationFailure(stage);
            }
        }, new TradeExecutionStore.RegistrationObserver() {
            @Override
            public void afterMiss(long tradeId) {
            }

            @Override
            public void beforeAwait(long tradeId) {
                contenderWaitingForOwner.countDown();
            }
        });
        TradeExecution abandoned = execution(1L, 10L, 20L);
        TradeExecution recovered = execution(1L, 10L, 21L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TradeRegistrationOutcome> owner = executor.submit(
                    () -> store.registerWithOutcome(abandoned));
            assertTrue(ownerBeforeFailure.await(5L, TimeUnit.SECONDS));
            Future<TradeRegistrationOutcome> contender = executor.submit(
                    () -> store.registerWithOutcome(recovered));

            assertTrue(contenderWaitingForOwner.await(5L, TimeUnit.SECONDS));
            allowOwnerFailure.countDown();
            ExecutionException ownerFailure = assertThrows(
                    ExecutionException.class, () -> getWithin(owner));
            TradeRegistrationOutcome created = getWithin(contender);

            assertTrue(ownerFailure.getCause() instanceof InjectedPublicationFailure);
            assertFalse(created.duplicate());
            assertEquals(recovered, created.record().execution());
            assertSame(created.record(), store.record(recovered.tradeId()));
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
            assertTrue(store.pendingTradeIds(abandoned.sellOrderId()).isEmpty());
            assertEquals(List.of(recovered.tradeId()),
                    store.pendingTradeIds(recovered.sellOrderId()));
        } finally {
            allowOwnerFailure.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 场景：并发结算与拒绝只允许一个终态获胜，等待任务必须在限定时间内结束。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void concurrentTerminalTransitionsChooseOneStateAndReleasePendingOnce() throws Exception {
        TradeExecutionStore store = new TradeExecutionStore(1, 1);
        TradeExecutionRecord record = store.register(execution(1L, 10L, 20L));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> settled = executor.submit(() -> {
                await(start);
                store.markSettled(1L, 100L);
            });
            Future<?> rejected = executor.submit(() -> {
                await(start);
                store.markRejected(1L, "rejected concurrently", 101L);
            });

            start.countDown();
            getWithin(settled);
            getWithin(rejected);

            assertTrue(record.state() == TradeExecutionState.SETTLED
                    || record.state() == TradeExecutionState.REJECTED);
            assertEquals(0, store.pendingCount());
            assertEquals(1, store.totalCount());
            assertTrue(store.pendingTradeIds(10L).isEmpty());
            assertTrue(store.pendingTradeIds(20L).isEmpty());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /** 场景：挂起、已结算和已拒绝记录都将精确重复绑定到同一权威实例和状态。 */
    @Test
    void duplicatesPreserveOriginalRecordAndStateAcrossAllLifecycleStates() {
        TradeExecution pending = execution(1L, 10L, 20L);
        TradeExecution settled = execution(2L, 11L, 21L);
        TradeExecution rejected = execution(3L, 12L, 22L);
        TradeExecutionStore store = new TradeExecutionStore(3, 3);
        TradeExecutionRecord pendingRecord = store.register(pending);
        TradeExecutionRecord settledRecord = store.register(settled);
        TradeExecutionRecord rejectedRecord = store.register(rejected);
        store.markSettled(2L, 100L);
        store.markRejected(3L, "deterministic rejection", 101L);

        assertSame(pendingRecord, store.register(pending));
        assertEquals(TradeExecutionState.PENDING, pendingRecord.state());
        assertSame(settledRecord, store.register(settled));
        assertEquals(TradeExecutionState.SETTLED, settledRecord.state());
        assertSame(rejectedRecord, store.register(rejected));
        assertEquals(TradeExecutionState.REJECTED, rejectedRecord.state());
        assertEquals("deterministic rejection", rejectedRecord.rejectionReason());
    }

    /** 场景：每个发布阶段发生故障都不得可见记录、残留索引或泄漏容量，并可随后重试。 */
    @Test
    void publicationFailuresRollBackEveryStageBeforeMakingRecordVisible() {
        for (TradeExecutionStore.PublicationStage failedStage
                : TradeExecutionStore.PublicationStage.values()) {
            AtomicBoolean failOnce = new AtomicBoolean(true);
            TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
                if (stage == failedStage && failOnce.compareAndSet(true, false)) {
                    throw new InjectedPublicationFailure(stage);
                }
            });
            TradeExecution execution = execution(1L, 10L, 20L);

            assertThrows(InjectedPublicationFailure.class, () -> store.register(execution));
            assertPublicationRolledBack(store, execution);

            TradeExecutionRecord recovered = store.register(execution);
            assertSame(recovered, store.record(1L));
            assertEquals(List.of(1L), store.pendingTradeIds(10L));
            assertEquals(List.of(1L), store.pendingTradeIds(20L));
        }
    }

    /**
     * 场景：首个登记者发布失败后，同载荷等待者释放登记槽、在限定时间内重试并成功。
     *
     * @throws Exception 当并发任务等待、取回结果或线程池关闭失败时抛出
     */
    @Test
    void ownerPublicationFailureReleasesInFlightSlotForWaitingDuplicateRetry() throws Exception {
        CountDownLatch ownerAtFailurePoint = new CountDownLatch(1);
        CountDownLatch allowOwnerFailure = new CountDownLatch(1);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        TradeExecutionStore store = new TradeExecutionStore(1, 1, stage -> {
            if (stage == TradeExecutionStore.PublicationStage.AFTER_RESERVATIONS
                    && failOnce.compareAndSet(true, false)) {
                ownerAtFailurePoint.countDown();
                await(allowOwnerFailure);
                throw new InjectedPublicationFailure(stage);
            }
        });
        TradeExecution execution = execution(1L, 10L, 20L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<TradeExecutionRecord> owner = executor.submit(() -> store.register(execution));
            assertTrue(ownerAtFailurePoint.await(5L, TimeUnit.SECONDS));
            Future<TradeExecutionRecord> duplicate = executor.submit(() -> store.register(execution));

            allowOwnerFailure.countDown();
            ExecutionException ownerFailure = assertThrows(ExecutionException.class, () -> getWithin(owner));
            assertTrue(ownerFailure.getCause() instanceof InjectedPublicationFailure);
            TradeExecutionRecord recovered = getWithin(duplicate);

            assertSame(recovered, store.record(1L));
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
            assertEquals(List.of(1L), store.pendingTradeIds(10L));
            assertEquals(List.of(1L), store.pendingTradeIds(20L));
        } finally {
            allowOwnerFailure.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
        }
    }

    /**
     * 等待所有并发注册完成并统计成功个数。
     *
     * @param futures 注册任务结果
     * @return 成功注册的新成交数量
     * @throws InterruptedException 当当前线程被中断时抛出
     * @throws ExecutionException 当任务抛出未预期异常时抛出
     * @throws TimeoutException 当任一任务未在限定时间内完成时抛出
     */
    private static int completedTrueCount(Collection<Future<Boolean>> futures)
            throws InterruptedException, ExecutionException, TimeoutException {
        int accepted = 0;
        for (Future<Boolean> future : futures) {
            if (getWithin(future)) {
                accepted++;
            }
        }
        return accepted;
    }

    /**
     * 在限定时间内获取并发任务结果。
     *
     * @param future 待完成的任务
     * @param <T> 任务结果类型
     * @return 任务结果
     * @throws InterruptedException 当当前线程被中断时抛出
     * @throws ExecutionException 当任务抛出未预期异常时抛出
     * @throws TimeoutException 当任务未在限定时间内完成时抛出
     */
    private static <T> T getWithin(Future<T> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(5L, TimeUnit.SECONDS);
    }

    /**
     * 等待测试同步信号，并将中断转换为失败以保留线程中断状态。
     *
     * @param latch 待等待的同步信号
     */
    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5L, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test synchronization");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for test synchronization", interrupted);
        }
    }

    /**
     * 断言一次失败发布没有遗留任何可观察存储状态。
     *
     * @param store 被检查的成交存储
     * @param execution 被拒绝发布的成交
     */
    private static void assertPublicationRolledBack(TradeExecutionStore store, TradeExecution execution) {
        assertNull(store.record(execution.tradeId()));
        assertTrue(store.pendingTradeIds(execution.buyOrderId()).isEmpty());
        assertTrue(store.pendingTradeIds(execution.sellOrderId()).isEmpty());
        assertEquals(0, store.pendingCount());
        assertEquals(0, store.totalCount());
    }

    /**
     * 测试专用的可控发布失败。
     *
     * <p>线程安全：异常本身不可变，可由任意登记线程抛出。</p>
     * <p>使用限制：只用于验证包可见发布故障注入点。</p>
     */
    private static final class InjectedPublicationFailure extends RuntimeException {
        /**
         * 以发生失败的发布阶段创建异常。
         *
         * @param stage 触发失败的阶段
         */
        private InjectedPublicationFailure(TradeExecutionStore.PublicationStage stage) {
            super("injected publication failure at " + stage);
        }
    }

    /**
     * 创建固定交易对、数量和序号的合法测试成交。
     *
     * @param tradeId 成交标识
     * @param buyOrderId 买方订单标识
     * @param sellOrderId 卖方订单标识
     * @return 合法的不可变成交
     */
    private static TradeExecution execution(long tradeId, long buyOrderId, long sellOrderId) {
        return new TradeExecution(
                tradeId, buyOrderId, sellOrderId, BTC_USDT,
                1L, 100L, 1L, 1L, 0L);
    }
}
