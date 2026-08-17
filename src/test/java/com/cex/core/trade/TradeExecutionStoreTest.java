package com.cex.core.trade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.junit.jupiter.api.Test;

/**
 * 验证成交幂等存储的容量边界、终态索引清理和并发注册语义。
 *
 * <p>线程安全：测试使用起跑闭锁使并发注册重叠，并验证计数不会超过配置上限。</p>
 * <p>使用限制：仅验证进程内存储，不覆盖后续双边结算协调器。</p>
 */
class TradeExecutionStoreTest {
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

    /** 场景：并发新 ID 注册永不突破任一容量，失败预留不会遗留在计数中。 */
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

    /** 场景：容量为一时并发同载荷登记全部返回同一记录，竞争预留最终完全回滚。 */
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
            TradeExecutionRecord original = futures.getFirst().get();
            for (Future<TradeExecutionRecord> future : futures) {
                assertSame(original, future.get());
            }
            assertEquals(1, store.pendingCount());
            assertEquals(1, store.totalCount());
        } finally {
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
     */
    private static int completedTrueCount(Collection<Future<Boolean>> futures)
            throws InterruptedException, ExecutionException {
        int accepted = 0;
        for (Future<Boolean> future : futures) {
            if (future.get()) {
                accepted++;
            }
        }
        return accepted;
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
