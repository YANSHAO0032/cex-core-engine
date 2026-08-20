package com.cex.core.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 验证多资产账户账本在资金迁移、异常输入与并发场景下保持资产守恒。
 *
 * <p>线程安全：测试通过用户条带锁协调并发任务。</p>
 * <p>限制：覆盖内存账本行为，不涉及外部持久化或跨进程并发。</p>
 */
class LedgerInvariantTest {
    /** 创建账本不变量测试实例。 */
    LedgerInvariantTest() {
    }

    /** 测试基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 测试报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");

    /** 场景：指定资产的冻结与解冻只迁移该资产，并保持所有资产不变量。 */
    @Test
    void assetAwareFreezeAndUnfreezePreserveAllAssetInvariants() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, BTC, 100L);
        ledger.createBalance(1L, USDT, 200L);

        withUserLock(locks, 1L, () -> ledger.freezeLocked(1L, BTC, 30L));
        withUserLock(locks, 1L, () -> ledger.unfreezeLocked(1L, BTC, 10L));

        assertEquals(new BalanceSnapshot(80L, 20L), ledger.balance(1L, BTC));
        assertEquals(new BalanceSnapshot(200L, 0L), ledger.balance(1L, USDT));
        assertTrue(ledger.invariantHolds(BTC));
        assertTrue(ledger.invariantHolds(USDT));
        assertTrue(ledger.allBalancesNonNegative());
    }

    /** 场景：双边成交迁移两种资产后，各资产初始总量保持不变。 */
    @Test
    void bilateralTradePreservesPerAssetInvariants() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = bilateralLedger(locks);
        withUserLock(locks, 1L, () -> ledger.freezeLocked(1L, USDT, 200L));
        withUserLock(locks, 2L, () -> ledger.freezeLocked(2L, BTC, 2L));

        withBothUserLocks(locks, 1L, 2L, () -> {
            TradeLedgerMutation mutation = ledger.prepareTradeLocked(
                    1L, 2L, BTC, USDT, 2L, 200L, 0L);
            ledger.commitTradeLocked(mutation);
        });

        assertEquals(new BalanceSnapshot(2L, 0L), ledger.balance(1L, BTC));
        assertEquals(new BalanceSnapshot(800L, 0L), ledger.balance(1L, USDT));
        assertEquals(new BalanceSnapshot(8L, 0L), ledger.balance(2L, BTC));
        assertEquals(new BalanceSnapshot(200L, 0L), ledger.balance(2L, USDT));
        assertTrue(ledger.allAssetInvariantsHold());
    }

    /** 场景：冻结金额超过指定资产可用余额时应拒绝操作。 */
    @Test
    void freezeRejectsInsufficientAvailable() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, USDT, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(locks, 1L,
                        () -> ledger.freezeLocked(1L, USDT, 101L)));

        assertEquals("available balance is insufficient", error.getMessage());
    }

    /** 场景：资产没有冻结余额时解冻请求应被拒绝。 */
    @Test
    void unfreezeRejectsInsufficientFrozen() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, USDT, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(locks, 1L,
                        () -> ledger.unfreezeLocked(1L, USDT, 1L)));

        assertEquals("frozen balance is insufficient", error.getMessage());
    }

    /** 场景：双边成交任一冻结余额不足时，四个资产桶均不得改变。 */
    @Test
    void tradeRejectsInsufficientFrozenWithoutPartialMutation() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = bilateralLedger(locks);
        withUserLock(locks, 1L, () -> ledger.freezeLocked(1L, USDT, 100L));
        withUserLock(locks, 2L, () -> ledger.freezeLocked(2L, BTC, 1L));

        assertThrows(InsufficientBalanceException.class,
                () -> withBothUserLocks(locks, 1L, 2L,
                        () -> ledger.prepareTradeLocked(
                                1L, 2L, BTC, USDT, 2L, 100L, 0L)));

        assertEquals(new BalanceSnapshot(900L, 100L), ledger.balance(1L, USDT));
        assertEquals(new BalanceSnapshot(9L, 1L), ledger.balance(2L, BTC));
        assertTrue(ledger.allAssetInvariantsHold());
    }

    /** 场景：冻结与解冻均拒绝零或负数金额。 */
    @Test
    void lockedOperationsRejectZeroAndNegativeAmounts() {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, USDT, 100L);

        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(locks, 1L,
                        () -> ledger.freezeLocked(1L, USDT, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(locks, 1L,
                        () -> ledger.unfreezeLocked(1L, USDT, -1L)));
    }

    /** 场景：创建指定资产余额导致初始总额溢出时不得发布新账户。 */
    @Test
    void createBalanceOverflowLeavesNoPublishedAccountOrTotalMutation() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createBalance(1L, USDT, Long.MAX_VALUE - 2L);

        assertThrows(ArithmeticException.class,
                () -> ledger.createBalance(7L, USDT, 3L));
        assertEquals(Long.MAX_VALUE - 2L,
                ledger.initialTotalAssets().get(USDT));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.getRequiredAccount(7L));

        ledger.createBalance(7L, USDT, 2L);
        assertEquals(new BalanceSnapshot(2L, 0L), ledger.balance(7L, USDT));
        assertEquals(Long.MAX_VALUE, ledger.initialTotalAssets().get(USDT));
    }

    /**
     * 场景：不同用户的并发资金迁移完成后，账本逐资产总额仍守恒。
     *
     * @throws Exception 并发任务提交、结果获取或执行器关闭失败时抛出
     */
    @Test
    void concurrentOperationsOnDifferentUsersPreserveInvariant() throws Exception {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, USDT, 10_000L);
        ledger.createBalance(2L, USDT, 10_000L);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> tasks = List.of(
                    userWorkflow(locks, ledger, 1L, 1_000L),
                    userWorkflow(locks, ledger, 2L, 1_500L));
            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(executor.submit(task));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5L, TimeUnit.SECONDS);
        }

        assertEquals(20_000L, ledger.currentTotalAssets().get(USDT));
        assertTrue(ledger.invariantHolds(USDT));
    }

    /**
     * 场景：同一用户的并发冻结与解冻操作由条带锁串行化。
     *
     * @throws Exception 并发任务提交、结果获取或执行器关闭失败时抛出
     */
    @Test
    void concurrentOperationsOnSameUserRemainSerializable() throws Exception {
        StripedLockManager locks = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, USDT, 1_000L);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int index = 0; index < 50; index++) {
                tasks.add(() -> userWorkflow(locks, ledger, 1L, 1L).call());
            }
            for (Future<Void> future : executor.invokeAll(tasks)) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5L, TimeUnit.SECONDS);
        }

        assertEquals(new BalanceSnapshot(1_000L, 0L), ledger.balance(1L, USDT));
        assertTrue(ledger.invariantHolds(USDT));
    }

    /**
     * 构造执行一次冻结和解冻的单用户并发工作流。
     *
     * @param locks 用户条带锁管理器
     * @param ledger 被操作的多资产账本
     * @param userId 用户标识
     * @param amount 要往返迁移的报价资产数量
     * @return 可提交给执行器的工作任务
     */
    private static Callable<Void> userWorkflow(
            StripedLockManager locks, AccountLedger ledger,
            long userId, long amount) {
        return () -> {
            withUserLock(locks, userId,
                    () -> ledger.freezeLocked(userId, USDT, amount));
            withUserLock(locks, userId,
                    () -> ledger.unfreezeLocked(userId, USDT, amount));
            return null;
        };
    }

    /**
     * 创建包含买卖双方基础与报价余额的账本。
     *
     * @param locks 账本使用的条带锁管理器
     * @return 初始化完成的双边账本
     */
    private static AccountLedger bilateralLedger(StripedLockManager locks) {
        AccountLedger ledger = new AccountLedger(locks);
        ledger.createBalance(1L, BTC, 0L);
        ledger.createBalance(1L, USDT, 1_000L);
        ledger.createBalance(2L, BTC, 10L);
        ledger.createBalance(2L, USDT, 0L);
        return ledger;
    }

    /**
     * 在指定用户的条带锁保护下执行操作。
     *
     * @param locks 用户条带锁管理器
     * @param userId 用户标识
     * @param action 要执行的操作
     */
    private static void withUserLock(
            StripedLockManager locks, long userId, ThrowingRunnable action) {
        ReentrantLock lock = locks.lockForUser(userId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 按条带索引升序获取双方用户锁并执行操作。
     *
     * @param locks 条带锁管理器
     * @param firstUserId 第一用户标识
     * @param secondUserId 第二用户标识
     * @param action 双边锁内操作
     */
    private static void withBothUserLocks(
            StripedLockManager locks, long firstUserId,
            long secondUserId, ThrowingRunnable action) {
        ReentrantLock first = locks.lockForUser(firstUserId);
        ReentrantLock second = locks.lockForUser(secondUserId);
        ReentrantLock low = locks.stripeIndexForUser(firstUserId)
                <= locks.stripeIndexForUser(secondUserId) ? first : second;
        ReentrantLock high = low == first ? second : first;
        low.lock();
        if (high != low) {
            high.lock();
        }
        try {
            action.run();
        } finally {
            if (high != low) {
                high.unlock();
            }
            low.unlock();
        }
    }

    /**
     * 在账本锁辅助方法中执行可抛异常的无返回值测试操作。
     *
     * <p>核心能力：让测试在真实用户条带锁内复用资金准备与提交断言。</p>
     * <p>线程安全：具体实现的线程安全性由测试场景负责。</p>
     * <p>使用限制：仅供本测试类的同步锁辅助方法调用。</p>
     */
    @FunctionalInterface
    private interface ThrowingRunnable {
        /** 执行测试操作。 */
        void run();
    }
}
