package com.cex.core.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.concurrent.StripedLockManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 验证账户账本在资金迁移、异常输入与并发场景下保持资产守恒。
 *
 * <p>线程安全：测试通过用户条带锁协调并发任务。</p>
 * <p>限制：覆盖内存账本行为，不涉及外部持久化或跨进程并发。</p>
 */
class LedgerInvariantTest {

    /** 场景：指定资产的冻结与解冻只迁移该资产，并保持所有资产不变量。 */
    @Test
    void assetAwareFreezeAndUnfreezePreserveAllAssetInvariants() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        com.cex.core.order.AssetId btc = new com.cex.core.order.AssetId("BTC");
        com.cex.core.order.AssetId usdt = new com.cex.core.order.AssetId("USDT");
        ledger.createBalance(1L, btc, 100L);
        ledger.createBalance(1L, usdt, 200L);

        withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, btc, 30L));
        withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, btc, 10L));

        assertEquals(new BalanceSnapshot(80L, 20L), ledger.balance(1L, btc));
        assertEquals(new BalanceSnapshot(200L, 0L), ledger.balance(1L, usdt));
        assertTrue(ledger.invariantHolds(btc));
        assertTrue(ledger.invariantHolds(usdt));
        assertTrue(ledger.allBalancesNonNegative());
    }

    /** 场景：冻结、解冻和结算后，可用、冻结与系统资金之和保持不变。 */
    @Test
    void freezeUnfreezeAndSettlePreserveInvariant() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 1_000L);

        withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 300L));
        withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 100L));
        withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 200L));

        Account account = ledger.getRequiredAccount(1L);
        assertEquals(800L, account.available());
        assertEquals(0L, account.frozen());
        assertEquals(200L, ledger.systemSettledAmount());
        assertEquals(1_000L, ledger.currentTotalAsset());
        assertTrue(ledger.invariantHolds());
    }

    /** 场景：冻结金额超过可用余额时应拒绝操作。 */
    @Test
    void freezeRejectsInsufficientAvailable() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 101L)));

        assertEquals("available balance is insufficient", error.getMessage());
    }

    /** 场景：账户没有冻结余额时解冻请求应被拒绝。 */
    @Test
    void unfreezeRejectsInsufficientFrozen() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 1L)));

        assertEquals("frozen balance is insufficient", error.getMessage());
    }

    /** 场景：账户没有冻结余额时结算请求应被拒绝。 */
    @Test
    void settleRejectsInsufficientFrozen() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 1L)));

        assertEquals("frozen balance is insufficient", error.getMessage());
    }

    /** 场景：冻结、解冻与结算均拒绝零或负数金额。 */
    @Test
    void lockedOperationsRejectZeroAndNegativeAmounts() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 100L);

        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 0L)));
        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, -1L)));
        assertThrows(IllegalArgumentException.class,
                () -> withUserLock(lockManager, 1L, () -> ledger.settleLocked(1L, 0L)));
    }

    /** 场景：创建账户导致资产总额溢出时应抛出异常。 */
    @Test
    void checkedArithmeticRejectsOverflow() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager, Long.MAX_VALUE - 1L);

        assertThrows(ArithmeticException.class, () -> ledger.createAccount(1L, 0L, 2L));
    }

    /** 场景：溢出创建不会发布账户或修改初始资产，随后合法创建仍可成功。 */
    @Test
    void createAccountOverflowLeavesNoPublishedAccountOrTotalMutation() {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager, Long.MAX_VALUE - 2L);

        assertThrows(ArithmeticException.class, () -> ledger.createAccount(7L, 0L, 3L));
        assertEquals(Long.MAX_VALUE - 2L, ledger.initialTotalAsset());
        assertThrows(IllegalArgumentException.class, () -> ledger.getRequiredAccount(7L));

        ledger.createAccount(7L, 0L, 2L);

        Account account = ledger.getRequiredAccount(7L);
        assertEquals(0L, account.available());
        assertEquals(2L, account.frozen());
        assertEquals(Long.MAX_VALUE, ledger.initialTotalAsset());
    }

    /**
     * 场景：不同用户的并发资金操作完成后，账本资产总额仍守恒。
     *
     * @throws Exception 并发任务提交、结果获取或执行器关闭失败时抛出
     */
    @Test
    void concurrentOperationsOnDifferentUsersPreserveInvariant() throws Exception {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 10_000L);
        ledger.createAccount(2L, 10_000L);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> tasks = List.of(
                    userWorkflow(lockManager, ledger, 1L, 1_000L, 400L),
                    userWorkflow(lockManager, ledger, 2L, 1_500L, 800L));

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(executor.submit(task));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(20_000L, ledger.currentTotalAsset());
        assertTrue(ledger.invariantHolds());
    }

    /**
     * 场景：同一用户的并发冻结与解冻操作由条带锁串行化。
     *
     * @throws Exception 并发任务提交、结果获取或执行器关闭失败时抛出
     */
    @Test
    void concurrentOperationsOnSameUserRemainSerializable() throws Exception {
        StripedLockManager lockManager = new StripedLockManager();
        AccountLedger ledger = new AccountLedger(lockManager);
        ledger.createAccount(1L, 1_000L);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int index = 0; index < 50; index++) {
                tasks.add(() -> {
                    withUserLock(lockManager, 1L, () -> ledger.freezeLocked(1L, 1L));
                    withUserLock(lockManager, 1L, () -> ledger.unfreezeLocked(1L, 1L));
                    return null;
                });
            }

            for (Future<Void> future : executor.invokeAll(tasks)) {
                future.get();
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        Account account = ledger.getRequiredAccount(1L);
        assertEquals(1_000L, account.available());
        assertEquals(0L, account.frozen());
        assertTrue(ledger.invariantHolds());
    }

    /**
     * 构造执行冻结、结算与解冻的单用户并发工作流。
     *
     * @param lockManager 用户条带锁管理器
     * @param ledger 被操作的账本
     * @param userId 用户标识
     * @param freezeAmount 要冻结的资产数量
     * @param settleAmount 要结算的资产数量
     * @return 可提交给执行器的工作任务
     */
    private static Callable<Void> userWorkflow(
            StripedLockManager lockManager,
            AccountLedger ledger,
            long userId,
            long freezeAmount,
            long settleAmount) {
        return () -> {
            withUserLock(lockManager, userId, () -> ledger.freezeLocked(userId, freezeAmount));
            withUserLock(lockManager, userId, () -> ledger.settleLocked(userId, settleAmount));
            withUserLock(lockManager, userId, () -> ledger.unfreezeLocked(userId, freezeAmount - settleAmount));
            return null;
        };
    }

    /**
     * 在指定用户的条带锁保护下执行操作。
     *
     * @param lockManager 用户条带锁管理器
     * @param userId 用户标识
     * @param action 要执行的操作
     */
    private static void withUserLock(StripedLockManager lockManager, long userId, ThrowingRunnable action) {
        ReentrantLock lock = lockManager.lockForUser(userId);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 表示可抛出受检异常的无返回值测试操作。
     *
     * <p>线程安全：接口实现的线程安全性由具体测试操作决定。</p>
     * <p>限制：仅用于本测试类的锁执行辅助方法。</p>
     */
    @FunctionalInterface
    private interface ThrowingRunnable {
        /** 执行测试操作。 */
        void run();
    }
}
