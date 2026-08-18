package com.cex.core.account;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.AssetId;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * 验证多资产账本在成交与余额不足场景下的双边资金迁移。
 *
 * <p>线程安全：每个成交测试在买卖双方用户条带锁保护下准备并提交变更。</p>
 * <p>限制：测试仅覆盖内存账本，不涉及订单撮合或持久化。</p>
 */
class MultiAssetLedgerTest {
    /** 创建多资产账本测试实例。 */
    MultiAssetLedgerTest() {
    }


    /** 比特币基础资产。 */
    private static final AssetId BTC = new AssetId("BTC");
    /** 泰达币报价资产。 */
    private static final AssetId USDT = new AssetId("USDT");

    /** 场景：成交应分别迁移基础资产和报价资产，且每种资产总额守恒。 */
    @Test
    void tradeMovesBothAssetsAndPreservesEachAssetTotal() {
        AccountLedger ledger = ledgerWithBuyerAndSeller();
        withBothUserLocks(ledger.lockManager(), 1L, 2L, () -> {
            TradeLedgerMutation mutation = ledger.prepareTradeLocked(
                    1L, 2L, BTC, USDT, 2L, 200L, 0L);
            ledger.commitTradeLocked(mutation);
        });

        assertEquals(new BalanceSnapshot(2L, 0L), ledger.balance(1L, BTC));
        assertEquals(new BalanceSnapshot(800L, 0L), ledger.balance(1L, USDT));
        assertEquals(new BalanceSnapshot(8L, 0L), ledger.balance(2L, BTC));
        assertEquals(new BalanceSnapshot(200L, 0L), ledger.balance(2L, USDT));
        assertTrue(ledger.invariantHolds(BTC));
        assertTrue(ledger.invariantHolds(USDT));
    }

    /** 场景：卖方基础资产不足时，成交准备不得改变任何关联余额桶。 */
    @Test
    void insufficientSellerBalanceLeavesAllFourBucketsUnchanged() {
        AccountLedger ledger = ledgerWithBuyerAndSeller();
        BalanceSnapshot buyerQuote = ledger.balance(1L, USDT);
        BalanceSnapshot sellerBase = ledger.balance(2L, BTC);

        assertThrows(InsufficientBalanceException.class, () ->
                withBothUserLocks(ledger.lockManager(), 1L, 2L, () ->
                        ledger.prepareTradeLocked(1L, 2L, BTC, USDT, 11L, 200L, 0L)));

        assertEquals(buyerQuote, ledger.balance(1L, USDT));
        assertEquals(sellerBase, ledger.balance(2L, BTC));
    }

    /** 场景：买方完全成交后，未花费报价预留必须与成交支出在同一变更中释放。 */
    @Test
    void fullBuyFillReleasesUnusedQuoteReserveInTheSameMutation() {
        AccountLedger ledger = ledgerWithFullyReservedBuyer();
        withBothUserLocks(ledger.lockManager(), 1L, 2L, () -> {
            TradeLedgerMutation mutation = ledger.prepareTradeLocked(
                    1L, 2L, BTC, USDT, 10L, 950L, 50L);
            ledger.commitTradeLocked(mutation);
        });

        assertEquals(new BalanceSnapshot(50L, 0L), ledger.balance(1L, USDT));
        assertTrue(ledger.allAssetInvariantsHold());
    }

    /**
     * 构造部分预留的买卖双方账户。
     *
     * @return 已冻结买方 200 USDT 和卖方 2 BTC 的账本
     */
    private static AccountLedger ledgerWithBuyerAndSeller() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createBalance(1L, BTC, 0L);
        ledger.createBalance(1L, USDT, 1_000L);
        ledger.createBalance(2L, BTC, 10L);
        ledger.createBalance(2L, USDT, 0L);
        withBothUserLocks(ledger.lockManager(), 1L, 2L, () -> {
            ledger.freezeLocked(1L, USDT, 200L);
            ledger.freezeLocked(2L, BTC, 2L);
        });
        return ledger;
    }

    /**
     * 构造报价资产全部预留的买方与足额冻结基础资产的卖方账户。
     *
     * @return 具备完全成交条件的账本
     */
    private static AccountLedger ledgerWithFullyReservedBuyer() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        ledger.createBalance(1L, BTC, 0L);
        ledger.createBalance(1L, USDT, 1_000L);
        ledger.createBalance(2L, BTC, 10L);
        ledger.createBalance(2L, USDT, 0L);
        withBothUserLocks(ledger.lockManager(), 1L, 2L, () -> {
            ledger.freezeLocked(1L, USDT, 1_000L);
            ledger.freezeLocked(2L, BTC, 10L);
        });
        return ledger;
    }

    /**
     * 在两个用户的条带锁保护下执行操作。
     *
     * @param lockManager 用户条带锁管理器
     * @param firstUserId 第一个用户标识
     * @param secondUserId 第二个用户标识
     * @param action 要执行的操作
     * @note 按条带索引排序并跳过同一锁，避免多用户结算与其他锁定路径形成锁顺序反转。
     */
    private static void withBothUserLocks(
            StripedLockManager lockManager, long firstUserId, long secondUserId, ThrowingRunnable action) {
        ReentrantLock firstLock = lockManager.lockForUser(firstUserId);
        ReentrantLock secondLock = lockManager.lockForUser(secondUserId);
        List<ReentrantLock> locks = firstLock == secondLock
                ? List.of(firstLock)
                : lockManager.stripeIndexForUser(firstUserId) < lockManager.stripeIndexForUser(secondUserId)
                ? List.of(firstLock, secondLock)
                : List.of(secondLock, firstLock);
        locks.forEach(ReentrantLock::lock);
        try {
            action.run();
        } finally {
            for (int index = locks.size() - 1; index >= 0; index--) {
                locks.get(index).unlock();
            }
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
