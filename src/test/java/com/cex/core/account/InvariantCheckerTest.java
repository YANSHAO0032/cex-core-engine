package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证不变量检查器在全条带锁保护下生成一致的账本快照。
 *
 * <p>线程安全：检查器通过锁住所有条带避免与账户写操作并发交错。</p>
 * <p>限制：本测试只覆盖守恒成立的基础快照场景。</p>
 */
class InvariantCheckerTest {
    /** 场景：检查器锁定全部条带后应报告资产守恒并累计快照次数。 */
    @Test
    void snapshotLocksAllStripesAndPreservesAssetInvariant() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager(8));
        com.cex.core.order.AssetId btc = new com.cex.core.order.AssetId("BTC");
        com.cex.core.order.AssetId usdt = new com.cex.core.order.AssetId("USDT");
        ledger.createBalance(1L, btc, 100L);
        ledger.createBalance(1L, usdt, 200L);
        ledger.createBalance(2L, btc, 300L);
        InvariantChecker checker = new InvariantChecker(ledger);

        assertTrue(checker.check());
        assertTrue(ledger.allAssetInvariantsHold());
        assertEquals(1L, checker.snapshotCount());
        assertEquals(0L, checker.failureCount());
    }

    /** 场景：即使资产总额恰好守恒，负余额桶也必须使检查器报告失败。 */
    @Test
    void checkRejectsNegativeBalanceBucketEvenWhenAssetTotalMatches() {
        AccountLedger ledger = new AccountLedger(new StripedLockManager(8));
        com.cex.core.order.AssetId btc = new com.cex.core.order.AssetId("BTC");
        ledger.createBalance(1L, btc, 100L);
        AssetBalance balance = ledger.getRequiredAccount(1L).requiredBalance(btc);
        balance.setAvailable(-1L);
        balance.setFrozen(101L);
        InvariantChecker checker = new InvariantChecker(ledger);

        assertTrue(ledger.invariantHolds(btc));
        assertFalse(checker.check());
        assertEquals(1L, checker.failureCount());
    }
}
