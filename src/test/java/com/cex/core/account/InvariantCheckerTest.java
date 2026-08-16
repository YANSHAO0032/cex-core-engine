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
        ledger.createAccount(1L, 100L);
        ledger.createAccount(2L, 200L);
        InvariantChecker checker = new InvariantChecker(ledger);

        assertTrue(checker.check());
        assertEquals(1L, checker.snapshotCount());
        assertEquals(0L, checker.failureCount());
    }
}
