package com.cex.core.account;

import com.cex.core.concurrent.StripedLockManager;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 在全条带锁保护下检查账本资产守恒不变量。
 *
 * <p>能力：生成一致性快照并累计检查次数与失败次数。</p>
 * <p>线程安全：检查时按固定顺序持有全部条带锁，统计量使用 {@link LongAdder} 并发累计。</p>
 * <p>限制：检查会阻塞所有账户写操作，适合监控、诊断或低频校验，不应用于高频业务路径。</p>
 */
public final class InvariantChecker {
    /** 被校验的账户账本。 */
    private final AccountLedger ledger;
    /** 与账本账户操作共享的条带锁。 */
    private final StripedLockManager locks;
    /** 已完成的一致性快照次数。 */
    private final LongAdder snapshots = new LongAdder();
    /** 检测到资产不守恒的次数。 */
    private final LongAdder failures = new LongAdder();

    /**
     * 创建账本不变量检查器。
     *
     * @param ledger 要检查的账本，不能为空
     * @throws NullPointerException 当账本为空时抛出
     */
    public InvariantChecker(AccountLedger ledger) {
        this.ledger = java.util.Objects.requireNonNull(ledger, "ledger");
        this.locks = ledger.lockManager();
    }

    /**
     * 锁定全部条带后检查当前资产总额是否守恒。
     *
     * @return 守恒时为 {@code true}
     * @note 按条带索引升序加锁、逆序释放，避免与采用相同顺序的操作发生死锁。
     */
    public boolean check() {
        int acquired = 0;
        try {
            for (; acquired < locks.stripeCount(); acquired++) {
                // 固定全局顺序获取锁，建立一致快照并避免锁顺序反转。
                locks.lockForStripe(acquired).lock();
            }
            snapshots.increment();
            boolean valid = ledger.allAssetInvariantsHold();
            if (!valid) {
                failures.increment();
            }
            return valid;
        } finally {
            for (int index = acquired - 1; index >= 0; index--) {
                ReentrantLock lock = locks.lockForStripe(index);
                lock.unlock();
            }
        }
    }

    /**
     * 获取已执行的一致性快照次数。
     *
     * @return 快照次数
     */
    public long snapshotCount() { return snapshots.sum(); }

    /**
     * 获取检测到的不变量失败次数。
     *
     * @return 失败次数
     */
    public long failureCount() { return failures.sum(); }
}
