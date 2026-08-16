package com.cex.core.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * 验证条带锁管理器的默认配置、用户映射及参数校验。
 *
 * <p>线程安全：测试只验证锁实例映射，不持久化或共享业务状态。</p>
 * <p>限制：未覆盖高竞争负载下的锁吞吐表现。</p>
 */
class StripedLockManagerTest {

    /** 场景：无参构造器应采用约定的默认条带数量。 */
    @Test
    void defaultConstructorUsesCanonicalStripeCount() {
        StripedLockManager lockManager = new StripedLockManager();

        assertEquals(256, lockManager.stripeCount());
    }

    /** 场景：同一用户应映射到按哈希计算的固定条带锁。 */
    @Test
    void stripeIndexUsesCanonicalUserHash() {
        StripedLockManager lockManager = new StripedLockManager(512);
        long userId = 987_654_321L;

        int expectedIndex = Long.hashCode(userId) & (512 - 1);

        assertEquals(expectedIndex, lockManager.stripeIndexForUser(userId));
        assertSame(lockManager.lockForUser(userId), lockManager.lockForUser(userId));
    }

    /** 场景：非二的幂或非正数的条带数量应被拒绝。 */
    @Test
    void stripeCountMustBePowerOfTwo() {
        assertThrows(IllegalArgumentException.class, () -> new StripedLockManager(255));
        assertThrows(IllegalArgumentException.class, () -> new StripedLockManager(0));
    }
}
