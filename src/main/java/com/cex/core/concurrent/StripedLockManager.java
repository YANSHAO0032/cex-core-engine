package com.cex.core.concurrent;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 将用户标识映射到固定数量可重入锁的条带锁管理器。
 *
 * <p>能力：为同一用户提供稳定锁实例，并允许全局检查按条带获取锁。</p>
 * <p>线程安全：锁数组在构造后不可变，返回的 {@link ReentrantLock} 可由多个线程安全使用。</p>
 * <p>限制：不同用户可能散列到同一条带，条带数必须是二的幂以支持掩码寻址。</p>
 */
public final class StripedLockManager {

    /** 默认条带数量，在锁粒度与对象数量之间取得平衡。 */
    public static final int DEFAULT_STRIPE_COUNT = 256;

    /** 按条带索引存放的可重入锁数组。 */
    private final ReentrantLock[] stripes;
    /** 用于将用户哈希快速限制到有效条带范围内的位掩码。 */
    private final int stripeMask;

    /** 创建使用默认条带数的锁管理器。 */
    public StripedLockManager() {
        this(DEFAULT_STRIPE_COUNT);
    }

    /**
     * 创建使用指定条带数的锁管理器。
     *
     * @param stripeCount 条带数量，必须为正数且为二的幂
     * @throws IllegalArgumentException 当条带数量不满足要求时抛出
     */
    public StripedLockManager(int stripeCount) {
        if (stripeCount <= 0 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        this.stripes = new ReentrantLock[stripeCount];
        for (int index = 0; index < stripeCount; index++) {
            stripes[index] = new ReentrantLock();
        }
        this.stripeMask = stripeCount - 1;
    }

    /**
     * 获取可用条带数量。
     *
     * @return 条带数量
     */
    public int stripeCount() {
        return stripes.length;
    }

    /**
     * 计算用户对应的条带索引。
     *
     * @param userId 用户标识
     * @return 位于有效范围内的条带索引
     */
    public int stripeIndexForUser(long userId) {
        return Long.hashCode(userId) & stripeMask;
    }

    /**
     * 获取保护指定用户操作的稳定条带锁。
     *
     * @param userId 用户标识
     * @return 对应的可重入锁
     * @note 调用方负责以 {@code lock()/unlock()} 成对管理锁的生命周期。
     */
    public ReentrantLock lockForUser(long userId) {
        return stripes[stripeIndexForUser(userId)];
    }

    /**
     * 按索引获取条带锁。
     *
     * @param stripeIndex 条带索引
     * @return 指定条带的可重入锁
     * @throws IndexOutOfBoundsException 当索引不在有效范围内时抛出
     */
    public ReentrantLock lockForStripe(int stripeIndex) {
        if (stripeIndex < 0 || stripeIndex >= stripes.length) {
            throw new IndexOutOfBoundsException("stripeIndex=" + stripeIndex);
        }
        return stripes[stripeIndex];
    }
}
