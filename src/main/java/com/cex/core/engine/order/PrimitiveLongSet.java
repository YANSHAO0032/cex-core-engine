package com.cex.core.engine.order;

/**
 * 订单事件幂等标识的 primitive long 开放寻址集合。
 *
 * <p>使用 long 数组和开放寻址替代 HashSet<Long>，避免每个事件产生装箱对象，
 * 适配高并发交易热路径和 -Xmx256m 内存限制；本集合只由所属订单分片锁保护。</p>
 */
final class PrimitiveLongSet {

    /** 哈希表装载因子，预留空槽以保证线性探测效率。 */
    private static final float LOAD_FACTOR = 0.6f;
    /** 事件幂等标识存储数组。 */
    private long[] keys;
    /** 槽位占用标识，支持 eventId=0。 */
    private boolean[] used;
    /** 当前已存储事件标识数量。 */
    private int size;
    /** 触发扩容的元素数量阈值。 */
    private int threshold;

    /** 创建具有初始固定容量的 primitive 集合。 */
    PrimitiveLongSet() {
        resize(8);
    }

    /**
     * 添加一个事件幂等标识。
     *
     * @param key 事件幂等标识
     * @return 首次添加返回 true，已存在时返回 false
     * @note 仅执行 CAS/锁保护之外的本地数组操作；调用方必须保证同一订单不被并发写入。
     */
    boolean add(long key) {
        if (size >= threshold) {
            resize(keys.length << 1);
        }
        // 开放寻址线性探测避免 Long 装箱，减少 GC 与指针追踪。
        int index = findIndex(key, keys, used);
        if (used[index]) {
            return false;
        }
        used[index] = true;
        keys[index] = key;
        size++;
        return true;
    }

    /**
     * 判断事件幂等标识是否已存在。
     *
     * @param key 事件幂等标识
     * @return 已存在返回 true，否则返回 false
     */
    boolean contains(long key) {
        if (size == 0) {
            return false;
        }
        int index = findIndex(key, keys, used);
        return used[index];
    }

    /**
     * 扩容并重新散列已有事件标识。
     *
     * @param newCapacity 新容量，必须为 2 的幂
     */
    private void resize(int newCapacity) {
        long[] oldKeys = keys;
        boolean[] oldUsed = used;
        keys = new long[newCapacity];
        used = new boolean[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);
        if (oldKeys == null) {
            return;
        }
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldUsed[i]) {
                int index = findIndex(oldKeys[i], keys, used);
                used[index] = true;
                keys[index] = oldKeys[i];
            }
        }
    }

    /**
     * 查找事件标识对应的槽位。
     *
     * @param key 事件幂等标识
     * @param keys 事件标识数组
     * @param used 槽位占用数组
     * @return 命中槽位或第一个空槽位索引
     */
    private static int findIndex(long key, long[] keys, boolean[] used) {
        int mask = keys.length - 1;
        int index = mix(key) & mask;
        while (used[index] && keys[index] != key) {
            index = (index + 1) & mask;
        }
        return index;
    }

    /**
     * 混合 long 标识以降低相邻订单事件的哈希冲突。
     *
     * @param value 原始事件幂等标识
     * @return 混合后的 int 哈希值
     */
    private static int mix(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return (int) value;
    }
}
