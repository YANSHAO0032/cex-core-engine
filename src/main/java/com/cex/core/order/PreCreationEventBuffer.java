package com.cex.core.order;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 缓存订单创建前到达的撤单确认。
 *
 * <p>核心能力：按订单与权威序号保存有界确认，并区分精确重复和同序号载荷冲突。</p>
 * <p>线程安全：不同订单通过并发映射独立处理，同一订单由映射的原子计算串行化，不使用全局锁。</p>
 * <p>使用限制：只接受 {@link CancelConfirmation}；提前到达的双边成交必须保存在成交存储中。</p>
 *
 * @note 每订单容量固定且不驱逐已接受确认；精确重复即使容量已满也保持幂等成功。
 */
public final class PreCreationEventBuffer {
    /** 每个尚未创建订单默认允许缓存的撤单确认数。 */
    public static final int DEFAULT_PER_ORDER_CAPACITY = 1_024;

    /** 每订单固定容量。 */
    private final int perOrderCapacity;
    /** 按订单标识保存的序号有序确认。 */
    private final ConcurrentMap<Long, NavigableMap<Long, CancelConfirmation>> confirmations =
            new ConcurrentHashMap<>();

    /**
     * 使用默认每订单容量创建缓存。
     */
    public PreCreationEventBuffer() {
        this(DEFAULT_PER_ORDER_CAPACITY);
    }

    /**
     * 使用固定每订单容量创建缓存。
     *
     * @param perOrderCapacity 单个订单可缓存的确认数，必须严格为正
     * @throws IllegalArgumentException 当容量不为正数时抛出
     */
    public PreCreationEventBuffer(int perOrderCapacity) {
        if (perOrderCapacity <= 0) {
            throw new IllegalArgumentException("perOrderCapacity must be positive");
        }
        this.perOrderCapacity = perOrderCapacity;
    }

    /**
     * 登记一个创建前撤单确认。
     *
     * @param confirmation 待缓存的不可变撤单确认，不能为空
     * @throws NullPointerException 当确认为 {@code null} 时抛出
     * @throws TradeSequenceConflictException 当同一订单同一序号已有不同载荷时抛出
     * @throws IllegalStateException 当新序号超过该订单固定缓存容量时抛出
     * @note 同一订单的比较、容量检查和插入在一次映射原子计算中完成，不会覆盖权威载荷。
     */
    public void register(CancelConfirmation confirmation) {
        Objects.requireNonNull(confirmation, "confirmation");
        confirmations.compute(confirmation.orderId(), (orderId, existing) -> {
            NavigableMap<Long, CancelConfirmation> ordered =
                    existing == null ? new TreeMap<>() : existing;
            CancelConfirmation accepted = ordered.get(confirmation.orderSequence());
            if (accepted != null) {
                if (accepted.equals(confirmation)) {
                    return ordered;
                }
                throw new TradeSequenceConflictException(
                        "different pre-creation confirmation for orderId=" + orderId
                                + ", sequence=" + confirmation.orderSequence());
            }
            if (ordered.size() >= perOrderCapacity) {
                throw new IllegalStateException(
                        "pre-creation confirmation capacity exceeded for orderId=" + orderId);
            }
            ordered.put(confirmation.orderSequence(), confirmation);
            return ordered;
        });
    }

    /**
     * 原子移除并返回指定订单的全部确认。
     *
     * @param orderId 严格为正的订单标识
     * @return 按权威序号升序排列的不可修改确认列表；没有缓存时为空列表
     * @throws IllegalArgumentException 当订单标识不为正数时抛出
     * @note 调用方应在订单所属用户锁内完成向 {@link OrderContext} 的转移；并发晚到确认须在登记后重新检查订单发布状态。
     */
    public List<CancelConfirmation> removeAll(long orderId) {
        if (orderId <= 0L) {
            throw new IllegalArgumentException("orderId must be positive");
        }
        NavigableMap<Long, CancelConfirmation> removed = confirmations.remove(orderId);
        return removed == null ? List.of() : List.copyOf(removed.values());
    }
}
