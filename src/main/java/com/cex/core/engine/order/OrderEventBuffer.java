package com.cex.core.engine.order;

import com.cex.core.engine.event.OrderEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 订单乱序事件缓存。
 *
 * <p>保存订单创建事件到达前的成交、撤单和风控事件，并按事件到达顺序提供补偿重放；
 * 通过 eventId 去重，支持不同订单并发写入。</p>
 */
public final class OrderEventBuffer {

    /** 按订单标识保存待处理事件集合。 */
    private final ConcurrentHashMap<Long, PendingEvents> pendingByOrder =
            new ConcurrentHashMap<>();

    /**
     * 将未知订单事件写入待处理队列，并按 eventId 保证只缓存一次。
     *
     * @param event 待缓存的订单事件
     * @return 首次缓存返回 true，eventId 已存在时返回 false
     * @note 滞后事件先缓存，ORDER_CREATED 到达后由状态机 drain 并后置补偿执行；禁止绕过 eventId 去重重复写入。
     */
    public boolean add(OrderEvent event) {
        PendingEvents pending = pendingByOrder.computeIfAbsent(
                event.getOrderId(), ignored -> new PendingEvents());
        // putIfAbsent 只允许同一订单的同一 eventId 进入补偿队列一次。
        if (pending.byId.putIfAbsent(event.getEventId(), event) != null) {
            return false;
        }
        pending.arrivalOrder.add(event);
        return true;
    }

    /**
     * 判断指定事件是否仍在乱序缓存中。
     *
     * @param orderId 订单标识
     * @param eventId 事件幂等标识
     * @return 事件仍待重放时返回 true，否则返回 false
     */
    public boolean contains(long orderId, long eventId) {
        PendingEvents pending = pendingByOrder.get(orderId);
        return pending != null && pending.byId.containsKey(eventId);
    }

    /**
     * 移除并返回订单的待处理事件。
     *
     * @param orderId 已创建订单的订单标识
     * @return 按到达顺序排列的待重放事件列表，无缓存时返回空列表
     * @note 调用后事件从缓存所有权转移给状态机，必须完成后置补偿执行。
     */
    public List<OrderEvent> drain(long orderId) {
        PendingEvents pending = pendingByOrder.remove(orderId);
        if (pending == null) {
            return List.of();
        }
        List<OrderEvent> events = new ArrayList<>(pending.arrivalOrder.size());
        OrderEvent event;
        while ((event = pending.arrivalOrder.poll()) != null) {
            events.add(event);
        }
        return events;
    }

    /**
     * 获取指定订单的待处理事件数量。
     *
     * @param orderId 订单标识
     * @return 当前缓存事件数量
     */
    public int size(long orderId) {
        PendingEvents pending = pendingByOrder.get(orderId);
        return pending == null ? 0 : pending.byId.size();
    }

    /**
     * 移除指定订单的一个待处理事件。
     *
     * @param orderId 订单标识
     * @param eventId 事件幂等标识
     * @return 成功移除返回 true
     */
    public boolean remove(long orderId, long eventId) {
        PendingEvents pending = pendingByOrder.get(orderId);
        if (pending == null || pending.byId.remove(eventId) == null) {
            return false;
        }
        pending.arrivalOrder.removeIf(event -> event.getEventId() == eventId);
        if (pending.byId.isEmpty()) {
            pendingByOrder.remove(orderId, pending);
        }
        return true;
    }

    /** 单个订单的事件幂等索引和到达顺序队列。 */
    private static final class PendingEvents {

        /** 按 eventId 存储事件，用于并发幂等去重。 */
        private final ConcurrentHashMap<Long, OrderEvent> byId = new ConcurrentHashMap<>();
        /** 按到达顺序保存事件，用于 CREATE 到达后的历史重放。 */
        private final ConcurrentLinkedQueue<OrderEvent> arrivalOrder =
                new ConcurrentLinkedQueue<>();
    }
}
