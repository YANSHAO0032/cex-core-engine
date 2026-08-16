package com.cex.core.order;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单个订单的不可变元数据、滞后事实与副作用提交上下文。
 * 核心能力是以原子事实位图处理并发重放，并在用户锁内保证副作用和状态转换幂等；线程安全边界明确。
 * 限制：除事实位图和可见状态外，其余可变字段必须由同一用户的外部锁保护，不能跨订单共享。
 */
public final class OrderContext {

    /** 订单唯一标识，创建上下文后不可变。 */
    private final long orderId;
    /** 订单归属用户标识，决定资金锁粒度且不可变。 */
    private final long userId;
    /** 订单金额，单位为货币最小单位且不可变。 */
    private final long amount;
    /** 已到达事实的原子位图，允许事件乱序和并发登记。 */
    private final AtomicInteger factBits = new AtomicInteger();

    /** 已成功提交副作用的位图，仅能在用户锁内读写。 */
    private int effectBits;
    /** 对未持锁读者可见的订单状态；状态迁移仍须在用户锁内完成。 */
    private volatile OrderStatus status = OrderStatus.INIT;
    /** 是否已记录成交与取消同时到达的终态冲突，默认 false 表示尚未记录，仅在用户锁内读写。 */
    private boolean terminalConflictRecorded;
    /** 是否已记录审批通过与拒绝同时到达的冲突，默认 false 表示尚未记录，仅在用户锁内读写。 */
    private boolean approvalConflictRecorded;

    /**
     * 创建持有固定业务元数据的订单上下文。
     *
     * @param orderId 全局唯一订单ID
     * @param userId 订单归属用户ID，也是资金并发串行化边界
     * @param amount 订单金额，单位为货币最小单位
     */
    private OrderContext(long orderId, long userId, long amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    /**
     * 依据首个事件建立订单上下文，不要求该事件一定是创建事件。
     *
     * @param firstEvent 首次到达的订单事件，不能为空
     * @return 持有该事件元数据的新订单上下文
     * @note 首事件可能乱序；其事实由调用者随后登记，创建前只缓存事实不触发资金操作。
     */
    public static OrderContext fromFirstEvent(OrderEvent firstEvent) {
        Objects.requireNonNull(firstEvent, "firstEvent");
        return new OrderContext(firstEvent.orderId(), firstEvent.userId(), firstEvent.amount());
    }

    /**
     * 返回订单 ID。
     *
     * @return 不可变订单唯一标识
     */
    public long orderId() {
        return orderId;
    }

    /**
     * 返回用户 ID。
     *
     * @return 不可变用户唯一标识
     */
    public long userId() {
        return userId;
    }

    /**
     * 返回订单金额。
     *
     * @return 以货币最小单位表示的不可变金额
     */
    public long amount() {
        return amount;
    }

    /**
     * 返回当前可见订单状态。
     *
     * @return 当前状态
     */
    public OrderStatus status() {
        return status;
    }

    /**
     * 在外部用户锁持有期间更新订单状态。
     *
     * @param status 目标状态，不能为空
     * @note volatile 写让未持锁读取者获得最新状态，但状态迁移决策必须仍在同一用户锁内完成。
     */
    public void setStatusLocked(OrderStatus status) {
        this.status = Objects.requireNonNull(status, "status");
    }

    /**
     * 校验后续事件未改变订单的身份和金额元数据。
     *
     * @param event 待校验事件，不能为空
     * @throws OrderMetadataMismatchException 当订单 ID、用户 ID 或金额不一致时
     */
    public void validateMetadata(OrderEvent event) {
        Objects.requireNonNull(event, "event");
        if (orderId != event.orderId() || userId != event.userId() || amount != event.amount()) {
            throw new OrderMetadataMismatchException(
                    "order metadata mismatch for orderId=" + orderId);
        }
    }

    /**
     * 原子登记事件对应的事实位，区分首次到达与重复到达。
     *
     * @param eventType 待登记的事件类型，不能为空
     * @return 首次成功置位时为 {@link FactRegistrationResult#NEW}，否则为重复结果
     * @note 通过 CAS 循环防止并发丢失位；重复事件不清除事实，因此可安全重放并在创建事实到达后收敛。
     */
    public FactRegistrationResult registerFact(OrderEventType eventType) {
        int mask = OrderFact.fromEventType(Objects.requireNonNull(eventType, "eventType")).mask();
        while (true) {
            int current = factBits.get();
            if ((current & mask) != 0) {
                return FactRegistrationResult.DUPLICATE;
            }
            int updated = current | mask;
            // CAS 成功才确认首次事实，竞争失败后重新读取完整位图。
            if (factBits.compareAndSet(current, updated)) {
                return FactRegistrationResult.NEW;
            }
        }
    }

    /**
     * 判断某类事件事实是否已被缓存。
     *
     * @param fact 待查询事实，不能为空
     * @return 已登记该事实时为 {@code true}
     */
    public boolean hasFact(OrderFact fact) {
        return (factBits.get() & fact.mask()) != 0;
    }

    /**
     * 在用户锁内判断副作用是否已提交。
     *
     * @param effect 待查询副作用，不能为空
     * @return 副作用已成功提交时为 {@code true}
     */
    public boolean hasEffect(OrderEffect effect) {
        return (effectBits & effect.mask()) != 0;
    }

    /**
     * 在用户锁内执行一次副作用，并在执行成功后提交其幂等标记。
     *
     * @param effect 待提交的副作用标记，不能为空
     * @param operation 实际副作用操作，不能为空
     * @return 本次执行并提交标记时为 {@code true}；已提交时为 {@code false}
     * @note 标记采用后置提交：资金操作失败不会写入 Effect Bit，后续重试可补偿；调用者须持有用户锁以防重复冻结、结算或解冻。
     */
    public boolean applyEffectLocked(OrderEffect effect, LockedEffectOperation operation) {
        Objects.requireNonNull(effect, "effect");
        Objects.requireNonNull(operation, "operation");
        if (hasEffect(effect)) {
            return false;
        }
        operation.run();
        // 副作用成功后才提交 Effect Bit，避免失败被误判为已完成。
        effectBits |= effect.mask();
        return true;
    }

    /**
     * 在用户锁内首次标记终态冲突。
     *
     * @return 首次标记时为 {@code true}，已标记时为 {@code false}
     */
    public boolean markTerminalConflictLocked() {
        if (terminalConflictRecorded) {
            return false;
        }
        terminalConflictRecorded = true;
        return true;
    }

    /**
     * 在用户锁内首次标记审批结果冲突。
     *
     * @return 首次标记时为 {@code true}，已标记时为 {@code false}
     */
    public boolean markApprovalConflictLocked() {
        if (approvalConflictRecorded) {
            return false;
        }
        approvalConflictRecorded = true;
        return true;
    }

    /**
     * 由调用方在持有用户锁时执行的副作用操作。
     * 核心能力是将实际变更与 Effect Bit 提交绑定；实现的线程安全由调用方锁保证。
     * 限制：实现可能抛出运行时异常，异常时对应 Effect Bit 不会提交。
     */
    @FunctionalInterface
    public interface LockedEffectOperation {
        /** 执行一次副作用操作。 */
        void run();
    }
}
