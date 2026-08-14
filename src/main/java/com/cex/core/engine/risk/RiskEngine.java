package com.cex.core.engine.risk;

import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.order.OrderStateMachine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于内存滑动窗口的用户成交金额风控引擎。
 *
 * <p>默认统计最近 10 秒成交金额；每个用户拥有一个窗口，并由用户锁分片保护，
 * 超过阈值后可产生独立 RISK_HOLD 事件。</p>
 */
public final class RiskEngine {

    /** 默认成交金额统计窗口，单位为毫秒，即 10 秒。 */
    public static final long DEFAULT_WINDOW_MILLIS = 10_000L;
    /** 默认风控用户锁分片数量。 */
    private static final int DEFAULT_STRIPE_COUNT = 1 << 10;

    /** 用户十秒窗口累计成交金额阈值，资金单位为资产最小单位。 */
    private final long thresholdAmount;
    /** 当前风控窗口时长，单位为毫秒。 */
    private final long windowMillis;
    /** 按用户保存成交滑动窗口。 */
    private final ConcurrentHashMap<Long, UserRiskWindow> windows =
            new ConcurrentHashMap<>();
    /** 用户风控窗口锁分片。 */
    private final ReentrantLock[] stripes;
    /** 用户锁分片掩码。 */
    private final int stripeMask;

    /**
     * 使用默认 10 秒窗口和默认锁分片创建风控引擎。
     *
     * @param thresholdAmount 十秒成交金额阈值，使用资产最小资金单位
     */
    public RiskEngine(long thresholdAmount) {
        this(thresholdAmount, DEFAULT_WINDOW_MILLIS, DEFAULT_STRIPE_COUNT);
    }

    /**
     * 创建可配置窗口的内存风控引擎。
     *
     * @param thresholdAmount 成交金额阈值，使用资产最小资金单位
     * @param windowMillis 成交金额统计窗口，单位为毫秒
     * @param stripeCount 用户风控锁分片数量，必须为正数且为 2 的幂
     * @throws IllegalArgumentException 参数不满足正数或分片约束时抛出
     */
    public RiskEngine(long thresholdAmount, long windowMillis, int stripeCount) {
        if (thresholdAmount <= 0L) {
            throw new IllegalArgumentException("thresholdAmount must be positive");
        }
        if (windowMillis <= 0L) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        if (stripeCount < 1 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        this.thresholdAmount = thresholdAmount;
        this.windowMillis = windowMillis;
        this.stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            this.stripes[i] = new ReentrantLock(false);
        }
        this.stripeMask = stripeCount - 1;
    }

    /**
     * 记录一笔成交并评估用户最近十秒累计成交金额。
     *
     * @param userId 成交用户标识
     * @param orderId 成交所属订单标识
     * @param tradeId 成交幂等标识
     * @param amount 成交金额，使用资产最小资金单位且必须为正数
     * @param timestampMillis 成交处理时间，单位为毫秒
     * @return 当前窗口金额、阈值和风险状态组成的不可变结果
     * @note 用户锁分片保证同一用户窗口的 Deque、去重集合和累计金额原子更新；不同用户可并发处理。
     * @note 重复 tradeId 不重复累计，但仍返回当前窗口风险状态。
     */
    public RiskDecision recordTrade(long userId,
                                    long orderId,
                                    long tradeId,
                                    long amount,
                                    long timestampMillis) {
        if (amount <= 0L) {
            throw new IllegalArgumentException("amount must be positive");
        }
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            UserRiskWindow userWindow = windows.computeIfAbsent(
                    userId, ignored -> new UserRiskWindow(windowMillis));
            SlidingWindow.RecordResult result = userWindow.window.record(
                    tradeId, amount, timestampMillis);
            // 超过阈值才进入风险冻结，等于阈值仍保持 NORMAL。
            RiskState state = result.getTotalAmount() > thresholdAmount
                    ? RiskState.RISK_HOLD : RiskState.NORMAL;
            return new RiskDecision(userId, orderId, result.getTotalAmount(),
                    thresholdAmount, state, result.isAccepted());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 记录成交并在超阈值时向订单状态机应用独立风险事件。
     *
     * @param stateMachine 接收 RISK_HOLD 事件的订单状态机
     * @param riskEventId 风控事件幂等标识，必须区别于成交事件标识
     * @param userId 成交用户标识
     * @param orderId 需要评估的订单标识
     * @param tradeId 成交幂等标识
     * @param amount 成交金额，使用资产最小资金单位
     * @param timestampMillis 成交处理时间，单位为毫秒
     * @return 风控评估结果
     * @throws NullPointerException stateMachine 为空时抛出
     * @note 风控计算和订单事件应用分两步完成；风险事件禁止复用成交 eventId，避免状态机幂等集合误判。
     */
    public RiskDecision recordTradeAndApply(OrderStateMachine stateMachine,
                                            long riskEventId,
                                            long userId,
                                            long orderId,
                                            long tradeId,
                                            long amount,
                                            long timestampMillis) {
        if (stateMachine == null) {
            throw new NullPointerException("stateMachine");
        }
        RiskDecision decision = recordTrade(userId, orderId, tradeId, amount, timestampMillis);
        if (decision.isRiskHold()) {
            stateMachine.apply(OrderEvent.riskHold(riskEventId, orderId));
        }
        return decision;
    }

    /**
     * Record a trade, apply RISK_HOLD when needed, and submit an async
     * in-memory approval task for the held order.
     *
     * @return created approval task when risk hold is triggered; otherwise null
     */
    public ApprovalTask recordTradeAndSubmitApproval(OrderStateMachine stateMachine,
                                                     ApprovalTaskService approvalTasks,
                                                     long taskId,
                                                     long riskEventId,
                                                     long approvalEventId,
                                                     long userId,
                                                     long orderId,
                                                     long tradeId,
                                                     long amount,
                                                     long timestampMillis) {
        if (approvalTasks == null) {
            throw new NullPointerException("approvalTasks");
        }
        RiskDecision decision = recordTradeAndApply(stateMachine, riskEventId,
                userId, orderId, tradeId, amount, timestampMillis);
        if (!decision.isRiskHold()) {
            return null;
        }
        return approvalTasks.submit(taskId, approvalEventId, userId, orderId,
                amount, timestampMillis);
    }

    /**
     * 清理指定用户滑动窗口中的过期成交。
     *
     * @param userId 用户标识
     * @param nowMillis 当前处理时间，单位为毫秒
     * @return 实际清理的过期成交数量
     * @note 只访问指定用户窗口，不扫描全部用户和全部交易；窗口内部只从 Deque 头部回收。
     */
    public int expireUser(long userId, long nowMillis) {
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            UserRiskWindow userWindow = windows.get(userId);
            return userWindow == null ? 0 : userWindow.window.expireOldTransactions(nowMillis);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取指定用户当前十秒窗口累计成交金额。
     *
     * @param userId 用户标识
     * @return 窗口累计成交金额，使用资产最小资金单位；用户无窗口时返回 0
     */
    public long currentWindowAmount(long userId) {
        ReentrantLock lock = lockFor(userId);
        lock.lock();
        try {
            UserRiskWindow userWindow = windows.get(userId);
            return userWindow == null ? 0L : userWindow.window.getTotalAmount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据用户标识选择风控锁分片。
     *
     * @param userId 用户标识
     * @return 用户对应的非公平锁分片
     */
    private ReentrantLock lockFor(long userId) {
        int hash = Long.hashCode(userId);
        hash ^= hash >>> 16;
        return stripes[hash & stripeMask];
    }

    /** 单用户风控窗口容器，访问由 RiskEngine 用户分片锁保护。 */
    private static final class UserRiskWindow {

        /** 用户独占的成交金额滑动窗口。 */
        private final SlidingWindow window;

        /**
         * 创建用户风控窗口。
         *
         * @param windowMillis 窗口时长，单位为毫秒
         */
        private UserRiskWindow(long windowMillis) {
            this.window = new SlidingWindow(windowMillis);
        }
    }
}
