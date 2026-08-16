package com.cex.core.chaos;

import com.cex.core.account.AccountLedger;
import com.cex.core.account.InvariantChecker;
import com.cex.core.concurrent.StripedLockManager;
import com.cex.core.order.OrderEngine;
import com.cex.core.order.OrderEvent;
import com.cex.core.order.OrderEventType;
import com.cex.core.order.OrderStatus;
import com.cex.core.risk.ApprovalDecision;
import com.cex.core.risk.ApprovalService;
import com.cex.core.risk.ManualClock;
import com.cex.core.risk.RiskPipeline;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 订单生命周期混沌测试，验证高并发、重复、乱序及终态冲突场景下的状态收敛与资产守恒。
 *
 * <p>测试使用固定数量的长生命周期线程处理独立订单，通过用户级条带锁共享线程安全的订单引擎；
 * 测试对象仅用于进程内测评，不提供生产流量回放、持久化恢复或跨进程故障演练能力。</p>
 *
 * @note 核心逻辑包括线程让出、短暂停顿和线程中断故障注入，并由 watchdog 及 worker 周期性校验
 *       总资产不变量；TPS 与延迟由 {@code PerformanceTest} 单独统计，避免混沌扰动污染性能样本。
 */
class ChaosInvariantTest {
    /** 混沌场景随机种子，默认值保证乱序序列和故障注入可复现 */
    private static final long CHAOS_SEED = Long.getLong("CHAOS_SEED", 20260816L);

    /** 并发处理订单的长生命周期工作线程数量 */
    private static final int WORKERS = 16;

    /** 混沌测试订单总数，同时也是独立测试用户数量 */
    private static final int ORDERS = 300_000;

    /** 事件时间基准，保证在指定随机种子下生成非负且不溢出的业务时间 */
    private static final long EVENT_TIME_BASE = Math.floorMod(CHAOS_SEED, Long.MAX_VALUE - ORDERS);

    /** 按随机种子洗牌后的场景循环，用于均匀覆盖成交、撤单、乱序和终态冲突 */
    private static final Scenario[] SCENARIO_CYCLE = shuffledScenarioCycle();

    /**
     * 验证带故障注入的订单生命周期最终收敛，且无资产不变量破坏、死锁或线程泄漏。
     *
     * @throws Exception worker 结果获取、线程等待或测试资源关闭失败时抛出
     * @note 16 个 worker 按订单索引分片并发处理，watchdog 同期获取一致性快照；乱序事实先缓存，
     *       CREATE 到达后由状态机补偿执行冻结及终态副作用，Effect Bit 保证重复事件不会重复结算或解冻。
     */
    @Test
    void seededLifecycleChaosConvergesWithoutInvariantFailureOrDeadlock() throws Exception {
        AccountLedger ledger = new AccountLedger(new StripedLockManager());
        for (long userId = 1L; userId <= ORDERS; userId++) {
            ledger.createAccount(userId, 2L);
        }
        ApprovalService approvals = new ApprovalService(1, 32);
        OrderEngine engine = new OrderEngine(
                ledger,
                new RiskPipeline(),
                new ManualClock(EVENT_TIME_BASE),
                approvals,
                event -> ApprovalDecision.PASS);
        InvariantChecker checker = new InvariantChecker(ledger);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean invariantFailure = new AtomicBoolean(false);
        AtomicReference<Throwable> watchdogFailure = new AtomicReference<>();
        LongAdder yieldInjections = new LongAdder();
        LongAdder parkInjections = new LongAdder();
        LongAdder interruptInjections = new LongAdder();
        Thread watchdog = new Thread(() -> {
            try {
                while (running.get()) {
                    // 在交易线程运行期间持续获取全条带一致快照，尽早发现瞬时资产破坏。
                    if (!checker.check()) {
                        invariantFailure.set(true);
                        return;
                    }
                    LockSupport.parkNanos(1_000_000L);
                }
            } catch (Throwable failure) {
                watchdogFailure.set(failure);
            }
        }, "asset-invariant-watchdog");
        ExecutorService workers = Executors.newFixedThreadPool(WORKERS);
        CountDownLatch start = new CountDownLatch(1);
        try {
            watchdog.start();
            List<Future<?>> futures = new ArrayList<>(WORKERS);
            for (int worker = 0; worker < WORKERS; worker++) {
                final int workerIndex = worker;
                futures.add(workers.submit(() -> runWorker(
                        workerIndex,
                        start,
                        engine,
                        checker,
                        invariantFailure,
                        yieldInjections,
                        parkInjections,
                        interruptInjections)));
            }
            start.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(90L, TimeUnit.SECONDS),
                    "worker termination timeout; seed=" + CHAOS_SEED);
            for (Future<?> future : futures) {
                future.get();
            }

            assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads(),
                    "deadlock detected; seed=" + CHAOS_SEED);
            running.set(false);
            watchdog.join(5_000L);
            assertFalse(watchdog.isAlive(), "watchdog did not terminate");
            assertNull(watchdogFailure.get(), "watchdog failed");
            assertFalse(invariantFailure.get(), "invariant failure; seed=" + CHAOS_SEED);
            assertTrue(checker.check());
            assertEquals(0L, checker.failureCount());

            // 逐订单核对预定终态，避免仅凭聚合资金守恒掩盖局部状态错误。
            long expectedFilled = 0L;
            long expectedCanceled = 0L;
            for (int orderIndex = 0; orderIndex < ORDERS; orderIndex++) {
                long orderId = orderIndex + 1L;
                Scenario scenario = scenarioFor(orderIndex);
                assertEquals(scenario.expectedStatus, engine.order(orderId).status(),
                        "orderId=" + orderId + ", scenario=" + scenario);
                if (scenario.expectedStatus == OrderStatus.FILLED) {
                    expectedFilled++;
                } else {
                    expectedCanceled++;
                }
            }

            assertEquals(180_000L, expectedFilled);
            assertEquals(120_000L, expectedCanceled);
            assertEquals(expectedFilled, engine.metrics().settleCount());
            assertEquals(expectedCanceled, engine.metrics().unfreezeCount());
            assertEquals(60_000L, engine.metrics().conflictingTerminalEvents());
            assertEquals(540_000L, engine.metrics().stateTransitions());
            assertEquals(ORDERS, engine.metrics().freezeCount());
            assertTrue(engine.metrics().duplicateEvents() > 0L);
            assertTrue(engine.metrics().outOfOrderEvents() > 0L);
            assertTrue(yieldInjections.sum() > 0L);
            assertTrue(parkInjections.sum() > 0L);
            assertTrue(interruptInjections.sum() > 0L);
            assertEquals(0L, ledger.currentTotalAsset() - ledger.initialTotalAsset());

            System.out.println("CHAOS SEED = " + CHAOS_SEED);
            System.out.println("Processed events: " + engine.metrics().processedEvents());
            System.out.println("Accepted facts: " + engine.metrics().acceptedFacts());
            System.out.println("Duplicate events: " + engine.metrics().duplicateEvents());
            System.out.println("Out-of-order events: " + engine.metrics().outOfOrderEvents());
            System.out.println("State transitions: " + engine.metrics().stateTransitions());
            System.out.println("Freeze count: " + engine.metrics().freezeCount());
            System.out.println("Settle count: " + engine.metrics().settleCount());
            System.out.println("Unfreeze count: " + engine.metrics().unfreezeCount());
            System.out.println("Terminal conflicts: " + engine.metrics().conflictingTerminalEvents());
            System.out.println("Expected filled: " + expectedFilled);
            System.out.println("Expected canceled: " + expectedCanceled);
            System.out.println("Yield injections: " + yieldInjections.sum());
            System.out.println("Park injections: " + parkInjections.sum());
            System.out.println("Interrupt injections: " + interruptInjections.sum());
            System.out.println("Invariant snapshots: " + checker.snapshotCount());
            System.out.println("Invariant failures: " + checker.failureCount());
            System.out.println("Asset delta: " + (ledger.currentTotalAsset() - ledger.initialTotalAsset()));
            System.out.println("Deadlock check: PASS");
            System.out.println("Termination check: PASS");
        } finally {
            running.set(false);
            workers.shutdownNow();
            watchdog.join(5_000L);
            engine.close();
        }
    }

    /**
     * 执行单个工作线程负责的订单分片，并按随机序列注入调度及中断故障。
     *
     * @param workerIndex 工作线程索引，用于划分订单及派生线程随机种子
     * @param start 全部工作线程的统一启动闩锁
     * @param engine 接收订单事件并执行状态收敛的订单引擎
     * @param checker 交易期间校验总资产不变量的检查器
     * @param invariantFailure 任意工作线程发现资产不变量失败后的共享标识
     * @param yieldInjections 已执行线程让出故障注入的计数器
     * @param parkInjections 已执行短暂停顿故障注入的计数器
     * @param interruptInjections 已执行线程中断故障注入的计数器
     * @note 每个订单仅由一个 worker 驱动，订单内部的重复和乱序由状态机处理；线程中断只作为协作信号，
     *       资金临界区仍必须完成，随后清除中断状态，防止污染线程池中的下一笔订单。
     */
    private static void runWorker(
            int workerIndex,
            CountDownLatch start,
            OrderEngine engine,
            InvariantChecker checker,
            AtomicBoolean invariantFailure,
            LongAdder yieldInjections,
            LongAdder parkInjections,
            LongAdder interruptInjections) {
        try {
            start.await();
            SplittableRandom random = new SplittableRandom(CHAOS_SEED + workerIndex);
            for (int orderIndex = workerIndex; orderIndex < ORDERS; orderIndex += WORKERS) {
                long orderId = orderIndex + 1L;
                processScenario(engine, orderId, scenarioFor(orderIndex));
                if ((orderIndex & 255) == 0) {
                    switch (random.nextInt(3)) {
                        case 0 -> {
                            yieldInjections.increment();
                            Thread.yield();
                        }
                        case 1 -> {
                            parkInjections.increment();
                            LockSupport.parkNanos(random.nextLong(10_000L, 100_001L));
                        }
                        default -> {
                            interruptInjections.increment();
                            // 在调用订单入口前设置中断位，验证资金操作不会因协作式中断留下半完成状态。
                            Thread.currentThread().interrupt();
                            engine.process(event(orderId, OrderEventType.ORDER_CREATED));
                            // 清除注入的中断位，避免同一 worker 后续订单继承故障状态。
                            Thread.interrupted();
                        }
                    }
                }
                // worker 定期校验资产守恒，补充 watchdog 可能错过的短时间窗口。
                if ((orderIndex & 1023) == 0 && !checker.check()) {
                    invariantFailure.set(true);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("worker interrupted", interrupted);
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * 按指定场景投递创建和终态事件，显式构造重复、乱序及终态冲突。
     *
     * @param engine 接收混沌事件的订单引擎
     * @param orderId 当前场景对应的订单ID
     * @param scenario 当前订单需要执行的混沌业务场景
     * @note 终态先于创建时只登记事实，不触发资金变化；CREATE 到达后执行冻结和后置补偿，
     *       重复事实依靠 Fact Bit 与 Effect Bit 共同避免重复冻结、结算或解冻。
     */
    private static void processScenario(OrderEngine engine, long orderId, Scenario scenario) {
        OrderEvent created = event(orderId, OrderEventType.ORDER_CREATED);
        OrderEvent filled = event(orderId, OrderEventType.MATCH_FILLED);
        OrderEvent cancelled = event(orderId, OrderEventType.ORDER_CANCELLED);
        switch (scenario) {
            case FILL_IN_ORDER -> {
                engine.process(created);
                engine.process(created);
                engine.process(filled);
            }
            case FILL_OUT_OF_ORDER -> {
                engine.process(filled);
                engine.process(filled);
                engine.process(created);
            }
            case CANCEL_IN_ORDER -> {
                engine.process(created);
                engine.process(cancelled);
                engine.process(cancelled);
            }
            case CANCEL_OUT_OF_ORDER -> {
                engine.process(cancelled);
                engine.process(cancelled);
                engine.process(created);
            }
            case CONFLICT_BEFORE_CREATE -> {
                engine.process(cancelled);
                engine.process(filled);
                engine.process(created);
            }
        }
    }

    /**
     * 创建金额为一个资金单位的测试订单事件。
     *
     * @param orderId 订单ID，同时作为测试用户ID以隔离用户资金
     * @param type 需要生成的订单事件类型
     * @return 具有确定事件时间和业务元数据的订单事件
     */
    private static OrderEvent event(long orderId, OrderEventType type) {
        return new OrderEvent(orderId, orderId, 1L, EVENT_TIME_BASE + orderId, type);
    }

    /**
     * 使用固定混沌种子洗牌场景枚举，生成可复现且覆盖均匀的场景循环。
     *
     * @return 洗牌后的独立场景数组
     */
    private static Scenario[] shuffledScenarioCycle() {
        Scenario[] scenarios = Scenario.values().clone();
        SplittableRandom random = new SplittableRandom(CHAOS_SEED);
        for (int index = scenarios.length - 1; index > 0; index--) {
            int swapIndex = random.nextInt(index + 1);
            Scenario current = scenarios[index];
            scenarios[index] = scenarios[swapIndex];
            scenarios[swapIndex] = current;
        }
        return scenarios;
    }

    /**
     * 根据订单索引从固定场景循环中选择业务场景。
     *
     * @param orderIndex 从零开始的订单索引
     * @return 当前订单应执行的混沌场景
     */
    private static Scenario scenarioFor(long orderIndex) {
        return SCENARIO_CYCLE[(int) (orderIndex % SCENARIO_CYCLE.length)];
    }

    /**
     * 混沌订单事件排列场景，覆盖顺序、乱序和互斥终态冲突。
     */
    private enum Scenario {
        /** 创建后成交，预期完成结算 */
        FILL_IN_ORDER(OrderStatus.FILLED),

        /** 成交先于创建到达，预期创建后补偿结算 */
        FILL_OUT_OF_ORDER(OrderStatus.FILLED),

        /** 创建后撤单，预期完成资金解冻 */
        CANCEL_IN_ORDER(OrderStatus.CANCELED),

        /** 撤单先于创建到达，预期创建后补偿解冻 */
        CANCEL_OUT_OF_ORDER(OrderStatus.CANCELED),

        /** 创建前同时缓存成交与撤单，预期按状态机优先级完成结算 */
        CONFLICT_BEFORE_CREATE(OrderStatus.FILLED);

        /** 场景完成后订单必须收敛到的业务状态 */
        private final OrderStatus expectedStatus;

        /**
         * 创建带预期终态的混沌场景。
         *
         * @param expectedStatus 场景执行完成后的订单预期状态
         */
        Scenario(OrderStatus expectedStatus) {
            this.expectedStatus = expectedStatus;
        }
    }
}
