package com.cex.core.engine.chaos;

import com.cex.core.engine.event.EventDispatcher;
import com.cex.core.engine.event.OrderEvent;
import com.cex.core.engine.ledger.LedgerBalance;
import com.cex.core.engine.ledger.LedgerService;
import com.cex.core.engine.order.OrderStateMachine;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CEX 内存核心组件 16 线程、60 秒混沌测试工具。
 *
 * <p>随机执行下单、撤单、成交、sleep 和 interrupt，并故意触发非法资金操作异常；
 * 结束时输出 TPS、平均延迟、P99，并校验每个用户的总资产守恒。</p>
 *
 * @note 测试包含线程中断故障注入、延迟样本统计和 available + frozen + traded 不变量校验，
 * 用于验证异常路径而非只验证成功案例。
 */
@Tag("chaos")
class ChaosConcurrencyTest {

    /** 混沌工作线程数量。 */
    private static final int WORKER_COUNT = 16;
    /** 共享测试用户数量，用于制造同一资产账户的线程竞争。 */
    private static final int USER_COUNT = 4;
    /** 每个用户可随机访问的订单槽位数量。 */
    private static final int ORDER_SLOT_COUNT = 512;
    /** 每个用户初始可用余额，资金单位为资产最小单位。 */
    private static final long INITIAL_BALANCE = 1_000_000L;
    /** 混沌持续时间，单位为毫秒，即 60 秒。 */
    private static final long CHAOS_DURATION_MILLIS = 60_000L;

    /**
     * 运行 16 线程 60 秒混沌流程并校验资产守恒。
     *
     * @throws Exception 测试线程等待或被外部中断时抛出
     * @note 测试协调线程随机 interrupt 工作线程；工作线程捕获并记录中断，finally 负责停止线程和排空事件。
     */
    @Test
    void runsSixteenThreadChaosForSixtySeconds() throws Exception {
        ChaosFixture fixture = new ChaosFixture();
        Thread[] workers = new Thread[WORKER_COUNT];
        CountDownLatch ready = new CountDownLatch(WORKER_COUNT);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> fatalFailure = new AtomicReference<>();

        fixture.dispatcher.start();
        long startedAt = System.nanoTime();
        for (int i = 0; i < WORKER_COUNT; i++) {
            final int workerId = i;
            workers[i] = new Thread(() -> fixture.runWorker(
                    workerId, ready, stop, fatalFailure), "chaos-worker-" + i);
            workers[i].start();
        }

        assertTrue(ready.await(10L, TimeUnit.SECONDS));
        long elapsedNanos;
        try {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(CHAOS_DURATION_MILLIS);
            while (System.nanoTime() < deadline && fatalFailure.get() == null) {
                Thread.sleep(10L);
                workers[ThreadLocalRandom.current().nextInt(WORKER_COUNT)].interrupt();
            }
        } finally {
            stop.set(true);
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.interrupt();
                }
            }
            for (Thread worker : workers) {
                if (worker != null) {
                    worker.join(10_000L);
                }
            }
            fixture.dispatcher.close();
            elapsedNanos = System.nanoTime() - startedAt;
        }

        ChaosMetrics metrics = fixture.metrics.snapshot(elapsedNanos);
        System.out.printf(Locale.ROOT,
                "Chaos metrics: operations=%d, TPS=%.2f, avgLatency=%.2f us, "
                        + "P99=%.2f us, expectedExceptions=%d, interrupts=%d%n",
                metrics.operations, metrics.tps, metrics.averageMicros,
                metrics.p99Micros, metrics.expectedExceptions, metrics.interrupts);

        assertNull(fatalFailure.get(), () -> "fatal worker failure: " + fatalFailure.get());
        assertTrue(metrics.operations > 0L);
        assertTrue(metrics.expectedExceptions > 0L,
                "chaos test must exercise exception paths");
        assertTrue(metrics.interrupts > 0L,
                "chaos test must exercise interrupt paths");

        for (long userId : fixture.userIds) {
            LedgerBalance balance = fixture.ledger.snapshot(userId);
            assertTrue(balance.isConserved(), "asset conservation failed for " + userId);
            assertTrue(balance.getAvailable() >= 0L && balance.getFrozen() >= 0L
                            && balance.getTraded() >= 0L,
                    "negative balance for " + userId);
        }
        long totalAvailable = 0L;
        long totalFrozen = 0L;
        for (long userId : fixture.userIds) {
            LedgerBalance balance = fixture.ledger.snapshot(userId);
            totalAvailable += balance.getAvailable();
            totalFrozen += balance.getFrozen();
        }
        assertEquals(USER_COUNT * INITIAL_BALANCE, totalAvailable + totalFrozen,
                "system asset total must be conserved across counterparties");
        assertTrue(fixture.ledger.settledTradeCount() > 0L,
                "chaos test must execute at least one two-sided settlement");
    }

    /** 组合账本、状态机和事件分发器的混沌测试夹具。 */
    private static final class ChaosFixture {

        /** 支持冻结、解冻和成交结算的内存账本。 */
        private final LedgerService ledger = new LedgerService(64);
        /** 支持乱序和幂等订单事件的状态机。 */
        private final OrderStateMachine stateMachine = new OrderStateMachine(ledger);
        /** 用于模拟 MQ 的对象事件分发器。 */
        private final EventDispatcher dispatcher = new EventDispatcher(stateMachine, 1 << 12);
        /** 按订单保存剩余冻结金额，使用 AtomicLong 支持并发扣减。 */
        private final ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicLong> reservations =
                new ConcurrentHashMap<>();
        /** 已成功提交过的订单集合，防止混沌重复下单重复冻结。 */
        private final ConcurrentHashMap<Long, Boolean> submittedOrders =
                new ConcurrentHashMap<>();
        /** 测试用户标识数组。 */
        private final long[] userIds = new long[USER_COUNT];
        /** 记录操作数量、异常、中断和延迟的指标收集器。 */
        private final ChaosMetricsRecorder metrics = new ChaosMetricsRecorder();

        /** 初始化测试用户及其守恒初始余额。 */
        private ChaosFixture() {
            for (int i = 0; i < USER_COUNT; i++) {
                userIds[i] = 10_000L + i;
                ledger.openAccount(userIds[i], INITIAL_BALANCE);
            }
        }

        /**
         * 执行单个混沌工作线程循环。
         *
         * @param workerId 工作线程标识，用于区分并发参与者
         * @param ready 工作线程就绪栅栏
         * @param stop 全局停止标识
         * @param fatalFailure 记录未预期致命异常的共享引用
         * @note 随机分发 submitOrder、cancelOrder、match；随机 sleep 和外部 interrupt 注入故障，所有操作都记录延迟。
         */
        private void runWorker(int workerId,
                               CountDownLatch ready,
                               AtomicBoolean stop,
                               AtomicReference<Throwable> fatalFailure) {
            ready.countDown();
            try {
                while (!stop.get()) {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    int action = random.nextInt(3);
                    long userId = userIds[random.nextInt(USER_COUNT)];
                    long orderId = userId * 1_000_000L
                            + random.nextInt(ORDER_SLOT_COUNT);
                    long startedAt = System.nanoTime();
                    try {
                        if (action == 0) {
                            submitOrder(userId, orderId, random);
                        } else if (action == 1) {
                            cancelOrder(userId, orderId, random);
                        } else {
                            match(userId, orderId, random);
                        }
                    } catch (IllegalArgumentException expected) {
                        metrics.expectedException();
                    } catch (InterruptedException interrupted) {
                        // 发布背压或等待被中断属于预期故障路径，清除中断标记后继续混沌循环。
                        metrics.interrupt();
                        Thread.interrupted();
                    } finally {
                        metrics.recordLatency(System.nanoTime() - startedAt);
                    }

                    if (random.nextInt(50) == 0) {
                        try {
                            Thread.sleep(random.nextInt(1, 4));
                        } catch (InterruptedException interrupted) {
                            // sleep 中断用于验证线程取消和故障恢复路径。
                            metrics.interrupt();
                            Thread.interrupted();
                        }
                    }
                }
            } catch (Throwable failure) {
                fatalFailure.compareAndSet(null, failure);
                stop.set(true);
            }
        }

        /**
         * 模拟提交订单并冻结对应用户资金。
         *
         * @param userId 下单用户标识
         * @param orderId 订单标识
         * @param random 当前工作线程随机源
         * @throws InterruptedException 事件发布期间线程被中断时抛出
         * @note 首次提交才冻结资金；非法零金额会故意触发账本异常，发布中断时补偿解冻并移除预留。
         */
        private void submitOrder(long userId,
                                 long orderId,
                                 ThreadLocalRandom random) throws InterruptedException {
            if (random.nextInt(100) == 0) {
                ledger.freeze(userId, 0L);
                return;
            }
            if (submittedOrders.putIfAbsent(orderId, Boolean.TRUE) != null) {
                return;
            }

            long amount = random.nextLong(1L, 101L);
            if (!ledger.freeze(userId, amount)) {
                submittedOrders.remove(orderId);
                return;
            }
            reservations.put(orderId, new java.util.concurrent.atomic.AtomicLong(amount));
            try {
                dispatcher.publishBlocking(OrderEvent.created(
                        orderId * 100L + 1L, orderId, userId,
                        "BTC-USDT", 1L, amount));
            } catch (InterruptedException interrupted) {
                reservations.remove(orderId);
                submittedOrders.remove(orderId);
                ledger.unfreeze(userId, amount);
                throw interrupted;
            }
        }

        /**
         * 模拟撤单、释放订单剩余冻结资金并发布撤单事件。
         *
         * @param userId 订单所属用户标识
         * @param orderId 待撤订单标识
         * @param random 当前工作线程随机源
         * @throws InterruptedException 撤单事件发布期间线程被中断时抛出
         * @note 订单可能尚未创建，撤单事件将由状态机乱序缓存；非法零金额用于制造异常路径。
         */
        private void cancelOrder(long userId,
                                 long orderId,
                                 ThreadLocalRandom random) throws InterruptedException {
            if (random.nextInt(100) == 0) {
                ledger.unfreeze(userId, 0L);
                return;
            }
            java.util.concurrent.atomic.AtomicLong reservation = reservations.get(orderId);
            if (reservation != null) {
                long amount = reservation.getAndSet(0L);
                if (amount > 0L) {
                    ledger.unfreeze(userId, amount);
                }
            }
            dispatcher.publishBlocking(OrderEvent.cancelled(orderId * 100L + 2L, orderId));
        }

        /**
         * 模拟撮合成交、扣减冻结资金并发布成交事件。
         *
         * @param userId 成交用户标识
         * @param orderId 成交订单标识
         * @param random 当前工作线程随机源
         * @throws InterruptedException 成交事件发布期间线程被中断时抛出
         * @note reservation 使用 CAS 扣减剩余冻结金额；成交事件携带买卖双方，
         * 由状态机消费时执行买方冻结到卖方可用的原子结算，成交事件可能先于 CREATE 到达状态机缓存。
         */
        private void match(long userId,
                           long orderId,
                           ThreadLocalRandom random) throws InterruptedException {
            if (random.nextInt(100) == 0) {
                ledger.tradeDebit(userId, 0L);
                return;
            }
            long amount = random.nextLong(1L, 11L);
            java.util.concurrent.atomic.AtomicLong reservation = reservations.get(orderId);
            if (reservation == null) {
                // 没有冻结预留就不能向状态机伪造成交事实。
                return;
            }
            {
                long current;
                do {
                    current = reservation.get();
                    if (current < amount) {
                        break;
                    }
                // CAS 只扣减本订单剩余预留，竞争失败时重新读取最新冻结数量。
                } while (!reservation.compareAndSet(current, current - amount));
                if (current < amount) {
                    return;
                }
            }
            long round = random.nextInt(8);
            long tradeId = orderId * 100L + 10L + round;
            long sellerUserId = sellerFor(userId);
            try {
                dispatcher.publishBlocking(OrderEvent.matchFilled(
                        tradeId, orderId, amount, tradeId, userId,
                        sellerUserId, amount));
            } catch (InterruptedException interrupted) {
                reservation.addAndGet(amount);
                throw interrupted;
            }
        }

        /** 为每个买方选择不同账户作为对手方，覆盖真实双边转账。 */
        private long sellerFor(long buyerUserId) {
            int buyerIndex = (int) (buyerUserId - 10_000L);
            return userIds[(buyerIndex + 1) % USER_COUNT];
        }
    }

    /** 线程安全记录混沌操作和固定容量延迟样本的工具。 */
    private static final class ChaosMetricsRecorder {

        /** 延迟样本固定容量，避免 60 秒测试无界持有对象导致 OOM。 */
        private static final int LATENCY_SAMPLE_CAPACITY = 1_000_000;
        /** 已完成操作计数。 */
        private final LongAdder operations = new LongAdder();
        /** 所有操作耗时总和，单位为纳秒。 */
        private final LongAdder totalNanos = new LongAdder();
        /** 预期异常计数。 */
        private final LongAdder expectedExceptions = new LongAdder();
        /** 线程中断计数。 */
        private final LongAdder interrupts = new LongAdder();
        /** 固定容量延迟样本数组。 */
        private final AtomicLongArray latencySamples =
                new AtomicLongArray(LATENCY_SAMPLE_CAPACITY);
        /** 延迟样本写入序号，用于循环复用数组槽位。 */
        private final java.util.concurrent.atomic.AtomicLong sampleSequence =
                new java.util.concurrent.atomic.AtomicLong();

        /**
         * 记录一次操作延迟。
         *
         * @param nanos 本次操作耗时，单位为纳秒
         * @note 使用 AtomicLongArray 循环覆盖样本，控制内存占用在 -Xmx256m 范围内。
         */
        private void recordLatency(long nanos) {
            operations.increment();
            totalNanos.add(nanos);
            long sequence = sampleSequence.getAndIncrement();
            latencySamples.set((int) (sequence % LATENCY_SAMPLE_CAPACITY), nanos);
        }

        /** 记录一次预期非法资金操作异常。 */
        private void expectedException() {
            expectedExceptions.increment();
        }

        /** 记录一次线程中断故障注入。 */
        private void interrupt() {
            interrupts.increment();
        }

        /**
         * 汇总 TPS、平均延迟和 P99 指标。
         *
         * @param elapsedNanos 混沌总耗时，单位为纳秒
         * @return 不可变混沌测试指标快照
         * @note P99 基于固定容量样本排序；同时保留异常、中断和操作数量用于确认并非只跑成功案例。
         */
        private ChaosMetrics snapshot(long elapsedNanos) {
            long operationCount = operations.sum();
            long storedCount = Math.min(operationCount, LATENCY_SAMPLE_CAPACITY);
            long[] samples = new long[(int) storedCount];
            for (int i = 0; i < samples.length; i++) {
                samples[i] = latencySamples.get(i);
            }
            Arrays.sort(samples);
            long p99 = samples.length == 0 ? 0L
                    : samples[Math.max(0, (int) Math.ceil(samples.length * 0.99) - 1)];
            double seconds = elapsedNanos / 1_000_000_000.0;
            return new ChaosMetrics(
                    operationCount,
                    operationCount / seconds,
                    operationCount == 0L ? 0.0
                            : totalNanos.sum() / (double) operationCount / 1_000.0,
                    p99 / 1_000.0,
                    expectedExceptions.sum(),
                    interrupts.sum());
        }
    }

    /** 混沌测试最终输出指标的不可变快照。 */
    private static final class ChaosMetrics {

        /** 混沌期间完成的操作数量。 */
        private final long operations;
        /** 每秒完成操作数量。 */
        private final double tps;
        /** 平均操作延迟，单位为微秒。 */
        private final double averageMicros;
        /** P99 操作延迟，单位为微秒。 */
        private final double p99Micros;
        /** 预期异常数量。 */
        private final long expectedExceptions;
        /** 中断故障注入数量。 */
        private final long interrupts;

        /**
         * 创建混沌测试指标快照。
         *
         * @param operations 操作数量
         * @param tps TPS
         * @param averageMicros 平均延迟，单位为微秒
         * @param p99Micros P99 延迟，单位为微秒
         * @param expectedExceptions 预期异常数量
         * @param interrupts 中断数量
         */
        private ChaosMetrics(long operations,
                             double tps,
                             double averageMicros,
                             double p99Micros,
                             long expectedExceptions,
                             long interrupts) {
            this.operations = operations;
            this.tps = tps;
            this.averageMicros = averageMicros;
            this.p99Micros = p99Micros;
            this.expectedExceptions = expectedExceptions;
            this.interrupts = interrupts;
        }
    }
}
