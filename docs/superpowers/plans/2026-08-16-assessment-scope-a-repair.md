# Technical Assessment Scope A Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the approval gate and state publication defects, then make the chaos and performance tests prove the behavior and thresholds required by the technical assessment.

**Architecture:** Keep the existing monotonic fact bits, exactly-once effect bits, per-user striped locks, single-asset ledger, and asynchronous approval event loop. Reorder reconciliation so risk is decided before a cached fill can settle, publish status with `volatile`, and strengthen tests without adding production dependencies or a counterparty model.

**Tech Stack:** Java 21, JDK concurrency utilities, JUnit Jupiter 5.10.2, Maven Surefire 3.2.5, G1GC, `-Xmx256m`.

**Spec:** `docs/superpowers/specs/2026-08-16-assessment-scope-a-repair-design.md`

## Global Constraints

- Preserve the invariant `sum(available + frozen) + systemSettledAmount = initialTotalAsset`.
- Keep `pom.xml` free of Spring, persistence, messaging, database, and benchmarking dependencies.
- Keep test JVM arguments exactly `-Xms128m -Xmx256m -XX:+UseG1GC`.
- Keep 16 long-lived chaos workers and at least 500,000 state transitions.
- Require representative state-machine throughput of at least 10,000 TPS and average latency below 1 ms.
- Do not add counterparties, base/quote assets, partial fills, an order book, or persistence.
- Use test-driven development: observe every new regression test fail before changing production code.

## File Map

- Modify `src/main/java/com/cex/core/order/OrderEngine.java`: enforce risk and approval ordering during reconciliation.
- Modify `src/main/java/com/cex/core/order/OrderContext.java`: publish order status safely.
- Modify `src/test/java/com/cex/core/risk/ApprovalTest.java`: cover cached fills before and during hold for both approval outcomes.
- Modify `src/test/java/com/cex/core/order/OrderContextTest.java`: enforce the volatile status contract.
- Modify `src/test/java/com/cex/core/chaos/ChaosInvariantTest.java`: generate seeded create/fill/cancel/conflict workloads and surface worker failures.
- Modify `src/test/java/com/cex/core/performance/PerformanceTest.java`: separate duplicate hot-path and representative lifecycle measurements.
- Modify `README.md`: document corrected transitions and copy only freshly measured results.

---

### Task 1: Gate cached fills on risk approval

**Files:**
- Modify: `src/test/java/com/cex/core/risk/ApprovalTest.java`
- Modify: `src/main/java/com/cex/core/order/OrderEngine.java:91-146`

**Interfaces:**
- Consumes: existing `OrderEngine.process(OrderEvent)`, monotonic order facts, `ApprovalService`, and `SlidingWindowAmountRule`.
- Produces: reconciliation semantics in which `FILLED_SEEN` is retained during hold and applied only after approval; no public signature changes.

- [ ] **Step 1: Add a shared blocking approval fixture to `ApprovalTest`**

Add these imports and nested helper. The helper creates one settled seed order so threshold `0` holds the next order.

```java
import java.util.concurrent.CountDownLatch;

private static final class BlockingApprovalFixture implements AutoCloseable {
    private final AccountLedger ledger = new AccountLedger(new StripedLockManager());
    private final CountDownLatch approvalEntered = new CountDownLatch(1);
    private final CountDownLatch releaseApproval = new CountDownLatch(1);
    private final ApprovalService approvals = new ApprovalService(1, 8);
    private final OrderEngine engine;

    private BlockingApprovalFixture(ApprovalDecision decision) {
        ledger.createAccount(1L, 1_000L);
        engine = new OrderEngine(
                ledger,
                new RiskPipeline(new SlidingWindowAmountRule(0L)),
                new ManualClock(1L),
                approvals,
                event -> {
                    approvalEntered.countDown();
                    try {
                        releaseApproval.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return ApprovalDecision.REJECT;
                    }
                    return decision;
                });
        process(1L, OrderEventType.ORDER_CREATED);
        process(1L, OrderEventType.MATCH_FILLED);
    }

    private void process(long orderId, OrderEventType type) {
        engine.process(new OrderEvent(orderId, 1L, 100L, 1L, type));
    }

    private void awaitApprovalEntry() throws InterruptedException {
        assertTrue(approvalEntered.await(2L, TimeUnit.SECONDS));
    }

    @Override
    public void close() {
        releaseApproval.countDown();
        engine.close();
    }
}
```

- [ ] **Step 2: Add the rejecting held-fill regression test**

```java
@Test
void fillDuringRiskHoldWaitsAndRejectedApprovalCancelsWithoutSettlement() throws Exception {
    try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
        fixture.process(2L, OrderEventType.ORDER_CREATED);
        fixture.awaitApprovalEntry();
        assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());

        fixture.process(2L, OrderEventType.MATCH_FILLED);
        fixture.process(2L, OrderEventType.MATCH_FILLED);

        assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());
        assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

        fixture.releaseApproval.countDown();
        fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

        assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());
        assertEquals(900L, fixture.ledger.getRequiredAccount(1L).available());
        assertEquals(0L, fixture.ledger.getRequiredAccount(1L).frozen());
        assertEquals(1L, fixture.engine.metrics().settleCount());
        assertEquals(1L, fixture.engine.metrics().unfreezeCount());
        assertTrue(fixture.ledger.invariantHolds());
    }
}
```

- [ ] **Step 3: Add the approving cached-fill regression test**

```java
@Test
void approvedRiskHoldAppliesCachedFillExactlyOnce() throws Exception {
    try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.PASS)) {
        fixture.process(2L, OrderEventType.ORDER_CREATED);
        fixture.awaitApprovalEntry();
        fixture.process(2L, OrderEventType.MATCH_FILLED);
        fixture.process(2L, OrderEventType.MATCH_FILLED);

        assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());

        fixture.releaseApproval.countDown();
        fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);

        assertEquals(OrderStatus.FILLED, fixture.engine.order(2L).status());
        assertEquals(200L, fixture.ledger.systemSettledAmount());
        assertEquals(2L, fixture.engine.metrics().settleCount());
        assertEquals(0L, fixture.engine.metrics().unfreezeCount());
        assertTrue(fixture.ledger.invariantHolds());
    }
}
```

- [ ] **Step 4: Add the fill-before-create risk regression test**

This prevents a cached out-of-order fill from skipping initial risk evaluation when CREATE finally arrives.

```java
@Test
void fillBeforeCreateStillEntersRiskHoldBeforeSettlement() throws Exception {
    try (BlockingApprovalFixture fixture = new BlockingApprovalFixture(ApprovalDecision.REJECT)) {
        fixture.process(2L, OrderEventType.MATCH_FILLED);
        fixture.process(2L, OrderEventType.ORDER_CREATED);
        fixture.awaitApprovalEntry();

        assertEquals(OrderStatus.RISK_HOLD, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());
        assertEquals(100L, fixture.ledger.getRequiredAccount(1L).frozen());

        fixture.releaseApproval.countDown();
        fixture.engine.awaitApprovals(2L, TimeUnit.SECONDS);
        assertEquals(OrderStatus.CANCELED, fixture.engine.order(2L).status());
        assertEquals(100L, fixture.ledger.systemSettledAmount());
        assertTrue(fixture.ledger.invariantHolds());
    }
}
```

- [ ] **Step 5: Run the three new tests and confirm the current implementation fails**

Run:

```powershell
$env:JAVA_HOME='C:/Users/10703/.jdks/ms-21.0.12'
$env:Path="$env:JAVA_HOME/bin;$env:Path"
mvn '-Dtest=ApprovalTest#fillDuringRiskHoldWaitsAndRejectedApprovalCancelsWithoutSettlement+approvedRiskHoldAppliesCachedFillExactlyOnce+fillBeforeCreateStillEntersRiskHoldBeforeSettlement' test
```

Expected: at least the rejecting and fill-before-create tests fail because status becomes `FILLED` and `systemSettledAmount` becomes `200` before approval.

- [ ] **Step 6: Reorder `OrderEngine.reconcileLocked` around a one-time initial risk decision**

Keep the existing conflict detection and missing-CREATE return. Replace the body after `applyFreezeLocked(context)` with this ordering, extracting the existing risk evaluation into the shown helper:

```java
if (rejected) {
    if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
        applyUnfreezeLocked(context);
        transitionLocked(context, OrderStatus.CANCELED);
    }
    return ReconcileResult.NONE;
}
if (cancelled && !filled) {
    if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
        applyUnfreezeLocked(context);
        transitionLocked(context, OrderStatus.CANCELED);
    }
    return ReconcileResult.NONE;
}

if (context.status() == OrderStatus.INIT) {
    if (approved) {
        transitionLocked(context, OrderStatus.NEW);
    } else {
        ReconcileResult riskResult = evaluateInitialRiskLocked(context);
        if (riskResult.approvalEvent != null || context.status() == OrderStatus.RISK_HOLD) {
            return riskResult;
        }
    }
}

if (context.status() == OrderStatus.RISK_HOLD) {
    if (!approved) {
        return ReconcileResult.NONE;
    }
    transitionLocked(context, OrderStatus.NEW);
}

if (filled) {
    if (!context.hasEffect(OrderEffect.UNFREEZE_APPLIED)) {
        applySettleLocked(context);
    }
    if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
        return ReconcileResult.NONE;
    }
    recordRiskTradeLocked(context);
    transitionLocked(context, OrderStatus.FILLED);
    return ReconcileResult.NONE;
}
if (cancelled) {
    if (!context.hasEffect(OrderEffect.SETTLE_APPLIED)) {
        applyUnfreezeLocked(context);
        transitionLocked(context, OrderStatus.CANCELED);
    }
    return ReconcileResult.NONE;
}
return ReconcileResult.NONE;
```

Add this helper using the existing risk, window, metrics, and approval-event code:

```java
private ReconcileResult evaluateInitialRiskLocked(OrderContext context) {
    TradeWindow window = tradeWindows.computeIfAbsent(
            context.userId(), id -> new TradeWindow(RISK_WINDOW_MILLIS));
    long now = clock.currentTimeMillis();
    RiskContext riskContext = new RiskContext(
            context.orderId(), context.userId(), context.amount(), now, window.currentSum(now));
    if (riskPipeline.evaluate(riskContext) == RiskDecision.HOLD) {
        transitionLocked(context, OrderStatus.RISK_HOLD);
        metrics.riskHold();
        if (context.applyEffectLocked(OrderEffect.APPROVAL_SCHEDULED, () -> { })) {
            metrics.approvalScheduled();
            return new ReconcileResult(new OrderEvent(
                    context.orderId(), context.userId(), context.amount(), now,
                    OrderEventType.ORDER_CREATED));
        }
        return ReconcileResult.NONE;
    }
    transitionLocked(context, OrderStatus.NEW);
    return ReconcileResult.NONE;
}
```

This also stops duplicate events from re-evaluating an already accepted `NEW` order against later window activity.

- [ ] **Step 7: Run focused order and risk tests**

Run:

```powershell
mvn -Dtest=ApprovalTest,RiskEngineTest,OutOfOrderStateMachineTest,TerminalConflictTest test
```

Expected: all focused tests pass; the new held-fill tests prove no pre-approval settlement, and existing terminal conflict precedence remains unchanged.

- [ ] **Step 8: Commit the risk gate**

```powershell
git add src/main/java/com/cex/core/order/OrderEngine.java src/test/java/com/cex/core/risk/ApprovalTest.java
git commit -m "fix: gate held fills on risk approval"
```

---

### Task 2: Publish order status safely

**Files:**
- Modify: `src/test/java/com/cex/core/order/OrderContextTest.java`
- Modify: `src/main/java/com/cex/core/order/OrderContext.java:13-16`

**Interfaces:**
- Consumes: public `OrderContext.status()` used by synchronous and asynchronous callers.
- Produces: a volatile publication contract with no signature or locking changes.

- [ ] **Step 1: Add a deterministic modifier contract test**

Add `java.lang.reflect.Modifier` and this test to `OrderContextTest`:

```java
@Test
void statusFieldIsVolatileForUnlockedReaders() throws Exception {
    int modifiers = OrderContext.class.getDeclaredField("status").getModifiers();
    assertTrue(Modifier.isVolatile(modifiers));
}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run:

```powershell
mvn -Dtest=OrderContextTest#statusFieldIsVolatileForUnlockedReaders test
```

Expected: FAIL because `status` is currently a plain private field.

- [ ] **Step 3: Make the minimal publication change**

Change only the field declaration:

```java
private volatile OrderStatus status = OrderStatus.INIT;
```

Do not make `effectBits` volatile and do not remove the user-lock requirement from mutation methods.

- [ ] **Step 4: Run context and approval tests**

```powershell
mvn -Dtest=OrderContextTest,ApprovalTest test
```

Expected: all tests pass.

- [ ] **Step 5: Commit the publication fix**

```powershell
git add src/main/java/com/cex/core/order/OrderContext.java src/test/java/com/cex/core/order/OrderContextTest.java
git commit -m "fix: publish order status to async readers"
```

---

### Task 3: Replace the narrow chaos workload with seeded lifecycle chaos

**Files:**
- Modify: `src/test/java/com/cex/core/chaos/ChaosInvariantTest.java`

**Interfaces:**
- Consumes: corrected reconciliation from Task 1, `InvariantChecker`, and existing metrics.
- Produces: deterministic scenario generation, expected terminal-state validation, cancellation/conflict coverage, disturbance counters, and propagated worker failures.

- [ ] **Step 1: Define deterministic scenarios and expectations**

Add the scenario enum and pure selector inside `ChaosInvariantTest`:

```java
private enum Scenario {
    FILL_IN_ORDER(OrderStatus.FILLED),
    FILL_OUT_OF_ORDER(OrderStatus.FILLED),
    CANCEL_IN_ORDER(OrderStatus.CANCELED),
    CANCEL_OUT_OF_ORDER(OrderStatus.CANCELED),
    CONFLICT_BEFORE_CREATE(OrderStatus.FILLED);

    private final OrderStatus expectedStatus;

    Scenario(OrderStatus expectedStatus) {
        this.expectedStatus = expectedStatus;
    }
}

private static final int ORDERS = 300_000;
private static final Scenario[] SCENARIO_CYCLE = shuffledScenarioCycle();

private static Scenario[] shuffledScenarioCycle() {
    Scenario[] scenarios = Scenario.values().clone();
    java.util.SplittableRandom random = new java.util.SplittableRandom(CHAOS_SEED);
    for (int index = scenarios.length - 1; index > 0; index--) {
        int swapIndex = random.nextInt(index + 1);
        Scenario current = scenarios[index];
        scenarios[index] = scenarios[swapIndex];
        scenarios[swapIndex] = current;
    }
    return scenarios;
}

private static Scenario scenarioFor(long orderIndex) {
    return SCENARIO_CYCLE[(int) (orderIndex % SCENARIO_CYCLE.length)];
}
```

The seeded shuffle changes scenario order while guaranteeing an exactly balanced mix. With 300,000 orders, four scenario types contribute two transitions and cancel-before-create contributes one, yielding 540,000 expected transitions.

- [ ] **Step 2: Add an exact scenario dispatcher**

```java
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

private static OrderEvent event(long orderId, OrderEventType type) {
    return new OrderEvent(orderId, orderId, 1L, CHAOS_SEED + orderId, type);
}
```

- [ ] **Step 3: Replace implicit executor success with captured futures**

Keep the 16-worker start latch, but store every result and surface worker exceptions:

```java
List<Future<?>> futures = new ArrayList<>(WORKERS);
for (int worker = 0; worker < WORKERS; worker++) {
    final int workerIndex = worker;
    futures.add(workers.submit(() -> runWorker(
            workerIndex, start, engine, checker, invariantFailure,
            yieldInjections, parkInjections, interruptInjections)));
}
start.countDown();
workers.shutdown();
assertTrue(workers.awaitTermination(90L, TimeUnit.SECONDS),
        "worker termination timeout; seed=" + CHAOS_SEED);
for (Future<?> future : futures) {
    future.get();
}
```

Add imports for `ArrayList`, `List`, `Future`, `LongAdder`, and `SplittableRandom`.

- [ ] **Step 4: Implement the seeded worker disturbance function**

```java
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
                        Thread.currentThread().interrupt();
                        engine.process(event(orderId, OrderEventType.ORDER_CREATED));
                        Thread.interrupted();
                    }
                }
            }
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
```

- [ ] **Step 5: Add per-order convergence and workload-composition assertions**

After workers terminate and before printing the report, add:

```java
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
assertEquals(expectedFilled, engine.metrics().settleCount());
assertEquals(expectedCanceled, engine.metrics().unfreezeCount());
assertTrue(expectedFilled > 0L);
assertTrue(expectedCanceled > 0L);
assertTrue(engine.metrics().conflictingTerminalEvents() > 0L);
assertTrue(engine.metrics().stateTransitions() >= 500_000L);
assertTrue(yieldInjections.sum() > 0L);
assertTrue(parkInjections.sum() > 0L);
assertTrue(interruptInjections.sum() > 0L);
```

Retain the watchdog, final invariant check, deadlock check, and termination assertions. Extend console output with expected filled/canceled counts and the three injection counters.

- [ ] **Step 6: Run the rewritten chaos test with three explicit seeds**

```powershell
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=20260816 test
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=1 test
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=987654321 test
```

Expected for every seed: 16 workers terminate; settle and unfreeze are both nonzero; conflicts, duplicates, disorder, and every disturbance counter are nonzero; state transitions are at least 500,000; invariant failures are zero; deadlock check passes.

- [ ] **Step 7: Commit the chaos coverage**

```powershell
git add src/test/java/com/cex/core/chaos/ChaosInvariantTest.java
git commit -m "test: cover seeded cancellation and conflict chaos"
```

---

### Task 4: Measure representative state transitions separately from duplicate events

**Files:**
- Modify: `src/test/java/com/cex/core/performance/PerformanceTest.java`

**Interfaces:**
- Consumes: corrected order engine, existing `LatencyHistogram`, `GcMetrics`, and engine metrics.
- Produces: two explicitly named reports: duplicate idempotency hot path and representative lifecycle path.

- [ ] **Step 1: Rename the existing benchmark and its report**

Rename the test method:

```java
void duplicateFilledEventHotPathIsMeasuredSeparately()
```

Change its report title to:

```java
System.out.println("CEX DUPLICATE IDEMPOTENCY HOT-PATH REPORT");
```

Keep its existing 500,000 duplicate operations and assertions, but do not describe it as the sole core-state-machine result.

- [ ] **Step 2: Add constants and an executor helper for the lifecycle benchmark**

```java
private static final int LIFECYCLE_ORDERS = 300_000;
private static final int LIFECYCLE_USERS = 4_096;

private static OrderEvent lifecycleEvent(long orderId, long userId, OrderEventType type) {
    return new OrderEvent(orderId, userId, 1L, 1L, type);
}
```

- [ ] **Step 3: Add the representative lifecycle performance test**

Use four deterministic lifecycle variants: fill in order, cancel in order, fill before create, and cancel before create. Capture executor futures so failures cannot be swallowed.

```java
@Test
void representativeLifecycleMeetsLatencyThroughputAndMemoryTargets() throws Exception {
    AccountLedger ledger = new AccountLedger(new StripedLockManager());
    for (long userId = 1L; userId <= LIFECYCLE_USERS; userId++) {
        ledger.createAccount(userId, 1_000L);
    }
    ApprovalService approvals = new ApprovalService(1, 16);
    OrderEngine engine = new OrderEngine(
            ledger, new RiskPipeline(), new ManualClock(1L), approvals,
            event -> ApprovalDecision.PASS);
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);
    try {
        LatencyHistogram histogram = new LatencyHistogram();
        GcMetrics gcBefore = GcMetrics.snapshot();
        long transitionBefore = engine.metrics().stateTransitions();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(THREADS);
        long started = System.nanoTime();
        for (int thread = 0; thread < THREADS; thread++) {
            final int threadIndex = thread;
            futures.add(executor.submit(() -> {
                try {
                    start.await();
                    for (int index = threadIndex; index < LIFECYCLE_ORDERS; index += THREADS) {
                        long orderId = 1_000_000L + index;
                        long userId = (index & (LIFECYCLE_USERS - 1)) + 1L;
                        OrderEvent created = lifecycleEvent(orderId, userId, OrderEventType.ORDER_CREATED);
                        OrderEvent terminal = lifecycleEvent(orderId, userId,
                                (index & 1) == 0
                                        ? OrderEventType.MATCH_FILLED
                                        : OrderEventType.ORDER_CANCELLED);
                        OrderEvent first = (index & 2) == 0 ? created : terminal;
                        OrderEvent second = (index & 2) == 0 ? terminal : created;
                        long firstStarted = System.nanoTime();
                        engine.process(first);
                        histogram.record(System.nanoTime() - firstStarted);
                        long secondStarted = System.nanoTime();
                        engine.process(second);
                        histogram.record(System.nanoTime() - secondStarted);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }));
        }
        start.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(60L, TimeUnit.SECONDS));
        for (Future<?> future : futures) {
            future.get();
        }
        long elapsed = System.nanoTime() - started;
        GcMetrics gcAfter = GcMetrics.snapshot();
        long operations = histogram.count();
        long transitions = engine.metrics().stateTransitions() - transitionBefore;
        double tps = operations / (elapsed / 1_000_000_000.0);
        double averageMillis = histogram.averageMicros() / 1_000.0;

        long expectedTransitions = LIFECYCLE_ORDERS * 7L / 4L;
        assertEquals(LIFECYCLE_ORDERS * 2L, operations);
        assertEquals(expectedTransitions, transitions);
        assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().settleCount());
        assertEquals(LIFECYCLE_ORDERS / 2L, engine.metrics().unfreezeCount());
        assertTrue(tps >= 10_000.0, () -> "TPS=" + tps);
        assertTrue(averageMillis < 1.0, () -> "average latency ms=" + averageMillis);
        assertTrue(ledger.invariantHolds());

        System.out.println("====================================");
        System.out.println("CEX REPRESENTATIVE LIFECYCLE REPORT");
        System.out.println("====================================");
        System.out.println("Measurement Operations: " + operations);
        System.out.println("State Transitions:       " + transitions);
        System.out.println("Threads:                 " + THREADS);
        System.out.printf("TPS:                     %.2f%n", tps);
        System.out.printf("Average latency:         %.2f us%n", histogram.averageMicros());
        System.out.println("P50:                     " + histogram.p50Nanos() / 1_000.0 + " us");
        System.out.println("P95:                     " + histogram.p95Nanos() / 1_000.0 + " us");
        System.out.println("P99:                     " + histogram.p99Nanos() / 1_000.0 + " us");
        System.out.println("MAX:                     " + histogram.maxNanos() / 1_000.0 + " us");
        System.out.println("Freeze count:            " + engine.metrics().freezeCount());
        System.out.println("Settle count:            " + engine.metrics().settleCount());
        System.out.println("Unfreeze count:          " + engine.metrics().unfreezeCount());
        System.out.println("Heap Max:                " + Runtime.getRuntime().maxMemory() / (1024L * 1024L) + " MB");
        System.out.println("Heap Used:               " + (Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory()) / (1024L * 1024L) + " MB");
        System.out.println("GC Count:                " + (gcAfter.collectionCount() - gcBefore.collectionCount()));
        System.out.println("GC Time:                 " + (gcAfter.collectionTimeMillis()
                - gcBefore.collectionTimeMillis()) + " ms");
        System.out.println("Old/Full GC Count:       " + (gcAfter.oldCollectorCount()
                - gcBefore.oldCollectorCount()));
        System.out.println("Invariant result:        PASS");
        System.out.println("====================================");
    } finally {
        executor.shutdownNow();
        engine.close();
    }
}
```

Add imports for `ArrayList`, `List`, and `Future`.

- [ ] **Step 4: Run the performance class twice to check threshold stability**

```powershell
mvn -Dtest=PerformanceTest test
mvn -Dtest=PerformanceTest test
```

Expected on both runs: both tests pass; the representative report contains exactly 600,000 operations and 525,000 transitions, nonzero settle and unfreeze counts, max heap 256 MB, TPS at least 10,000, average latency below 1 ms, and invariant PASS.

- [ ] **Step 5: Commit the performance split**

```powershell
git add src/test/java/com/cex/core/performance/PerformanceTest.java
git commit -m "test: benchmark representative order lifecycles"
```

---

### Task 5: Run the full acceptance matrix and update README from evidence

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: final behavior and console output from Tasks 1-4.
- Produces: documentation whose state diagram, chaos description, benchmark labels, and recorded values match the verified implementation.

- [ ] **Step 1: Run focused correctness tests**

```powershell
$env:JAVA_HOME='C:/Users/10703/.jdks/ms-21.0.12'
$env:Path="$env:JAVA_HOME/bin;$env:Path"
mvn -Dtest=ApprovalTest,OrderContextTest,OutOfOrderStateMachineTest,TerminalConflictTest,RiskEngineTest test
```

Expected: zero failures and errors; held-fill rejection, held-fill approval, fill-before-create risk, metadata, terminal conflict, and visibility tests all pass.

- [ ] **Step 2: Run the final seeded chaos matrix**

```powershell
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=20260816 test
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=1 test
mvn -Dtest=ChaosInvariantTest -DCHAOS_SEED=987654321 test
```

Expected: all three runs meet every chaos acceptance condition from Task 3.

- [ ] **Step 3: Run a fresh full suite and save its console output as the documentation source**

```powershell
mvn clean test | Tee-Object -FilePath target/final-verification.log
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
```

Expected: `BUILD SUCCESS`, zero failures/errors, max heap 256 MB, representative TPS at least 10,000, average latency below 1 ms, no OOM, zero invariant failures, and deadlock/termination PASS.

- [ ] **Step 4: Update the state-machine documentation**

In README's state diagram and reconciliation list, replace the held path with these exact semantics:

```text
[RISK_HOLD] + MATCH_FILLED -> cache FILLED_SEEN; remain RISK_HOLD; do not settle
[RISK_HOLD] + FILLED_SEEN + APPROVAL_PASSED -> settle once -> [FILLED]
[RISK_HOLD] + FILLED_SEEN + APPROVAL_REJECTED -> unfreeze once -> [CANCELED]
```

State explicitly that initial risk evaluation occurs before a cached out-of-order fill can settle and that an accepted `NEW` order is not re-evaluated by duplicate messages.

- [ ] **Step 5: Update the chaos documentation**

Document these exact workload dimensions:

```text
Workers: 16 long-lived workers
Scenarios: in-order fill, out-of-order fill, in-order cancel, out-of-order cancel,
           fill/cancel conflict before create, and duplicate facts
Disturbance: seeded yield, 10-100 us park, and interrupt-status injection
Assertions: per-order terminal convergence, >=500,000 transitions, settle/unfreeze/conflict
            coverage, invariant failures=0, asset delta=0, no deadlock, termination PASS
```

- [ ] **Step 6: Update both benchmark sections using `target/final-verification.log`**

Create separate README tables titled:

```text
Duplicate Idempotency Hot-Path Benchmark
Representative Lifecycle Benchmark
```

For the representative table, copy the fresh output values for operations, state transitions, threads, TPS, average/P50/P95/P99/MAX latency, freeze/settle/unfreeze counts, heap max/used, GC count/time, old/full GC count, and invariant result. Do not reuse the pre-repair numbers already in README.

- [ ] **Step 7: Re-run the full suite after documentation edits**

```powershell
mvn clean test
git diff --check
git status --short
```

Expected: `BUILD SUCCESS`; zero failures/errors; `git diff --check` produces no output; only `README.md` is uncommitted at this task boundary.

- [ ] **Step 8: Commit the verified documentation**

```powershell
git add README.md
git commit -m "docs: record corrected chaos and lifecycle results"
```

- [ ] **Step 9: Perform the final repository verification**

```powershell
mvn clean test
git status --short
git log -5 --oneline
```

Expected: `BUILD SUCCESS`, all assessment acceptance metrics pass, and `git status --short` is empty.

## Plan Self-Review Result

- Spec coverage: every confirmed behavior, non-goal, test category, and acceptance threshold maps to Tasks 1-5.
- Scope: production changes are limited to reconciliation ordering and `volatile status`; all other changes are tests and README evidence.
- Type consistency: all snippets use existing public types and signatures; new helpers are test-local.
- Placeholder scan: the plan contains no deferred implementation markers; runtime metrics are copied from a named fresh log because their values are machine-dependent.
