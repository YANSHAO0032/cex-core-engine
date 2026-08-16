# Technical Assessment Scope A Repair Design

## Purpose

Bring the existing single-asset CEX assessment implementation into alignment with the assessment's risk-control, chaos-testing, memory, and performance requirements without adding counterparties, dual-asset settlement, an order book, persistence, or external frameworks.

## Confirmed Scope

This design covers four verified review findings:

1. A `RISK_HOLD` order can currently process `MATCH_FILLED` and settle before approval.
2. `OrderContext.status` is written under a user stripe lock but exposed through an unlocked, non-volatile read.
3. `ChaosInvariantTest` does not send `ORDER_CANCELLED` and does not provide seeded randomized event ordering or meaningful worker disturbance.
4. `PerformanceTest` reports throughput for duplicate events on already completed orders rather than a representative state-transition workload.

The current single-party settlement equation remains unchanged:

```text
sum(available + frozen) + systemSettledAmount = initialTotalAsset
```

## Non-Goals

- No buyer/seller counterparty model.
- No base/quote dual-asset ledger.
- No partial fills, trade identifiers, order book, matching priority, or fee account.
- No persistence, database, message broker, Spring component, or new runtime dependency.
- No unrelated package restructuring.

## Behavioral Design

### Risk-held fill handling

Observed facts remain monotonic and may arrive out of order. A fill received while an order is held is retained as `FILLED_SEEN`, but it is not allowed to apply `SETTLE_APPLIED` until approval has passed.

The reconciliation rules become:

```text
CREATED + HOLD                  -> RISK_HOLD, freeze once, schedule approval once
RISK_HOLD + FILLED              -> RISK_HOLD, retain FILLED_SEEN, no settlement
RISK_HOLD + FILLED + APPROVED   -> FILLED, settle once, record risk once
RISK_HOLD + FILLED + REJECTED   -> CANCELED, unfreeze once, never settle
RISK_HOLD + APPROVED            -> NEW
RISK_HOLD + REJECTED            -> CANCELED, unfreeze once
```

Rejection has priority over approval when both facts are present, matching the current approval-conflict policy. A terminal fill that was already settled before any risk hold remains terminal and is not rolled back.

The implementation should express the approval gate explicitly in `OrderEngine.reconcileLocked`. It must not clear or overwrite `FILLED_SEEN`; approval processing should trigger the existing repeatable reconciliation path.

### State publication

`OrderContext.status` becomes `volatile`. Writes remain serialized by the existing user stripe lock. This preserves the low-lock architecture while making asynchronous status observations safe without introducing a new query lock or changing public method signatures.

Effect bits and conflict markers remain lock-confined. They are not part of the supported unlocked query surface.

## Test Design

### Risk workflow regression tests

Add focused tests to `ApprovalTest` for these sequences:

1. Create a recent settled amount that triggers HOLD for a subsequent order.
2. Block the approval policy with latches so the order remains observably held.
3. Deliver `MATCH_FILLED` while held and assert status remains `RISK_HOLD`, settled funds do not increase, and funds remain frozen.
4. Release a rejecting approval and assert `CANCELED`, exactly one unfreeze, zero settlement for the held order, and the asset invariant holds.
5. Repeat with approving approval and assert the cached fill settles exactly once after approval.
6. Deliver duplicate fill and approval events and assert effect counters remain exactly once.

### Status visibility test

Add a bounded cross-thread test in `OrderContextTest` or `ApprovalTest` that observes an asynchronous status transition through the public `status()` getter. Synchronization controls should coordinate the writer's action but should not acquire the user stripe lock on the reading side. The test verifies the supported publication contract rather than depending on a probabilistic stale-read failure.

### Chaos workload

Refactor `ChaosInvariantTest` into a seeded, reproducible workload while retaining 16 long-lived workers and the invariant watchdog.

Use `SplittableRandom` instances derived from `CHAOS_SEED` and worker index. Generate deterministic scenario classes that include:

- create then fill;
- fill then create;
- create then cancel;
- cancel then create;
- duplicate create, fill, and cancel;
- a controlled minority of conflicting fill and cancel facts.

Each order must have a predetermined expected terminal outcome so final assertions can distinguish valid convergence from asset conservation alone. Conflict cases follow the documented first-successful-effect policy, with fill priority only when both facts are present before the first post-create reconciliation.

Worker disturbance uses seeded choices between `Thread.yield`, bounded `LockSupport.parkNanos` in the 10-100 microsecond range, and setting/clearing the current worker's interrupt status around engine calls. The test records injection counts and asserts every configured disturbance type was exercised. It must not interrupt latch waits or executor shutdown coordination.

Final chaos assertions include:

- at least 500,000 state transitions;
- both settle and unfreeze counts are greater than zero;
- out-of-order and duplicate counts are greater than zero;
- terminal conflicts are exercised and counted;
- every order reaches its predetermined terminal state;
- invariant failure count is zero during and after execution;
- total-asset delta is zero;
- no JVM deadlock is detected;
- all workers and the watchdog terminate within their deadlines.

### Performance workloads

Retain the current duplicate-fill workload but rename its test and report section to identify it as an idempotent duplicate hot-path benchmark. Its result must not be presented as the sole core state-machine throughput result.

Add a representative lifecycle benchmark using unique order IDs across a bounded reusable user set. The measured mix includes creation, in-order and out-of-order fills, and cancellations. It measures calls that produce freeze, settle, unfreeze, and state-transition effects.

The representative benchmark reports:

- measured operations and actual state transitions;
- thread count;
- TPS;
- average, P50, P95, P99, and maximum latency;
- freeze, settle, and unfreeze counts;
- heap maximum and used memory;
- total and old/full GC count and time;
- final asset-invariant result.

The assessment thresholds are applied to the representative workload: TPS at least 10,000 and average latency below 1 ms. The duplicate hot-path result remains informational.

## Files and Responsibilities

- `src/main/java/com/cex/core/order/OrderEngine.java`: enforce the approval gate during reconciliation.
- `src/main/java/com/cex/core/order/OrderContext.java`: safely publish status.
- `src/test/java/com/cex/core/risk/ApprovalTest.java`: reproduce and lock down held-fill approval semantics.
- `src/test/java/com/cex/core/order/OrderContextTest.java`: verify the status publication contract if the visibility test is kept at the context level.
- `src/test/java/com/cex/core/chaos/ChaosInvariantTest.java`: provide seeded cancellation, conflict, disorder, disturbance, and invariant coverage.
- `src/test/java/com/cex/core/performance/PerformanceTest.java`: separate duplicate hot-path and representative lifecycle measurements.
- `README.md`: document the corrected state transitions, chaos composition, benchmark definitions, and fresh measured results.

No new production class is required unless implementation reveals that deterministic chaos scenarios cannot remain readable inside the existing test class. If a helper is needed, it must be a package-private test-only type under `src/test/java/com/cex/core/chaos` and contain no production behavior.

## Verification Order

Implementation follows test-driven development in this order:

1. Add held-fill rejection test and prove it fails on current code.
2. Add held-fill approval test and prove the corrected gate passes both cases.
3. Add the publication test and make the minimal `volatile` change.
4. Upgrade chaos scenarios and run multiple explicit seeds.
5. Separate performance workloads and validate the representative thresholds.
6. Run the full Maven suite under the configured `-Xmx256m` test JVM.
7. Update README only from the final fresh output, then rerun the complete suite.

## Acceptance Criteria

- `mvn clean test` succeeds on JDK 21 with zero failures, errors, and skipped tests.
- Surefire continues to launch tests with `-Xms128m -Xmx256m -XX:+UseG1GC`.
- A held order cannot settle before `APPROVAL_PASSED`.
- A rejected held order always unfreezes once and cannot later settle from a cached or duplicate fill.
- An approved held order with a cached fill settles exactly once.
- Asynchronous callers can safely observe status transitions through `OrderContext.status()`.
- Chaos runs with 16 workers, cancellation traffic, seeded disorder, duplicate traffic, conflict traffic, and recorded disturbance injection.
- Chaos reaches at least 500,000 state transitions with zero invariant failures and no deadlock.
- The representative lifecycle workload reaches at least 10,000 TPS with average latency below 1 ms.
- The final run uses no more than a 256 MB maximum heap and reports no OOM.
- README diagrams, workload descriptions, and recorded metrics match the implemented behavior and the latest verification output.

