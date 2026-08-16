package com.cex.core.order;

import java.util.concurrent.atomic.LongAdder;

public final class OrderEngineMetrics {
    private final LongAdder processedEvents = new LongAdder();
    private final LongAdder acceptedFacts = new LongAdder();
    private final LongAdder duplicateEvents = new LongAdder();
    private final LongAdder outOfOrderEvents = new LongAdder();
    private final LongAdder stateTransitions = new LongAdder();
    private final LongAdder freezeCount = new LongAdder();
    private final LongAdder settleCount = new LongAdder();
    private final LongAdder unfreezeCount = new LongAdder();
    private final LongAdder riskRecordedCount = new LongAdder();
    private final LongAdder riskHoldCount = new LongAdder();
    private final LongAdder approvalScheduledCount = new LongAdder();
    private final LongAdder approvalPassCount = new LongAdder();
    private final LongAdder approvalRejectCount = new LongAdder();
    private final LongAdder conflictingTerminalEvents = new LongAdder();
    private final LongAdder approvalConflictEvents = new LongAdder();
    private final LongAdder metadataConflictEvents = new LongAdder();

    public void processedEvent() { processedEvents.increment(); }
    public void acceptedFact() { acceptedFacts.increment(); }
    public void duplicateEvent() { duplicateEvents.increment(); }
    public void outOfOrderEvent() { outOfOrderEvents.increment(); }
    public void stateTransition() { stateTransitions.increment(); }
    public void freeze() { freezeCount.increment(); }
    public void settle() { settleCount.increment(); }
    public void unfreeze() { unfreezeCount.increment(); }
    public void riskRecorded() { riskRecordedCount.increment(); }
    public void riskHold() { riskHoldCount.increment(); }
    public void approvalScheduled() { approvalScheduledCount.increment(); }
    public void approvalPass() { approvalPassCount.increment(); }
    public void approvalReject() { approvalRejectCount.increment(); }
    public void conflictingTerminalEvent() { conflictingTerminalEvents.increment(); }
    public void approvalConflict() { approvalConflictEvents.increment(); }
    public void metadataConflict() { metadataConflictEvents.increment(); }

    public long processedEvents() { return processedEvents.sum(); }
    public long acceptedFacts() { return acceptedFacts.sum(); }
    public long duplicateEvents() { return duplicateEvents.sum(); }
    public long outOfOrderEvents() { return outOfOrderEvents.sum(); }
    public long stateTransitions() { return stateTransitions.sum(); }
    public long freezeCount() { return freezeCount.sum(); }
    public long settleCount() { return settleCount.sum(); }
    public long unfreezeCount() { return unfreezeCount.sum(); }
    public long riskRecordedCount() { return riskRecordedCount.sum(); }
    public long riskHoldCount() { return riskHoldCount.sum(); }
    public long approvalScheduledCount() { return approvalScheduledCount.sum(); }
    public long approvalPassCount() { return approvalPassCount.sum(); }
    public long approvalRejectCount() { return approvalRejectCount.sum(); }
    public long conflictingTerminalEvents() { return conflictingTerminalEvents.sum(); }
    public long approvalConflictEvents() { return approvalConflictEvents.sum(); }
    public long metadataConflictEvents() { return metadataConflictEvents.sum(); }
}
