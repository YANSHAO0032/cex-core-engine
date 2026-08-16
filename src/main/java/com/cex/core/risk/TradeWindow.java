package com.cex.core.risk;

import com.cex.core.util.MoneyMath;

public final class TradeWindow {
    private final long windowMillis;
    private long[] timestamps;
    private long[] amounts;
    private int head;
    private int size;
    private long rollingSum;

    public TradeWindow(long windowMillis) {
        if (windowMillis <= 0L) {
            throw new IllegalArgumentException("windowMillis must be positive");
        }
        this.windowMillis = windowMillis;
        this.timestamps = new long[16];
        this.amounts = new long[16];
    }

    public void record(long timestampMillis, long amount) {
        MoneyMath.requirePositive(amount);
        evict(timestampMillis);
        ensureCapacity(size + 1);
        int tail = (head + size) % timestamps.length;
        timestamps[tail] = timestampMillis;
        amounts[tail] = amount;
        size++;
        rollingSum = MoneyMath.checkedAdd(rollingSum, amount);
    }

    public long currentSum(long nowMillis) {
        evict(nowMillis);
        return rollingSum;
    }

    public int size() {
        return size;
    }

    private void evict(long nowMillis) {
        long cutoff = nowMillis - windowMillis;
        while (size > 0 && timestamps[head] < cutoff) {
            rollingSum = MoneyMath.checkedSubtract(rollingSum, amounts[head]);
            timestamps[head] = 0L;
            amounts[head] = 0L;
            head = (head + 1) % timestamps.length;
            size--;
        }
    }

    private void ensureCapacity(int required) {
        if (required <= timestamps.length) {
            return;
        }
        int next = timestamps.length << 1;
        while (next < required) {
            next <<= 1;
        }
        long[] newTimestamps = new long[next];
        long[] newAmounts = new long[next];
        for (int i = 0; i < size; i++) {
            int index = (head + i) % timestamps.length;
            newTimestamps[i] = timestamps[index];
            newAmounts[i] = amounts[index];
        }
        timestamps = newTimestamps;
        amounts = newAmounts;
        head = 0;
    }
}
