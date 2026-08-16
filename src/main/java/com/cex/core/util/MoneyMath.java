package com.cex.core.util;

public final class MoneyMath {

    private MoneyMath() {
    }

    public static long checkedAdd(long left, long right) {
        return Math.addExact(left, right);
    }

    public static long checkedSubtract(long left, long right) {
        return Math.subtractExact(left, right);
    }

    public static long requirePositive(long amount) {
        if (amount <= 0L) {
            throw new IllegalArgumentException("amount must be positive");
        }
        return amount;
    }

    public static long requireNonNegative(long amount) {
        if (amount < 0L) {
            throw new IllegalArgumentException("amount must be nonnegative");
        }
        return amount;
    }
}
