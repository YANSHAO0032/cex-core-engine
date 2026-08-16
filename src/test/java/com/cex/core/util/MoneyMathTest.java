package com.cex.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MoneyMathTest {

    @Test
    void checkedAddUsesExactArithmetic() {
        assertEquals(15L, MoneyMath.checkedAdd(10L, 5L));
        assertThrows(ArithmeticException.class, () -> MoneyMath.checkedAdd(Long.MAX_VALUE, 1L));
    }

    @Test
    void checkedSubtractUsesExactArithmetic() {
        assertEquals(7L, MoneyMath.checkedSubtract(10L, 3L));
        assertThrows(ArithmeticException.class, () -> MoneyMath.checkedSubtract(Long.MIN_VALUE, 1L));
    }

    @Test
    void requirePositiveRejectsZeroAndNegativeValues() {
        assertEquals(9L, MoneyMath.requirePositive(9L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requirePositive(0L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requirePositive(-1L));
    }

    @Test
    void requireNonNegativeRejectsNegativeValues() {
        assertEquals(0L, MoneyMath.requireNonNegative(0L));
        assertEquals(9L, MoneyMath.requireNonNegative(9L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requireNonNegative(-1L));
    }
}
