package com.cex.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * 验证金额工具的精确算术与金额范围校验。
 *
 * <p>线程安全：被测工具无状态，可安全并发调用。</p>
 * <p>限制：仅覆盖 {@code long} 整数金额，不涉及币种与舍入规则。</p>
 */
class MoneyMathTest {
    /** 创建资金算术测试实例。 */
    MoneyMathTest() {
    }


    /** 场景：精确加法返回正确结果，并在上溢时失败。 */
    @Test
    void checkedAddUsesExactArithmetic() {
        assertEquals(15L, MoneyMath.checkedAdd(10L, 5L));
        assertThrows(ArithmeticException.class, () -> MoneyMath.checkedAdd(Long.MAX_VALUE, 1L));
    }

    /** 场景：精确减法返回正确结果，并在下溢时失败。 */
    @Test
    void checkedSubtractUsesExactArithmetic() {
        assertEquals(7L, MoneyMath.checkedSubtract(10L, 3L));
        assertThrows(ArithmeticException.class, () -> MoneyMath.checkedSubtract(Long.MIN_VALUE, 1L));
    }

    /** 场景：正数金额校验接受正数并拒绝零和负数。 */
    @Test
    void requirePositiveRejectsZeroAndNegativeValues() {
        assertEquals(9L, MoneyMath.requirePositive(9L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requirePositive(0L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requirePositive(-1L));
    }

    /** 场景：非负金额校验接受零与正数并拒绝负数。 */
    @Test
    void requireNonNegativeRejectsNegativeValues() {
        assertEquals(0L, MoneyMath.requireNonNegative(0L));
        assertEquals(9L, MoneyMath.requireNonNegative(9L));
        assertThrows(IllegalArgumentException.class, () -> MoneyMath.requireNonNegative(-1L));
    }
}
