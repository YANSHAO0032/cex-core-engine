package com.cex.core.util;

/**
 * 面向整数最小货币单位的安全算术与金额校验工具。
 *
 * <p>能力：执行溢出检测的加减运算，并约束金额为正数或非负数。</p>
 * <p>线程安全：仅包含无状态静态方法，可由任意线程并发调用。</p>
 * <p>限制：不处理小数精度、币种换算或舍入规则，调用方须统一金额单位。</p>
 */
public final class MoneyMath {

    /** 阻止工具类被实例化。 */
    private MoneyMath() {
    }

    /**
     * 相加两个金额并在结果溢出时失败。
     *
     * @param left 左操作数
     * @param right 右操作数
     * @return 精确相加结果
     * @throws ArithmeticException 当相加结果超出 {@code long} 范围时抛出
     */
    public static long checkedAdd(long left, long right) {
        return Math.addExact(left, right);
    }

    /**
     * 相减两个金额并在结果溢出时失败。
     *
     * @param left 被减数
     * @param right 减数
     * @return 精确相减结果
     * @throws ArithmeticException 当相减结果超出 {@code long} 范围时抛出
     */
    public static long checkedSubtract(long left, long right) {
        return Math.subtractExact(left, right);
    }

    /**
     * 校验金额为严格正数。
     *
     * @param amount 待校验金额
     * @return 原金额
     * @throws IllegalArgumentException 当金额不为正数时抛出
     */
    public static long requirePositive(long amount) {
        if (amount <= 0L) {
            throw new IllegalArgumentException("amount must be positive");
        }
        return amount;
    }

    /**
     * 校验金额为非负数。
     *
     * @param amount 待校验金额
     * @return 原金额
     * @throws IllegalArgumentException 当金额为负数时抛出
     */
    public static long requireNonNegative(long amount) {
        if (amount < 0L) {
            throw new IllegalArgumentException("amount must be nonnegative");
        }
        return amount;
    }
}
