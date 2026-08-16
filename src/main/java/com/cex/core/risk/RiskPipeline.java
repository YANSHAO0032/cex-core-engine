package com.cex.core.risk;

import java.util.Arrays;
import java.util.Objects;

/**
 * 按顺序执行的一组可热替换风险规则。
 * 核心能力：短路拦截高风险订单并动态维护规则集；线程安全：读操作通过 volatile 快照无锁执行，写操作同步 copy-on-write；使用限制：规则不得为 {@code null}。
 */
public final class RiskPipeline {
    /** 供读线程遍历的 volatile 规则快照，写入端以 copy-on-write 原子替换。 */
    private volatile RiskRule[] rules;

    /**
     * 创建不含规则的风控管道。
     */
    public RiskPipeline() {
        this.rules = new RiskRule[0];
    }

    /** 创建包含初始规则的风控管道。
     * @param initialRules 初始规则集合，不能为 {@code null} 且不能包含 {@code null}
     * @throws NullPointerException 当规则数组或其元素为 {@code null} 时抛出
     */
    public RiskPipeline(RiskRule... initialRules) {
        replaceRules(initialRules);
    }

    /**
     * 依次评估规则，任一规则要求挂起即短路返回。
     * @param context 当前订单风险评估快照
     * @return 首次命中挂起时返回 {@link RiskDecision#HOLD}，否则返回 {@link RiskDecision#PASS}
     * @throws NullPointerException 当上下文为 {@code null} 时抛出
     * @note 读取 volatile 快照后仅遍历数组，避免评估路径被规则更新锁阻塞。
     */
    public RiskDecision evaluate(RiskContext context) {
        Objects.requireNonNull(context, "context");
        for (RiskRule rule : rules) {
            // 命中高风险即停止后续规则，保持规则优先级和低延迟。
            if (rule.evaluate(context) == RiskDecision.HOLD) {
                return RiskDecision.HOLD;
            }
        }
        return RiskDecision.PASS;
    }

    /**
     * 在管道末尾注册规则。
     * @param rule 要追加的非空风险规则
     * @throws NullPointerException 当规则为 {@code null} 时抛出
     * @note 同步写入端复制数组后再赋给 volatile 字段，读者始终看到完整快照。
     */
    public synchronized void registerRule(RiskRule rule) {
        Objects.requireNonNull(rule, "rule");
        RiskRule[] copy = Arrays.copyOf(rules, rules.length + 1);
        copy[copy.length - 1] = rule;
        rules = copy;
    }

    /**
     * 移除首个与给定规则相等的规则。
     * @param rule 要移除的非空规则
     * @return 找到并移除时为 {@code true}，否则为 {@code false}
     * @throws NullPointerException 当规则为 {@code null} 时抛出
     * @note 采用 copy-on-write 更新，保证并发评估不会观察到半修改数组。
     */
    public synchronized boolean removeRule(RiskRule rule) {
        Objects.requireNonNull(rule, "rule");
        int index = -1;
        for (int i = 0; i < rules.length; i++) {
            if (rules[i].equals(rule)) {
                index = i;
                break;
            }
        }
        if (index < 0) {
            return false;
        }
        RiskRule[] copy = new RiskRule[rules.length - 1];
        System.arraycopy(rules, 0, copy, 0, index);
        System.arraycopy(rules, index + 1, copy, index, copy.length - index);
        rules = copy;
        return true;
    }

    /**
     * 以给定规则集合整体替换现有管道。
     * @param replacement 新规则集合，不能为 {@code null} 且不能包含 {@code null}
     * @throws NullPointerException 当数组或其元素为 {@code null} 时抛出
     * @note 克隆调用方数组，避免外部后续改写破坏 volatile 快照的不可变约定。
     */
    public synchronized void replaceRules(RiskRule... replacement) {
        Objects.requireNonNull(replacement, "replacement");
        RiskRule[] copy = replacement.clone();
        for (RiskRule rule : copy) {
            Objects.requireNonNull(rule, "replacement contains null");
        }
        rules = copy;
    }

    /** 获取当前快照中的规则数量。
     * @return 当前规则数
     */
    public int ruleCount() {
        return rules.length;
    }
}
