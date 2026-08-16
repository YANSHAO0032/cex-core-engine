package com.cex.core.risk;

import java.util.Arrays;
import java.util.Objects;

public final class RiskPipeline {
    private volatile RiskRule[] rules;

    public RiskPipeline() {
        this.rules = new RiskRule[0];
    }

    public RiskPipeline(RiskRule... initialRules) {
        replaceRules(initialRules);
    }

    public RiskDecision evaluate(RiskContext context) {
        Objects.requireNonNull(context, "context");
        for (RiskRule rule : rules) {
            if (rule.evaluate(context) == RiskDecision.HOLD) {
                return RiskDecision.HOLD;
            }
        }
        return RiskDecision.PASS;
    }

    public synchronized void registerRule(RiskRule rule) {
        Objects.requireNonNull(rule, "rule");
        RiskRule[] copy = Arrays.copyOf(rules, rules.length + 1);
        copy[copy.length - 1] = rule;
        rules = copy;
    }

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

    public synchronized void replaceRules(RiskRule... replacement) {
        Objects.requireNonNull(replacement, "replacement");
        RiskRule[] copy = replacement.clone();
        for (RiskRule rule : copy) {
            Objects.requireNonNull(rule, "replacement contains null");
        }
        rules = copy;
    }

    public int ruleCount() {
        return rules.length;
    }
}
