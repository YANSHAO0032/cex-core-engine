package com.cex.core.order;

public enum OrderEffect {
    FREEZE_APPLIED(1 << 0),
    SETTLE_APPLIED(1 << 1),
    UNFREEZE_APPLIED(1 << 2),
    RISK_RECORDED(1 << 3),
    APPROVAL_SCHEDULED(1 << 4);

    private final int mask;

    OrderEffect(int mask) {
        this.mask = mask;
    }

    public int mask() {
        return mask;
    }
}
