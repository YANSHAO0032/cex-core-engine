package com.cex.core.engine.ledger;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 账本资金守恒和并发测试工具。
 *
 * <p>验证冻结、解冻、成交借贷记操作及同一用户 16 线程并发更新后的资产不变量。</p>
 */
class LedgerServiceTest {

    /** 验证基本资金操作始终满足 available + frozen + traded 守恒。 */
    @Test
    void keepsTheConservationInvariantAcrossOperations() {
        LedgerService ledger = new LedgerService(16);
        ledger.openAccount(1L, 1_000L);

        assertTrue(ledger.freeze(1L, 400L));
        assertTrue(ledger.unfreeze(1L, 100L));
        assertTrue(ledger.tradeDebit(1L, 200L));
        assertTrue(ledger.tradeCredit(1L, 300L));

        LedgerBalance balance = ledger.snapshot(1L);
        assertEquals(1_000L, balance.getAvailable() + balance.getFrozen()
                + balance.getTraded());
        assertTrue(balance.isConserved());
    }

    /** 验证同一用户的并发资金变更被分片锁正确串行化。 */
    @Test
    void serializesConcurrentChangesForTheSameUser() throws Exception {
        LedgerService ledger = new LedgerService(16);
        long userId = 99L;
        int threadCount = 16;
        int iterations = 1_000;
        ledger.openAccount(userId, (long) threadCount * iterations);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                tasks.add(() -> {
                    for (int j = 0; j < iterations; j++) {
                        if (!ledger.freeze(userId, 1L)
                                || !ledger.tradeDebit(userId, 1L)
                                || !ledger.tradeCredit(userId, 1L)) {
                            return false;
                        }
                    }
                    return true;
                });
            }

            for (Future<Boolean> result : executor.invokeAll(tasks)) {
                assertTrue(result.get());
            }
        } finally {
            executor.shutdownNow();
        }

        LedgerBalance balance = ledger.snapshot(userId);
        assertEquals(threadCount * (long) iterations, balance.getAvailable());
        assertEquals(0L, balance.getFrozen());
        assertEquals(0L, balance.getTraded());
        assertTrue(balance.isConserved());
    }
}
