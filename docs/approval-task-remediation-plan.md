# ApprovalTask 功能缺失补齐计划

## 1. 背景

测评文档要求动态风控链路在用户滑动窗口成交金额超过阈值后，将后续订单置为 `RISK_HOLD`，并触发异步 `ApprovalTask`。审批通过时订单放行并执行资金扣减；审批拒绝时自动撤单并解冻资金。

当前工程已经完成：

- `RiskEngine` 基于滑动窗口识别超阈值交易。
- `OrderStateMachine` 支持 `RISK_HOLD` 状态。
- `LedgerService` 支持冻结、解冻和成交扣减，并维护资产守恒。

当前缺口：

- 没有显式的 `ApprovalTask` 领域模型。
- 没有异步审批任务队列和 worker。
- `RISK_HOLD` 缺少审批通过后的放行事件。
- 风控、订单状态机和账本之间缺少审批通过/拒绝的闭环测试。

## 2. 补齐目标

1. 增加纯内存 `ApprovalTask` 工作流，不依赖外部流程引擎或中间件。
2. 超阈值后创建审批任务，并由异步 worker 模拟人工或高级风控审批。
3. 审批通过：
   - 从冻结金额执行成交扣减。
   - 发布订单放行事件，使订单从 `RISK_HOLD` 回到挂起前的活跃状态。
4. 审批拒绝：
   - 解冻订单冻结金额。
   - 发布撤单事件，使订单进入 `CANCELLED`。
5. 所有审批任务按 `taskId` 幂等提交，重复提交不会重复扣减或解冻。

## 3. 状态与事件设计

新增订单事件：

- `RISK_RELEASED`：审批通过后的风控放行事件。

状态转移：

```text
CREATED / PARTIAL_FILLED
        |
        | RISK_HOLD
        v
    RISK_HOLD
      /    \
     /      \
RISK_RELEASED  ORDER_CANCELLED
   |              |
   v              v
挂起前活跃态      CANCELLED
```

说明：

- `RISK_HOLD` 进入时记录挂起前状态。
- 审批通过后恢复到挂起前状态，例如 `CREATED` 或 `PARTIAL_FILLED`。
- 审批拒绝复用 `ORDER_CANCELLED`，避免引入重复撤单语义。
- `RISK_HOLD` 期间到达的成交事件继续被忽略并记录幂等键，不在放行后补执行。

## 4. 并发与幂等设计

- `ApprovalTaskService` 使用 JDK 内存队列承载待审批任务。
- 单 worker 异步消费审批任务，避免审批流程与交易热路径抢锁。
- `ConcurrentHashMap<Long, ApprovalTask>` 作为任务幂等索引。
- `submit` 使用 `putIfAbsent(taskId)`，同一任务只会进入队列一次。
- 订单状态仍由 `OrderStateMachine` 的 `orderId` 分片锁保护。
- 资金变更仍由 `LedgerService` 的 `userId` 分片锁保护。

## 5. 失败策略

- 审批策略返回空结果时，任务标记为 `FAILED`。
- 审批通过但冻结余额不足时，任务标记为 `FAILED`，订单保持 `RISK_HOLD`。
- 审批拒绝但解冻失败时，任务标记为 `FAILED`，订单保持 `RISK_HOLD`。
- worker 异常会记录到 `workerFailure`，便于测试和监控发现。

## 6. 验收用例

新增测试覆盖：

- 审批通过：`RISK_HOLD -> RISK_RELEASED -> CREATED`，冻结金额转入 `traded`。
- 审批拒绝：`RISK_HOLD -> ORDER_CANCELLED -> CANCELLED`，冻结金额回到 `available`。
- 重复 `taskId`：只处理一次，不重复扣减或解冻。
- 订单状态机单测：`RISK_RELEASED` 只能释放 `RISK_HOLD` 订单，且恢复挂起前状态。

验收命令：

```powershell
$env:MAVEN_OPTS='-Xmx256m'
mvn -q test
Remove-Item Env:MAVEN_OPTS
```

## 7. 实施结果

已按本计划完成实现：

- 新增 `ApprovalTask`、`ApprovalTaskStatus`、`ApprovalDecision` 和 `ApprovalDecisionProvider`，表达审批任务、任务生命周期和审批结果。
- 新增 `ApprovalTaskService`，使用 `ConcurrentHashMap` 做 `taskId` 幂等索引，使用单 worker 消费内存队列。
- 新增 `RISK_RELEASED` 订单事件，审批通过后将订单从 `RISK_HOLD` 恢复到挂起前活跃状态。
- `RiskEngine` 新增 `recordTradeAndSubmitApproval`，在超阈值时同时投递 `RISK_HOLD` 并提交审批任务。
- 新增 `ApprovalTaskServiceTest` 覆盖审批通过、审批拒绝和重复任务幂等。
- 扩展 `OrderStateMachineTest` 覆盖 `RISK_RELEASED` 的有效释放和非挂起忽略逻辑。

最近一次验收结果：

```text
mvn -q '-DexcludedGroups=chaos,benchmark' test
通过

MAVEN_OPTS=-Xmx256m mvn -q test
通过
Benchmark After: TPS=2481144.54, avg/event=0.40 us, GC collections=0
Chaos: operations=21686993, TPS=361390.20, avgLatency=0.15 us, P99=0.70 us
```
