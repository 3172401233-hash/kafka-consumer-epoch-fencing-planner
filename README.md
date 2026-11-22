# kafka-consumer-epoch-fencing-planner

规划消费者 epoch、分区接管和重复提交防护。

> **注意：本工具只生成状态决策。** 它不连接真实 broker、不发送任何
> 请求，只对输入的事件序列确定性地判定"允许 / 拒绝"，并输出最终
> 状态快照。所有结论均可离线复现。

## 环境

- Node.js >= 24.16.0（仅使用标准库，无第三方依赖）

## 功能

`src/state-machine.js` 导出 `OwnershipStateMachine`，消费以下事件：

| 事件 | 方法 | 说明 |
| --- | --- | --- |
| 加入 / 重新加入 | `join({ memberId, generation, partitions })` | 同 generation 的重复加入幂等 |
| 分区撤销 | `revoke({ memberId, generation, partitions })` | 晚到的撤销不会剥夺新持有者的所有权 |
| 开启事务 | `beginTransaction({ memberId, generation, partition, txnId, producerEpoch })` | 更高 epoch 会 fence 旧事务 |
| 提交偏移 | `commit({ memberId, generation, partition, offset, txnId?, producerEpoch? })` | 逐条判定是否允许 |
| 成员崩溃 | `crash({ memberId })` | 释放分区并中止未决事务 |

## Fencing 规则

- `stale-generation`：携带旧 generation 的提交一律被拒绝。
- `not-owner`：非当前分区持有者的提交被拒绝。
- `epoch-regression`：producer epoch 回退时提交被拒绝，事务中止。
- `late-revocation`：所有权已在更高 generation 转移后到达的撤销事件被忽略。

## 输出

`report()` 返回确定性快照：

- `holders`：每个分区的当前持有者
- `illegalCommits`：所有非法提交及拒绝原因
- `abortedTransactions`：所有被中止的事务及原因
- `takeoverOrder`：分区接管 / 释放的完整顺序
- `ignoredRevocations`：被忽略的晚到撤销事件

## 测试

```sh
node --test
```

覆盖场景：重复加入、撤销晚到、epoch 回退、事务提交竞争、成员崩溃，
以及旧 generation fencing 与接管顺序链。
