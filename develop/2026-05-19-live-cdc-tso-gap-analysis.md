# Live CDC MVCC TSO Gap 分析

日期：2026-05-19

## 架构背景

PCR 复制的是 KV 字节流，目标端有两层：

```
层1: RocksDB（物理 KV）    ← PCR 直接写入，SST ingest
层2: TiDB（MVCC reader）   ← 通过快照读 RocksDB，快照 TS 来自 target PD TSO
```

两层之间无主动通知。TiDB 只在查询时用当前 target TSO 作为快照读 RocksDB。

## 问题机制

```
全量扫描阶段（prepare 时的数据）:
  commit_ts ≈ 466372400000000000（prepare 时的 source TSO）
  target TSO 启动后自然增长 → 几分钟后追上 → TiDB 可见 ✅

Live CDC 阶段（bench 时的数据）:
  commit_ts ≈ 466375500000000000（bench 时 source TSO 更高）
  target TSO 需继续增长才能追上 → 需要等待 ❌
```

## 时间线

```
T0: TPCC prepare → commit_ts ≈ 4663724xxxxxxxx
T1: 全量扫描完成 → target TSO 追上 T0 → 数据可见
T2: TPCC bench → commit_ts ≈ 4663755xxxxxxxx
T3: target TSO 仍在 T0~T1 附近 → bench 数据不可见
T4: target TSO 自己涨到 T2 → bench 数据可见
```

## 为什么小测试能收敛

INSERT 500 的 commit_ts 仅比全量扫描数据高几秒。target TSO 自然增长几秒即可追上。TPCC bench 的 commit_ts 比全量扫描高几分钟 → target TSO 需更长时间。

## Cockroach 为什么没这个问题

CRDB PCR 有 resolved_ts 机制：consumer 收到 checkpoint 后主动将 min_resolved_ts 推送给 target MVCC 层。TiDB 没有此接口。

## 当前 Demo 状态

| 层 | 状态 |
|----|------|
| source → consumer KV 数据流 | ✅ |
| consumer → target RocksDB ingest | ✅（并行 4 workers） |
| target RocksDB → TiDB 可见 | ❌ 依赖 target TSO 自然追赶 |
| schema bump（DDL discover） | ✅ 已修 |
| MVCC TSO bump | ❌ 未实现 |

## 解法方向

Consumer 每次 record checkpoint 后，用 min_resolved_ts 推进 target PD TSO。需写 PD etcd —— Demo 可行但不优雅。
