# SpanBridge Event-Driven 架构重构设计

日期：2026-05-18

## 核心诊断

当前 SpanBridge 的根本问题不是 "300s 太长"，而是 **topology source 选错了**。

```
当前：SpanBridge → PD polling → 推测 delegate 生命周期
正确：SpanBridge ← endpoint/observer → 知道 delegate 生死的真正源头
```

PD 只能看到 region metadata，看不到 delegate rebuild、pcr sink state、observer lifecycle。PD polling 天生滞后，不管缩短到多少秒，split <1s 的窗口永远存在。

## 根因链

```
bulk INSERT → region split (<1s)
  → old delegate deregister (pcr_event_sink 随 delegate 一起消失)
  → new delegate created (pcr_event_sink = None)
  → Raft cmd 继续 apply，但无人 consume PCR event
  → 数据永久丢失（非 visibility lag，是 hard data loss）
```

CDC observer 本身不是 WAL，event 不缓冲。delegate 无 sink 期间的数据没有任何地方暂存。

## 正确架构

### P0：split event → child region immediate attach sink

```
split happened
  ↓
emit RegionSplitEvent { parent, left, right }
  ↓
SpanBridge apply delta（不刷新全量 topology）
  ↓
spawn child bridges immediately
```

**不要通知 "refresh"，通知 "topology delta"**。全量 refresh 和 delta apply 有本质区别。

### P1：topology delta event 替代 full refresh

```rust
enum RegionLifecycleEvent {
    Split   { parent, left, right },
    Merge   { from, into },
    Destroy { region_id },
}
```

PD polling 降级为 periodic reconciliation（如 30min 一次），不是主路径。

### P2：PCR sink registration intrinsic to delegate

```
现在：delegate create → later StartPcrStream → enable_pcr()
应改：delegate create → auto inherit PCR registration
```

sink 应该属于 delegate 的内在状态，不是 SpanBridge 的外部状态。

### P3：下沉到 observer registry

```
subscribe_span(["", ""))
  ↓
CDC observer registry 保存 span subscription
  ↓
delegate 创建时 auto match span → auto attach PCR sink
  ↓
split 时 child delegate auto rematch → auto attach
```

这才是 Cockroach PCR 的核心思想：span subscription 长期存在，split/merge 只是 internal ownership transfer，对 subscription 完全透明。

## 最终目标架构

```
subscribe_span()
  → observer registry
    → delegate auto bind

split:
  parent deregister
  → child delegate create
  → registry auto rematch
  → auto attach sink

结果:
  无 topo ticker
  无 refresh window
  无 split data loss
  无 repair all
  无 polling storm
```

这才是真正的 span-native PCR architecture。
