# PCR Span Control-Plane 设计文档

## 问题

1. PCR 启动后 CREATE TABLE 的新表数据复制不稳定——有时到达有时不到
2. Region split 时存在数据丢失窗口（split race）
3. 当前 region-based 订阅模型缺乏逻辑拓扑层

## 架构

```
┌──────────────────────────────────────────┐
│           Control Plane                   │
│      stream-ingest/src/span_ctl/          │
│                                           │
│  SpanRegistry {                           │
│      spans: Vec<ReplicationSpan>,         │
│  }                                        │
│                                           │
│  初始化: src TiDB info_schema → spans     │
│         → PD resolve regions → workers    │
│                                           │
│  运行时: tgt DDL job KV → new span        │
│         → PD resolve → new workers        │
│                                           │
│  Split/Merge 事件驱动:                    │
│    invalidate(span)                       │
│    → scan_delta(new_regions)              │
│    → open live stream                     │
│    → stop old region                      │
│                                           │
│  Periodic reconcile: 30s PD 全量对比      │
│    → safety net 兜底                      │
└──────────────┬───────────────────────────┘
               │ start/stop per-region workers
               ▼
┌──────────────────────────────────────────┐
│           Execution Plane                 │
│      (existing per-region pipeline)       │
│                                           │
│   observer → gRPC → batcher → SST ingest  │
└──────────────────────────────────────────┘
```

## 核心数据结构

```rust
struct ReplicationSpan {
    table_id: i64,
    start_key: Vec<u8>,    // t{table_id}_r_
    end_key: Vec<u8>,      // t{table_id}_s
    checkpoint_ts: TimeStamp,
    subscribed_regions: HashSet<u64>,
}
```

## 关键行为

### 新 Region 创建（Split 事件驱动）

```
Split 事件 → delegate.emit_pcr_split(split_key, new_region_ids)
  → gRPC PcrSplit 到 Consumer
    → Control Plane.invalidate(span)
      → 对每个 new_region:
          1. scan_delta(region, since=span.checkpoint_ts)  ← 补齐遗漏
          2. flush SST → RocksDB
          3. subscribe observer + open live gRPC stream
      → 旧 region: 标记 draining → delta scan 完成 → stop
      
  PD 不参与 split 路径——事件自带 new_region_ids
```

### 新表创建（DDL 监控驱动）

```
目标 RocksDB: mysql.tidb_ddl_job KV
  → 检测 type=create table, state=done
    → 提取 table_id
      → ReplicationSpan { table_id, start_key, end_key }
        → PD resolve regions → 创建 per-region workers
```

### Periodic Reconcile（Safety Net）

```
每 30s:
  PD query → 全量 region 列表
  ↔ 当前 SpanRegistry.subscribed_regions
  → add missing / remove stale
```

## Span 生命周期

```
Initialized → Subscribing → Active → Draining → Removed
    ↑           │             │
    │           │             ├── split: new children inherit checkpoint_ts
    │           │             └── merge: children merge into parent
    │           │
    │           └── DDL drop/truncate → remove span
    │
    └── periodic reconcile 发现遗漏
```

## 数据流时序

### Split 窗口保护

```
时间线:
  T0: span checkpoint_ts = 1000
  T1: region_42 split → [region_42, region_43]
  T2: split 事件到达 consumer
  T3: scan_delta(region_43, since=1000)  ← 补齐 T0~T3 的 commit
  T4: region_43 live stream 开始
  T5: region_42 stop

窗口 T0~T3 的数据通过 scan_delta 补齐 → 零丢失
```

## 改动范围

| 模块 | 新文件 | 改动 |
|------|--------|------|
| `stream-ingest/src/span_ctl/` | `mod.rs`, `registry.rs`, `resolver.rs` | 新增 ~300 行 |
| `stream-ingest/src/task.rs` | — | +30 行：Control Plane 集成 |
| `stream-ingest/src/subscriber.rs` | — | +20 行：span 感知 |

## 与现有实现的关系

- `delegate.rs`: `emit_pcr_split` 已存在，无需改
- `observer.rs`: `capture_change` 已修复，无需改
- `task.rs`: rediscover 降级为 safety net（保留，降低频率）
- `endpoint.rs`: `on_start_pcr_stream` 已修复，无需改
