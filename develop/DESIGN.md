# PCR Demo Architecture

> 版本：2026-05-26 | 与 `CLAUDE.md` PCR ARCHITECTURE RULES 一致

## 1. 顶层架构

```
源端 TiKV (Producer)                     目标端 TiKV (Consumer)
┌──────────────────────┐                 ┌──────────────────────┐
│  SpanBridge          │  gRPC stream    │  StreamIngestTask    │
│  ┌────────────────┐  │  PcrEvent       │  ┌────────────────┐  │
│  │ full scan      │──┼────────────────┼─→│ SST Batcher    │  │
│  │ relay task     │  │                 │  │ Direct Ingest  │  │
│  │ checkpoint     │  │                 │  │ Schema Sync    │  │
│  │ rediscovery    │  │                 │  │ TSO Bumper     │  │
│  └────────────────┘  │                 │  └────────────────┘  │
│                      │                 │                      │
│  CDC Observer ──→    │                 │  HTTP Control API    │
│  Delegate.batcher ──→│                 │  /pcr/status         │
│  shared_sink ────────→│                 │  /pcr/create         │
│                      │                 │  /pcr/pause          │
│  PcrRegistry         │                 │  /pcr/resume         │
│  auto_match_pcr      │                 │  /pcr/cutover        │
└──────────────────────┘                 └──────────────────────┘
```

## 2. 数据流（两条路径）

### 2.1 全量扫描路径
```
SpanBridge::resolve_regions()
  → scan_region() → pcr_snapshot::stream_cf_full (O(1MB) batch)
  → PcrKvBatch protobuf → merge_tx → gRPC sink → consumer
  → SstBatcher::add_kv → flush → DirectIngest::ingest_sst → target RocksDB
```

### 2.2 Live CDC 路径
```
CDC Observer::on_flush_applied_cmd_batch
  → Delegate::on_batch → sink_data
  → LogicalMutation::from_write_ref (WriteRef → Put/Delete)
  → PcrEventBatcher::add_kv → force_flush
  → emit_pcr_event → shared_sink.unbounded_send
  → fut_tx → relay task (batch 64 events / 1ms)
  → merge_tx → gRPC writer → consumer
  → StreamIngestTask::handle_event → SstBatcher → DirectIngest → target RocksDB
```

### 2.3 关键区别
| | 全量扫描 | Live CDC |
|------|------|------|
| 触发 | SpanBridge 启动 + rediscovery | CDC observer |
| 数据来源 | RocksDB iterator | Raft apply cmd batch |
| 到 merge_tx 的方式 | scan 直接 send | 经 shared_sink → relay |
| 绕过 | relay（直线 merge_tx） | — |
| 内存 | O(1MB) streaming | PcrEventBatcher byte threshold |

## 3. 模块依赖图

```
cdc/observer ────→ cdc/delegate ────→ cdc/logical_mutation
                       │                    ↓
                       │              cdc/pcr_event_batcher
                       │                    ↓
                       │              cdc/channel (unbounded_send)
                       │                    ↓
                  cdc/old_value       fut_tx → relay → merge_tx
                       │                    ↓
                  cdc/initializer     cdc/span_bridge ──→ cdc/pcr_snapshot
                       │                    │
                       ↓                    ↓
              cdc/endpoint ←── cdc/pcr_registry
                  (auto_match_pcr)
                       │
                  cdc/pcr_service (gRPC Subscribe)
                       │
                       ↓ (gRPC stream)
              ┌──────────────────────────┐
              │ stream-ingest/subscriber │
              │ stream-ingest/task       │
              │ stream-ingest/sst_batcher│
              │ stream-ingest/direct_ingest │
              │ stream-ingest/schema_sync│
              │ stream-ingest/checkpoint │
              │ stream-ingest/http_control│
              └──────────────────────────┘
```

## 4. PcrRegistry 生命周期

```
SpanBridge 启动
  → Task::RegisterSpanSubscription(shared_sink)
  → Endpoint: PcrRegistry.register(span_sub)
  → 对每个已有的 delegate: delegate.enable_pcr(shared_sink.clone())
  → 之后新 delegate 出生: auto_match_pcr → enable_pcr

SpanBridge rediscovery (30s)
  → 新 shared_sink + 新 relay
  → RegisterSpanSubscription 重注册
  → Endpoint: PcrRegistry.clear() → 注册新 sub
  → 所有 delegate 换新 sink

Split 后 child delegate
  → endpoint 创建 child delegate
  → auto_match_pcr(child_delegate, pcr_registry)
  → match_region(child_start, child_end)
  → enable_pcr(shared_sink.clone())
  → 零窗口继承
```

## 5. Relay 架构

```
delegate 1 ─┐
delegate 2 ─┤  shared_sink (Arc<UnboundedSender>)
delegate N ─┘       │
                    ↓
              fut_rx (relay task 读取)
                    │
              batch 64 events / 1ms
                    │
              merge_tx.send(batch)
                    │ (bounded, 32 slots)
              merge_rx
                    │
              gRPC writer → consumer

Observability: Arc<AtomicU64> 计数器在 SpanBridge::run() 范围，
                relay instance 重生不丢失计数
```

## 6. Key Fixes（5 TiKV + 3 TiDB）

### TiKV 侧
| # | Fix | File | What |
|---|-----|------|------|
| 1 | Relay observability | span_bridge.rs | Arc<AtomicU64> counters |
| 2 | TSO bumper 1s | task.rs | 500K batch, <2s visibility |
| 3 | WriteRef fallback DEFAULT CF | delegate.rs | Both CFs with correct TS suffix |
| 4 | Full scan cross-CF synthesis | span_bridge.rs | Synthesize from short_value + old_value_cb |
| 5 | PD schema version sync | schema_sync.rs | Read source PD → write target PD |

### TiDB 侧
| # | Fix | File | What |
|---|-----|------|------|
| 1 | Online FullLoad trigger | domain.go | 0 diffs → force FullLoad |
| 2 | PCR read-only bootstrap | main.go | DefaultNotFound → createReadOnlyDomain |
| 3 | Same TiDB version | — | v8.5.6 source + target |

## 7. Demo Limitations

1. **ALTER TABLE / CREATE INDEX 在线不可见** — TiDB ApplyDiff 需要完整 DDL job 历史（mysql.tidb_ddl_job），PCR 不复制这些。FullLoad（重启）绕过此限制。
2. **TiDB 重启不可靠** — Source RocksDB 自身 WriteRef → DEFAULT CF 引用已断裂（compaction GC）。PCR 无法重构不存在的数据。TiDB 通过 short_value 路径工作，PCR 不复制此逻辑。
3. **目标 TiDB 需 read-only 模式** — `PCR_READ_ONLY=1` 跳过 BootstrapSession 写入。

## 8. 已知边缘风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| short_value=None + old_value_cb 失败 | DEFAULT CF 静默丢失 | 低概率，系统表 JSON >255B 但 WriteRef 通常有 short_value |
| merge channel backpressure | 全量扫阻塞 | 32 slot bounded channel，gRPC writer 慢时自动节流 |
| relay instance 30s 重生 | 事件窗口丢失 | shared_sink Arc 不丢，新 relay 从 fut_rx 继续读 |
| multi-version meta key | 目标端缺历史版本 | TiDB FullLoad 扫全 KV 绕过版本链 |
