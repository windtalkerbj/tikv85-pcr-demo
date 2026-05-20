# PCR Span-Native Architecture: PcrRegistry + Shared Sink

> Spec Version: 1.0 | Date: 2026-05-18 | Status: Approved

## 1. Problem Statement

当前 SpanBridge 通过 300s PD polling 发现 region split。Split 后 parent delegate 被 deregister（pcr_event_sink 随 delegate 消失），child delegate 创建时 `pcr_event_sink = None`。SpanBridge 需等到下一次拓扑刷新（≤300s）才知道子 region 存在，这期间的 DML 数据**静默丢失**。

**根因**：sink 属于 SpanBridge（per-region），delegate 属于 endpoint。split 后两者的生命周期断裂。

**影响**：not visibility lag, it's **hard data loss**.

## 2. Architecture Change

### Before

```
SpanBridge: per-region sub-bridge management
  → StartPcrStream(region_id, event_sink)
  → delegate.enable_pcr(event_sink)

Split: parent delegate lost → child delegate has no sink
  → wait 300s topo refresh
```

### After

```
SpanBridge: span-level scan + write pipeline
  → RegisterSpanSubscription(span=["",""), shared_sink)

Endpoint.PcrRegistry: holds span subscription + shared Arc<sink>
  → delegate created → auto-match span → delegate.enable_pcr(sink.clone())

Split: child delegate auto inherits shared sink clone
  → zero window
```

### Core Insight

**Sink 不应该属于 SpanBridge，而应该属于 endpoint runtime。** Subscription 是 span-level 的，region 只是 runtime sharding detail。Split/merge 应该对 subscription 完全透明。

## 3. Component Design

### 3.1 PcrRegistry (new, in endpoint.rs)

```rust
struct SpanSubscription {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    sink: Arc<futures::channel::mpsc::UnboundedSender<Vec<u8>>>,
    event_buffer_size: usize,
    event_flush_interval_ms: u64,
}

struct PcrRegistry {
    subscriptions: Vec<SpanSubscription>,
}

impl PcrRegistry {
    fn new() -> Self;
    fn register(&mut self, sub: SpanSubscription);
    fn match_region(&self, region_id: u64, start_key: &[u8], end_key: &[u8])
        -> Option<(Arc<...>, usize, u64)>;
    fn clear(&mut self);
}
```

`match_region` checks key range overlap between region `[r_start, r_end)` and span `[sub.start, sub.end)`. Empty keys mean unbounded.

### 3.2 Endpoint Changes

**New field:**
```rust
pub struct Endpoint {
    // ... existing ...
    pcr_registry: PcrRegistry,
}
```

**New Task variant (replaces StartPcrStream):**
```rust
Task::RegisterSpanSubscription {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    shared_sink: Arc<futures::channel::mpsc::UnboundedSender<Vec<u8>>>,
    event_buffer_size: usize,
    event_flush_interval_ms: u64,
}
```

**New method `on_register_span_subscription`:**
- Register span in PcrRegistry
- Retroactively match all existing delegates in `capture_regions`
- Call `delegate.enable_pcr(sink.clone(), ...)` for each match

**Delegate birth auto-match** (2 locations: `on_start_pcr_stream` line 762, `on_register` line 1055):
```rust
let (r_start, r_end) = get_region_key_range(&self.store_meta, region_id);
if let Some((sink, buf, flush)) = self.pcr_registry.match_region(
    region_id, &r_start, &r_end) {
    delegate.enable_pcr(sink, buf, flush);
}
```

**Helper:**
```rust
fn get_region_key_range(store_meta: &Arc<Mutex<StoreMeta>>, region_id: u64)
    -> (Vec<u8>, Vec<u8>)
```

### 3.3 SpanBridge Simplification

**Deleted:**
- `active: HashMap<u64, RegionMeta>` — no per-region tracking
- `RegionMeta` struct
- `spawn_sub_bridge()` method
- `run_sub_bridge()` per-region live CDC loop (~100 lines)
- Topo refresh per-region create/cancel logic
- `broken_rx` sink repair StartPcrStream resend

**Retained:**
- `resolve_regions_static()` — used once for initial scan, then periodic reconcile
- Writer task: `merge_rx` → gRPC sink
- Scan logic: per-region RocksDB scan (reused, not lifecycle-managed)

**New `run()` flow:**
1. Create merge channel (`merge_tx`, `merge_rx`)
2. Send `Task::RegisterSpanSubscription` to endpoint (once)
3. Initial full scan: iterate PD regions, scan RocksDB, send via merge_tx
4. Spawn writer task: `merge_rx.recv()` → `sink.send()`
5. Periodic reconciliation (30min, not 300s): re-scan all regions as fallback

### 3.4 pcr_service.rs Changes

`RegisterSpanSubscription` replaces per-region `StartPcrStream`. SpanBridge no longer spawns per-region sub-bridges.

## 4. Split Timeline (After Fix)

```
T0: delegate[24].pcr_event_sink = Some(shared_sender.clone())  ← Arc clone

T1: bulk INSERT → region split (<1s)

T2: CdcObserver::on_region_changed(Split)
    → Deregister::Delegate { region_id: 24 }
    → delegate[24] dropped (sender clone dropped, shared sender still alive)

T3: emit_pcr_split() → shared_sink  ← PcrSplit event still delivered

T4: child region 130/132 created by raftstore

T5: first write or TiCDC registration to child region
    → endpoint.on_register(region_id=130)
    → Delegate::new(130)
    → PcrRegistry.match_region(130, start_key, end_key) → HIT
    → delegate[130].enable_pcr(shared_sender.clone())
    ✅ auto-attached, zero window

T6: DML to region 130 → delegate.sink_data() → shared_sender.unbounded_send()
    → SpanBridge merge_rx → gRPC → Consumer
    ✅ no data loss
```

## 5. Compatibility

- `Task::StartPcrStream` kept as deprecated, forwarded to auto-match logic
- `enable_pcr()` interface unchanged
- `Delegate::new()` unchanged — pcr fields still initialized to None, filled by auto-match

## 6. Change Summary

| File | Add | Delete | Net |
|------|-----|--------|-----|
| `endpoint.rs` | +80 | -10 | +70 |
| `span_bridge.rs` | +40 | -220 | **-180** |
| `pcr_service.rs` | +15 | -30 | -15 |
| `lib.rs` | +1 | 0 | +1 |
| **Total** | **+136** | **-260** | **-124** |

## 7. Test Plan

### Unit Tests (pcr_registry)
| # | Test | Validates |
|---|------|-----------|
| T1 | Full span match | `["","")` matches any region |
| T2 | Partial span match | span subset matches in-range regions |
| T3 | Boundary non-match | region outside span returns None |
| T4 | Retroactive match | existing delegates get sink on register |
| T5 | Clear | no match after clear |

### Integration Tests
| # | Scenario | Success Criteria |
|---|----------|-----------------|
| I1 | INSERT 500 → split → DELETE | child delegate auto-sink, DELETE <5s |
| I2 | Bulk INSERT 1000 → auto split | COUNT/SUM match, no data loss |
| I3 | Pause+Resume | SpanBridge re-registers, delegate updates sink |
| I4 | StartPcrStream compat | old task variant still works |

## 8. Rollback Plan

Restore `active: HashMap<u64, RegionMeta>` and per-region topo refresh in SpanBridge. PcrRegistry left in endpoint but not used by old path.

## 9. What This Fixes

- ✅ Split data loss window: eliminated
- ✅ Topology polling: relegated to 30min reconciliation fallback only
- ✅ Repair-all storm: unnecessary (sink never lost on split)
- ✅ Per-region sub-bridge lifecycle: removed entirely
- ✅ Region churn under bulk DML: transparent to PCR
