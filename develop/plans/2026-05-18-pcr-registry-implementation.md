# PcrRegistry Event-Driven SpanBridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate split data loss by moving PCR sink ownership from SpanBridge (per-region) to Endpoint.PcrRegistry (span-global shared sink with auto-match on delegate birth).

**Architecture:** Add PcrRegistry inside endpoint holding span subscriptions with shared `Arc<sink>`. On delegate creation, auto-match region key range against registry; if hit, `enable_pcr(sink.clone())`. SpanBridge simplified from per-region sub-bridge management to span-level scan + write pipeline + 30min reconciliation fallback.

**Tech Stack:** Rust, TiKV cdc crate, tokio, futures::channel::mpsc

**Files:**
- Create: `components/cdc/src/pcr_registry.rs` (new)
- Modify: `components/cdc/src/endpoint.rs` (Task enum + handler + auto-match)
- Modify: `components/cdc/src/span_bridge.rs` (simplify, remove per-region management)
- Modify: `components/cdc/src/pcr_service.rs` (RegisterSpanSubscription)
- Modify: `components/cdc/src/lib.rs` (add mod)

---

### Task 1: Create PcrRegistry module

**Files:**
- Create: `components/cdc/src/pcr_registry.rs`

- [ ] **Step 1: Write PcrRegistry with unit tests**

```rust
// components/cdc/src/pcr_registry.rs
// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::channel::mpsc;

/// A span-level PCR subscription. The shared sink is cloned to every
/// matching delegate — split/merge is transparent because the subscription
/// outlives any single region.
pub struct SpanSubscription {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub sink: Arc<mpsc::UnboundedSender<Vec<u8>>>,
    pub event_buffer_size: usize,
    pub event_flush_interval_ms: u64,
}

pub struct PcrRegistry {
    subscriptions: Vec<SpanSubscription>,
}

impl PcrRegistry {
    pub fn new() -> Self {
        Self { subscriptions: Vec::new() }
    }

    pub fn register(&mut self, sub: SpanSubscription) {
        self.subscriptions.push(sub);
    }

    /// Check if a region's key range overlaps any active span subscription.
    /// Returns the shared sink + PCR config if matched.
    pub fn match_region(
        &self,
        _region_id: u64,
        r_start: &[u8],
        r_end: &[u8],
    ) -> Option<(Arc<mpsc::UnboundedSender<Vec<u8>>>, usize, u64)> {
        for sub in &self.subscriptions {
            let left = sub.start_key.is_empty()
                || r_end.is_empty()
                || r_end.as_slice() > sub.start_key.as_slice();
            let right = sub.end_key.is_empty()
                || r_start.is_empty()
                || r_start.as_slice() < sub.end_key.as_slice();
            if left && right {
                return Some((
                    sub.sink.clone(),
                    sub.event_buffer_size,
                    sub.event_flush_interval_ms,
                ));
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.subscriptions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sink() -> Arc<mpsc::UnboundedSender<Vec<u8>>> {
        let (tx, _rx) = mpsc::unbounded();
        Arc::new(tx)
    }

    #[test]
    fn test_full_span_matches_any_region() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: vec![],
            end_key: vec![],
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        assert!(reg.match_region(1, b"abc", b"def").is_some());
        assert!(reg.match_region(2, &[], &[]).is_some());
        assert!(reg.match_region(3, b"t_100_", b"t_200_").is_some());
    }

    #[test]
    fn test_partial_span_match() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: b"t_100_".to_vec(),
            end_key: b"t_200_".to_vec(),
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        // region inside span
        assert!(reg.match_region(1, b"t_120_", b"t_150_").is_some());
        // region overlaps span start
        assert!(reg.match_region(2, b"t_050_", b"t_120_").is_some());
        // region outside span
        assert!(reg.match_region(3, b"t_200_", b"t_300_").is_none());
    }

    #[test]
    fn test_empty_registry_returns_none() {
        let reg = PcrRegistry::new();
        assert!(reg.match_region(1, b"abc", b"def").is_none());
    }

    #[test]
    fn test_clear() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: vec![],
            end_key: vec![],
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        assert!(reg.match_region(1, b"abc", b"def").is_some());
        reg.clear();
        assert!(reg.match_region(1, b"abc", b"def").is_none());
    }
}
```

- [ ] **Step 2: Run unit tests**

Run: `cargo test -p cdc -- pcr_registry`
Expected: 4 tests PASS

- [ ] **Step 3: Register module in lib.rs**

Read `components/cdc/src/lib.rs`, add `mod pcr_registry;` after `mod observer;`

- [ ] **Step 4: Verify compilation**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No errors

---

### Task 2: Add RegisterSpanSubscription to Task enum

**Files:**
- Modify: `components/cdc/src/endpoint.rs:221-230`

- [ ] **Step 1: Add Task variant**

After the `StartPcrStream` variant (line 230, before the closing `}`), add:

```rust
    /// Register a span-level PCR subscription. Replaces per-region
    /// StartPcrStream. SpanBridge calls this once for the full key range.
    /// All matching delegates (existing + future) auto-attach via PcrRegistry.
    RegisterSpanSubscription {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        shared_sink: Arc<futures::channel::mpsc::UnboundedSender<Vec<u8>>>,
        event_buffer_size: usize,
        event_flush_interval_ms: u64,
    },
```

Note: `Arc` is already imported at the top of endpoint.rs.

- [ ] **Step 2: Add Display/Debug entry**

Find the `Task::StartPcrStream` display impl (around line 314). Add below it:

```rust
            Task::RegisterSpanSubscription { ref start_key, ref end_key, .. } => de
                .field("type", &"register_span_subscription")
                .field("start_key", &hex::encode(start_key))
                .field("end_key", &hex::encode(end_key))
                .finish(),
```

If `hex` crate is not available, use `String::from_utf8_lossy` instead.

- [ ] **Step 3: Verify compilation**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No errors (may have "unused variant" warning, OK for now)

---

### Task 3: Add PcrRegistry to Endpoint and implement handler

**Files:**
- Modify: `components/cdc/src/endpoint.rs`

- [ ] **Step 1: Add import and field**

At top of endpoint.rs, near other crate imports:
```rust
use crate::pcr_registry::{PcrRegistry, SpanSubscription};
```

In the `Endpoint` struct (find `pub struct Endpoint`), add after existing fields:
```rust
    pcr_registry: PcrRegistry,
```

Find where Endpoint is constructed (search for `Endpoint {`). Add initialization:
```rust
    pcr_registry: PcrRegistry::new(),
```

- [ ] **Step 2: Add helper function `get_region_key_range`**

In the `impl Endpoint` block, add:

```rust
    fn get_region_key_range(&self, region_id: u64) -> (Vec<u8>, Vec<u8>) {
        self.store_meta
            .lock()
            .unwrap()
            .reader(region_id)
            .map(|r| (r.start_key().to_vec(), r.end_key().to_vec()))
            .unwrap_or_default()
    }
```

- [ ] **Step 3: Add `auto_match_pcr` helper**

```rust
    /// Auto-attach PCR sink to a delegate if its region overlaps a
    /// registered span subscription. Called at delegate birth.
    fn auto_match_pcr(&self, region_id: u64, delegate: &mut Delegate) {
        let (r_start, r_end) = self.get_region_key_range(region_id);
        if let Some((sink, buf_size, flush_ms)) =
            self.pcr_registry.match_region(region_id, &r_start, &r_end)
        {
            delegate.enable_pcr(sink, buf_size, flush_ms);
        }
    }
```

- [ ] **Step 4: Implement RegisterSpanSubscription handler**

In the `run` method's match block, find where `Task::StartPcrStream` is handled (near line 1469). Add BEFORE it:

```rust
            Task::RegisterSpanSubscription {
                start_key,
                end_key,
                shared_sink,
                event_buffer_size,
                event_flush_interval_ms,
            } => {
                let sub = SpanSubscription {
                    start_key,
                    end_key,
                    sink: shared_sink,
                    event_buffer_size,
                    event_flush_interval_ms,
                };
                // Retroactively match all existing delegates
                for (&region_id, delegate) in self.capture_regions.iter_mut() {
                    let (r_start, r_end) = self.get_region_key_range(region_id);
                    if self.pcr_registry.match_region(region_id, &r_start, &r_end).is_some() {
                        delegate.enable_pcr(
                            sub.sink.clone(),
                            sub.event_buffer_size,
                            sub.event_flush_interval_ms,
                        );
                    }
                }
                self.pcr_registry.register(sub);
                info!("PCR: span subscription registered";
                    "start_key" => ?String::from_utf8_lossy(&self.pcr_registry.subscriptions.last().map(|s| &*s.start_key).unwrap_or(b"")),
                    "matched_delegates" => self.capture_regions.len(),
                );
            }
```

- [ ] **Step 5: Add auto_match_pcr calls at delegate birth (2 locations)**

**Location 1:** In `on_start_pcr_stream` (line ~762), after `e.insert(delegate)`:

```rust
                self.auto_match_pcr(region_id, e.into_mut());
```

Wait — need to adjust because the variable is consumed by `e.insert()`. Read the actual code to place correctly. The call should be after `delegate` is accessible as `&mut Delegate`. In the Occupied branch, it's `e.into_mut()`. In the Vacant branch, after `e.insert(delegate)`, it's `e.get_mut().unwrap()`.

Better: add the call AFTER the entire match block, on the `delegate` reference:

```rust
        // After the match block (both Occupied and Vacant set up delegate)
        // PCR auto-match: if a span subscription covers this region, attach sink
        self.auto_match_pcr(region_id, delegate);
```

This single line goes right before the `capture_change` block (before line 788's `let observe_handle = delegate.handle.fresh_handle();`).

**Location 2:** In `on_register` (line ~1066), after the match block (after `e.insert(delegate)`), before `let observe_id = delegate.handle.id;`:

```rust
        self.auto_match_pcr(region_id, delegate);
```

- [ ] **Step 6: Deprecate StartPcrStream handler**

In the `Task::StartPcrStream` handler (line ~1469), change to forward to auto-match:

```rust
            Task::StartPcrStream {
                region_id,
                event_sink,
                event_buffer_size,
                event_flush_interval_ms,
                ..
            } => {
                // DEPRECATED: SpanBridge now uses RegisterSpanSubscription.
                // Forward to auto-match for backward compat.
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    let sink = Arc::new(event_sink);
                    delegate.enable_pcr(sink, event_buffer_size, event_flush_interval_ms);
                }
                // sink is Arc'd and held by delegate; if no delegate, it's dropped
            }
```

- [ ] **Step 7: Verify compilation**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No errors

---

### Task 4: Simplify SpanBridge

**Files:**
- Modify: `components/cdc/src/span_bridge.rs`

This is the largest change. We remove per-region sub-bridge management and replace with span-global scan + writer.

- [ ] **Step 1: Remove RegionMeta struct and active field**

Delete the `RegionMeta` struct (lines ~22-26) and the `active: HashMap<u64, RegionMeta>` field from `SpanBridge` (line ~36).

```rust
// REMOVE:
// struct RegionMeta { ... }
// In SpanBridge struct: active: HashMap<u64, RegionMeta>,
```

- [ ] **Step 2: Remove spawn_sub_bridge method**

Delete the entire `spawn_sub_bridge` method (lines ~67-100). It creates per-region event_sink + sub-bridge task — no longer needed.

- [ ] **Step 3: Rewrite SpanBridge::run()**

Replace the entire `run()` method. The new version:

```rust
    pub async fn run(
        &mut self,
        scheduler: Scheduler<Task>,
        source_engine: Option<Arc<engine_rocks::RocksEngine>>,
        event_buffer_size: usize,
        event_flush_interval_ms: u64,
        start_ts: u64,
        bridge_rt: Arc<tokio::runtime::Runtime>,
        mut sink: ServerStreamingSink<PcrEvent>,
    ) {
        // Merge channel: all scan data and (future) live CDC data flows here.
        let (merge_tx, mut merge_rx) =
            tokio::sync::mpsc::unbounded_channel::<PcrEvent>();

        // Register span subscription with endpoint. This gives every
        // existing + future delegate a clone of the shared sender.
        let shared_sink = Arc::new(
            futures::channel::mpsc::UnboundedSender::from(merge_tx.clone())
        );
        let task = Task::RegisterSpanSubscription {
            start_key: self.span_start.clone(),
            end_key: self.span_end.clone(),
            shared_sink: shared_sink.clone(),
            event_buffer_size,
            event_flush_interval_ms,
        };
        if let Err(e) = scheduler.schedule(task) {
            error!("PCR span_bridge: failed to register span subscription"; "error" => ?e);
            return;
        }

        // Initial full scan: resolve regions once, scan each, send via merge_tx.
        let regions = self.resolve_regions();
        if let Some(ref engine) = source_engine {
            for &(region_id, _, _, _, _) in &regions {
                let engine = engine.clone();
                let tx = merge_tx.clone();
                let rts = self.pcr_resolved_ts.clone();
                let is_full = start_ts == 0;
                let scan_start_ts = start_ts;
                bridge_rt.spawn(async move {
                    Self::scan_region(region_id, is_full, scan_start_ts, engine, tx, rts).await;
                });
            }
        }

        // Writer task: forward merge_rx to gRPC sink.
        let writer_rt = bridge_rt.clone();
        writer_rt.spawn(async move {
            while let Some(event) = merge_rx.recv().await {
                if sink.send((event, WriteFlags::default())).await.is_err() {
                    warn!("PCR span_bridge: gRPC sink error");
                    break;
                }
            }
        });

        // Periodic reconciliation: re-scan all regions as fallback (30min).
        let mut reconcile = tokio::time::interval(
            std::time::Duration::from_secs(1800),
        );
        loop {
            reconcile.tick().await;
            let regions = self.resolve_regions();
            if let Some(ref engine) = source_engine {
                for &(region_id, _, _, _, _) in &regions {
                    let engine = engine.clone();
                    let tx = merge_tx.clone();
                    let rts2 = self.pcr_resolved_ts.clone();
                    let scan_start_ts = start_ts;
                    let is_full_scan = start_ts == 0;
                    bridge_rt.spawn(async move {
                        Self::scan_region(region_id, is_full_scan, scan_start_ts, engine, tx, rts2).await;
                    });
                }
            }
            info!("PCR span_bridge: periodic reconciliation"; "regions" => regions.len());
        }
    }
```

- [ ] **Step 4: Extract scan_region static method from run_sub_bridge**

Take the scan logic from `run_sub_bridge` (lines 301-345) and make it a standalone static method. Keep `run_sub_bridge` for backward reference but mark deprecated.

```rust
    /// Scan one region's RocksDB and send results via out_tx.
    /// Used for initial scan and periodic reconciliation.
    async fn scan_region(
        region_id: u64,
        is_full: bool,
        start_ts: u64,
        source_engine: Arc<engine_rocks::RocksEngine>,
        out_tx: tokio::sync::mpsc::UnboundedSender<PcrEvent>,
        _pcr_resolved_ts: Arc<AtomicU64>,
    ) {
        // (key, value, op, cf) tuples for chunked batch emission.
        let mut kvs: Vec<(Vec<u8>, Vec<u8>, OpType, &str)> = Vec::new();

        if is_full {
            let engine = source_engine.clone();
            let (scan_default, scan_write) = tokio::task::spawn_blocking(move || {
                let empty: &[u8] = &[];
                (
                    crate::pcr_snapshot::scan_default_cf(&engine, empty, empty, usize::MAX),
                    crate::pcr_snapshot::scan_write_cf_raw(&engine, empty, empty, usize::MAX),
                )
            }).await.unwrap_or_default();
            for (k, v) in &scan_default { kvs.push((k.clone(), v.clone(), OpType::Put, "default")); }
            for (k, v) in &scan_write { kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
        } else {
            let engine = source_engine.clone();
            let (wr, dr) = tokio::task::spawn_blocking(move || {
                let empty: &[u8] = &[];
                crate::pcr_snapshot::scan_delta_entries(&engine, start_ts, empty, empty)
            }).await.unwrap_or_default();
            for (k, v) in &wr { kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
            for (k, v, op) in &dr { kvs.push((k.clone(), v.clone(), *op, "default")); }
        }

        let total = kvs.len();
        if total > 0 {
            for chunk in kvs.chunks(100) {
                let mut batch = PcrKvBatch::new();
                for (k, v, op, cf) in chunk {
                    let mut kv = PcrKv::new();
                    kv.set_key(k.clone()); kv.set_value(v.clone());
                    kv.set_op(*op); kv.set_cf(cf.to_string());
                    batch.mut_kvs().push(kv);
                }
                let mut event = PcrEvent::new();
                event.set_kv_batch(batch);
                event.set_is_snapshot(true);
                if out_tx.send(event).is_err() { return; }
            }
        }
        info!("PCR span_bridge: scan complete"; "region_id" => region_id, "kvs" => total);
    }
```

- [ ] **Step 5: Remove unused imports and dead code**

Remove:
- `use std::collections::HashMap;` (no longer needed)
- `use futures::{SinkExt, StreamExt};` (may still be needed by writer)
- `RegionMeta` struct definition
- `spawn_sub_bridge` method (already removed)

- [ ] **Step 6: Verify compilation**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No errors. Fix any unused import warnings.

---

### Task 5: Update pcr_service.rs

**Files:**
- Modify: `components/cdc/src/pcr_service.rs`

- [ ] **Step 1: Update SpanBridge::run() call**

Find where `SpanBridge::run()` is called. Remove per-region parameters that are no longer needed. The new signature is:

```rust
// Before (old):
// let mut bridge = SpanBridge::new(span_start, span_end, source_pd, pcr_resolved_ts);
// bridge.run(scheduler, source_engine, event_buffer_size, event_flush_interval_ms,
//            start_ts, bridge_rt, sink).await;

// After (new):
let mut bridge = SpanBridge::new(span_start, span_end, source_pd, pcr_resolved_ts);
bridge.run(scheduler, source_engine, event_buffer_size, event_flush_interval_ms,
           start_ts, bridge_rt, sink).await;
// Signature is same, but run() no longer manages per-region sub-bridges
```

The SpanBridge::new() and run() signatures remain compatible — the internal implementation changes but the caller doesn't.

- [ ] **Step 2: Verify compilation**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No errors

---

### Task 6: Full build + integration test

**Files:** None (test only)

- [ ] **Step 1: Build tikv-server**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo build -p tikv-server`
Expected: Build successful

- [ ] **Step 2: Clean restart cluster**

```bash
cp /Users/cjn/Documents/OpenSource/dig_tidb/tikv85/target/debug/tikv-server /tmp/tikv-server-pcr
pkill -9 tikv-server; pkill -9 pd-server; pkill -9 tidb-server; sleep 2
rm -rf /tmp/pcr-src-data /tmp/pcr-tgt-data /tmp/pcr-src-pd /tmp/pcr-tgt-pd
# ... restart PDs, TiKVs, TiDBs
```

- [ ] **Step 3: Run split regression test**

```sql
CREATE DATABASE pcr; USE pcr;
CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(50));
INSERT INTO t (v) VALUES ('r1'),('r2'),('r3'),('r4'),('r5');
-- start PCR, wait full scan
-- INSERT 500 → verify convergence
-- DELETE 100 → verify <5s convergence
```

- [ ] **Step 4: Run bulk INSERT 1000 with auto-split test**

```sql
INSERT INTO t (v) VALUES ... -- 1000 rows to trigger split
-- wait 30s → verify COUNT/SUM match on source and target
-- UPDATE → verify
-- DELETE 200 → verify <5s convergence
```

- [ ] **Step 5: Verify no data loss**

Source and target COUNT and SUM(LENGTH(v)) must be identical after each phase.

---

### Task 7: Cleanup and commit

- [ ] **Step 1: Remove unused run_sub_bridge if only referenced by old code**

If `run_sub_bridge` is no longer called anywhere, delete it. Check:
```bash
grep -rn "run_sub_bridge" components/cdc/src/
```

- [ ] **Step 2: Final cargo check**

Run: `export CMAKE_POLICY_VERSION_MINIMUM=3.5 && cargo check -p cdc`
Expected: No warnings, no errors

- [ ] **Step 3: Run unit tests**

Run: `cargo test -p cdc -- pcr_registry`
Expected: All 4 tests PASS
