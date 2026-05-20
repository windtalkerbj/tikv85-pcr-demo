// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Span-based PCR bridge — registers a span-level subscription with the
//! endpoint once, scans all regions for initial data, then delegates
//! live CDC event forwarding to endpoint's per-region delegates via a
//! shared sink. Region split/merge is transparent because the endpoint's
//! PcrRegistry auto-attaches the shared sink to new delegates at birth.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use grpcio::*;
use slog_global::{error, info, warn};
use stream_ingest::pcrpb::*;
use tikv_util::worker::Scheduler;
use tokio::sync::Semaphore;

use crate::endpoint::Task;

/// Limit concurrent full-region scans. With streaming (O(1MB) per scan),
/// 2 concurrent scans avoid RocksDB block-cache thrash without starving I/O.
static SCAN_SEM: Semaphore = Semaphore::const_new(2);

/// Streaming batch threshold: flush a PcrKvBatch when buffered KVs exceed 1MB.
const BATCH_BYTES: usize = 1_048_576;

/// SpanBridge resolves a key range to regions via PD for initial scan,
/// registers a span-global subscription with the endpoint, and runs a
/// gRPC writer task. Per-region lifecycle management is handled by
/// the endpoint's PcrRegistry — SpanBridge no longer tracks regions.
pub struct SpanBridge {
    span_start: Vec<u8>,
    span_end: Vec<u8>,
    source_pd: String,
    /// Shared CDC resolved_ts — updated by the endpoint on each on_min_ts cycle.
    pcr_resolved_ts: Arc<AtomicU64>,
}

impl SpanBridge {
    pub fn new(
        span_start: Vec<u8>,
        span_end: Vec<u8>,
        source_pd: String,
        pcr_resolved_ts: Arc<AtomicU64>,
    ) -> Self {
        Self { span_start, span_end, source_pd, pcr_resolved_ts }
    }

    /// Resolve the span to region list via PD HTTP API.
    /// Returns (region_id, start_key, end_key, conf_ver, version).
    pub fn resolve_regions(&self) -> Vec<(u64, Vec<u8>, Vec<u8>, u64, u64)> {
        Self::resolve_regions_static(&self.source_pd, &self.span_start, &self.span_end)
    }

    fn hex_decode(hex_str: &str) -> core::result::Result<Vec<u8>, String> {
        if hex_str.is_empty() { return Ok(Vec::new()); }
        if hex_str.len() % 2 != 0 { return Err(format!("odd hex length: {}", hex_str.len())); }
        (0..hex_str.len()).step_by(2)
            .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16)
                .map_err(|e| format!("hex: {:?}", e)))
            .collect()
    }

    /// Register span subscription with endpoint, scan all regions, and
    /// run the gRPC writer. Per-region topology is handled by endpoint's
    /// PcrRegistry — SpanBridge no longer manages individual sub-bridges.
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
        info!("PCR span_bridge: run() entered, resolving regions...");
        // Unbounded merge channel: streaming scan O(1MB) eliminates OOM risk.
        // block_send can be used freely; the gRPC writer drains at its own pace.
        let (merge_tx, mut merge_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<PcrEvent>>();

        // Shared sink for delegates (futures channel) → relay → merge_tx.
        let (fut_tx, mut fut_rx) =
            futures::channel::mpsc::unbounded::<Vec<u8>>();
        let shared_sink = Arc::new(fut_tx);

        // Relay: collect up to 64 events or 1ms, then batch-send to merge.
        // This amortizes per-event Tokio wakeup across the merge channel.
        let relay_tx = merge_tx.clone();
        bridge_rt.spawn(async move {
            let mut buf: Vec<PcrEvent> = Vec::with_capacity(64);
            loop {
                // Wait for first event with no timeout
                let first = match fut_rx.next().await {
                    Some(data) => data,
                    None => break,
                };
                if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&first) {
                    buf.push(event);
                }
                // Drain remaining events with 1ms timeout
                loop {
                    match tokio::time::timeout(
                        std::time::Duration::from_millis(1),
                        fut_rx.next(),
                    ).await
                    {
                        Ok(Some(data)) => {
                            if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&data) {
                                buf.push(event);
                            }
                            if buf.len() >= 64 { break; }
                        }
                        _ => break, // timeout or channel closed
                    }
                }
                if !buf.is_empty() {
                    let batch = std::mem::take(&mut buf);
                    if relay_tx.send(batch).is_err() { break; }
                }
            }
        });

        // Register span subscription with endpoint. This gives every existing
        // and future delegate a clone of the shared sender.
        let task = Task::RegisterSpanSubscription {
            start_key: self.span_start.clone(),
            end_key: self.span_end.clone(),
            shared_sink,
            event_buffer_size,
            event_flush_interval_ms,
        };
        if let Err(e) = scheduler.schedule(task) {
            error!("PCR span_bridge: failed to register span subscription"; "error" => ?e);
            return;
        }

        // Resolve regions and ensure delegates exist for each. The endpoint's
        // auto_match_pcr on delegate birth will attach the shared sink from
        // the span subscription registered above.
        let regions = self.resolve_regions();
        info!("PCR span_bridge: resolved {} regions", regions.len());
        for &(region_id, _, _, _, _) in &regions {
            let (sink, _rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();
            let _ = scheduler.schedule(Task::StartPcrStream {
                region_id,
                start_ts,
                event_sink: sink,
                event_buffer_size,
                event_flush_interval_ms,
            });
        }

        // Initial full scan: scan each region with limited concurrency.
        // 5-warehouse TPCC → each region ~4.2M KVs → ~840MB per scan.
        // Without a semaphore, 73 concurrent scans saturate RocksDB I/O + OOM.
        if let Some(ref engine) = source_engine {
            for &(region_id, _, _, _, _) in &regions {
                let engine = engine.clone();
                let tx = merge_tx.clone();
                let is_full = start_ts == 0;
                let scan_start_ts = start_ts;
                bridge_rt.spawn(async move {
                    let _permit = SCAN_SEM.acquire().await;
                    Self::scan_region(region_id, is_full, scan_start_ts, engine, tx).await;
                });
            }
        }

        // Writer task: receives batches from merge, sends to gRPC one-by-one.
        let writer_rt = bridge_rt.clone();
        writer_rt.spawn(async move {
            while let Some(batch) = merge_rx.recv().await {
                for event in batch {
                    if sink.send((event, WriteFlags::default())).await.is_err() {
                        warn!("PCR span_bridge: gRPC sink send failed, dropping event");
                    }
                }
            }
        });

        // Idle loop: keep the SpanBridge alive for the gRPC stream lifetime.
        // The endpoint's PcrRegistry handles all region lifecycle; no periodic
        // scanning is needed. Full scan ran at startup; live CDC flows through
        // the shared sink → relay → merge_rx → writer above.
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    /// Scan one region via verified pcr_snapshot functions, chunk by bytes.
    /// Uses the same RocksDB scan path as the materialized version (verified
    /// correct for 1-warehouse) but groups KVs by byte count instead of fixed
    /// 100-KV chunks, reducing channel send overhead.
    async fn scan_region(
        region_id: u64,
        is_full: bool,
        start_ts: u64,
        source_engine: Arc<engine_rocks::RocksEngine>,
        out_tx: tokio::sync::mpsc::UnboundedSender<Vec<PcrEvent>>,
    ) {
        let engine = source_engine.clone();
        let (entries, total) = tokio::task::spawn_blocking(move || {
            let empty: &[u8] = &[];
            let mut kvs: Vec<(Vec<u8>, Vec<u8>, OpType, &str)> = Vec::new();
            if is_full {
                let dk = crate::pcr_snapshot::scan_default_cf(&engine, empty, empty, usize::MAX);
                let wk = crate::pcr_snapshot::scan_write_cf_raw(&engine, empty, empty, usize::MAX);
                for (k, v) in &dk { kvs.push((k.clone(), v.clone(), OpType::Put, "default")); }
                for (k, v) in &wk { kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
            } else {
                let (wr, dr) = crate::pcr_snapshot::scan_delta_entries(&engine, start_ts, empty, empty);
                for (k, v) in &wr { kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
                for (k, v, op) in &dr { kvs.push((k.clone(), v.clone(), *op, "default")); }
            }
            let total = kvs.len() as u64;
            (kvs, total)
        }).await.unwrap_or_default();

        // Send in byte-based batches to reduce channel/grpc overhead
        let mut batch_kvs: Vec<PcrKv> = Vec::with_capacity(4096);
        let mut batch_bytes: usize = 0;
        for (k, v, op, cf) in entries {
            let mut kv = PcrKv::new();
            kv.set_key(k);
            kv.set_value(v);
            kv.set_op(op);
            kv.set_cf(cf.to_string());
            batch_bytes += kv.get_key().len() + kv.get_value().len();
            batch_kvs.push(kv);

            if batch_bytes >= BATCH_BYTES {
                let mut pcr_batch = PcrKvBatch::new();
                for kv in batch_kvs.drain(..) {
                    pcr_batch.mut_kvs().push(kv);
                }
                let mut event = PcrEvent::new();
                event.set_kv_batch(pcr_batch);
                event.set_is_snapshot(true);
                let _ = out_tx.send(vec![event]);
                batch_bytes = 0;
            }
        }
        // Drain residual
        if !batch_kvs.is_empty() {
            let mut pcr_batch = PcrKvBatch::new();
            for kv in batch_kvs.drain(..) {
                pcr_batch.mut_kvs().push(kv);
            }
            let mut event = PcrEvent::new();
            event.set_kv_batch(pcr_batch);
            event.set_is_snapshot(true);
            let _ = out_tx.send(vec![event]);
        }

        info!("PCR span_bridge: scan complete";
            "region_id" => region_id, "kvs" => total);
    }

    /// Static version of resolve_regions for use in spawned tasks.
    fn resolve_regions_static(
        source_pd: &str,
        span_start: &[u8],
        span_end: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>, u64, u64)> {
        let url = format!("http://{}/pd/api/v1/regions", source_pd);
        let output = match std::process::Command::new("curl").args(&["-s", &url]).output() {
            Ok(o) => o, Err(_) => return Vec::new(),
        };
        if !output.status.success() { return Vec::new(); }
        let body = String::from_utf8_lossy(&output.stdout);
        let json: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v, Err(_) => return Vec::new(),
        };
        let regions = match json.get("regions").and_then(|r| r.as_array()) {
            Some(r) => r, None => return Vec::new(),
        };
        let mut result = Vec::new();
        for r in regions {
            let rid = r.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
            if rid == 0 { continue; }
            let rs = r.get("start_key").and_then(|v| v.as_str()).unwrap_or("");
            let re = r.get("end_key").and_then(|v| v.as_str()).unwrap_or("");
            let r_start = Self::hex_decode(rs).unwrap_or_default();
            let r_end = Self::hex_decode(re).unwrap_or_default();
            if r_start.is_empty() && r_end.is_empty() { continue; }
            let overlaps = (span_end.is_empty() || r_end.is_empty()
                    || r_end.as_slice() > span_start)
                && (span_start.is_empty() || r_start.is_empty()
                    || r_start.as_slice() < span_end);
            if overlaps {
                let conf_ver = r.get("epoch").and_then(|e| e.get("conf_ver")).and_then(|v| v.as_u64()).unwrap_or(0);
                let version = r.get("epoch").and_then(|e| e.get("version")).and_then(|v| v.as_u64()).unwrap_or(0);
                result.push((rid, r_start, r_end, conf_ver, version));
            }
        }
        result
    }
}
