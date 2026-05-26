// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Span-based PCR bridge — registers a span-level subscription with the
//! endpoint once, scans all regions for initial data, then delegates
//! live CDC event forwarding to endpoint's per-region delegates via a
//! shared sink. Region split/merge is transparent because the endpoint's
//! PcrRegistry auto-attaches the shared sink to new delegates at birth.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use engine_traits::Peekable;
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
        // Bounded merge channel: 32 slots × ~1MB = 32MB max buffered.
        // Backpressure: when gRPC writer is slow, scan tasks block on send(),
        // throttling the RocksDB scan to match consumer ingest rate.
        let (merge_tx, mut merge_rx) =
            tokio::sync::mpsc::channel::<Vec<PcrEvent>>(32);

        // Shared sink for delegates (futures channel) → relay → merge_tx.
        let (fut_tx, mut fut_rx) =
            futures::channel::mpsc::unbounded::<Vec<u8>>();
        let shared_sink = Arc::new(fut_tx);

        // Shared relay counters survive per-instance relay restarts.
        // Without these, the 30s heartbeat always reports 0 because the
        // relay instance that processed events already exited.
        let relay_events: Arc<std::sync::atomic::AtomicU64> = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let relay_parse_ok: Arc<std::sync::atomic::AtomicU64> = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let relay_parse_fail: Arc<std::sync::atomic::AtomicU64> = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Relay: collect up to 64 events or 1ms, then batch-send to merge.
        // This amortizes per-event Tokio wakeup across the merge channel.
        let relay_tx = merge_tx.clone();
        let r_events = relay_events.clone();
        let r_ok = relay_parse_ok.clone();
        let r_fail = relay_parse_fail.clone();
        bridge_rt.spawn(async move {
            let mut buf: Vec<PcrEvent> = Vec::with_capacity(64);
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    first_data = fut_rx.next() => {
                        let first = match first_data {
                            Some(data) => data,
                            None => break,
                        };
                        if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&first) {
                            buf.push(event);
                            r_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            r_fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                                        r_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    } else {
                                        r_fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    if buf.len() >= 64 { break; }
                                }
                                _ => break,
                            }
                        }
                        if !buf.is_empty() {
                            let batch = std::mem::take(&mut buf);
                            r_events.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            if relay_tx.send(batch).await.is_err() {
                                warn!("PCR relay: merge_tx send failed, exiting";
                                    "events_processed" => r_events.load(std::sync::atomic::Ordering::Relaxed),
                                    "parse_ok" => r_ok.load(std::sync::atomic::Ordering::Relaxed),
                                    "parse_fail" => r_fail.load(std::sync::atomic::Ordering::Relaxed),
                                );
                                break;
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        info!("PCR relay: heartbeat";
                            "events_processed" => r_events.load(std::sync::atomic::Ordering::Relaxed),
                            "parse_ok" => r_ok.load(std::sync::atomic::Ordering::Relaxed),
                            "parse_fail" => r_fail.load(std::sync::atomic::Ordering::Relaxed),
                        );
                    }
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
        // NOTE: after sink.send() returns Err, the underlying CqFuture is consumed.
        // Polling it again causes FATAL panic ("Resolved future is not supposed
        // to be polled again"). We must break out of both loops.
        let writer_rt = bridge_rt.clone();
        writer_rt.spawn(async move {
            'writer: while let Some(batch) = merge_rx.recv().await {
                for event in batch {
                    if sink.send((event, WriteFlags::default())).await.is_err() {
                        warn!("PCR span_bridge: gRPC sink send failed, closing writer");
                        break 'writer;
                    }
                }
            }
        });

        // Periodic checkpoint ticker: sends Checkpoint events every 5s.
        let regions = self.resolve_regions();
        let mut known_region_ids: Vec<u64> = regions.iter().map(|(rid, _, _, _, _)| *rid).collect();
        let mut cp_ticker = tokio::time::interval(std::time::Duration::from_secs(5));
        // Region re-discovery: catch new regions from CREATE TABLE etc.
        // Every 30s, re-resolve the span and scan any new regions.
        let mut rediscover = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = cp_ticker.tick() => {
                    let resolved_ts = self.pcr_resolved_ts.load(Ordering::Acquire);
                    if resolved_ts > 0 {
                        let mut cp = PcrCheckpoint::new();
                        cp.set_resolved_ts(resolved_ts);
                        for &rid in &known_region_ids {
                            cp.mut_region_ids().push(rid);
                        }
                        let mut event = PcrEvent::new();
                        event.set_checkpoint(cp);
                        let batch = vec![event];
                        let _ = merge_tx.send(batch).await;
                    }
                }
                _ = rediscover.tick() => {
                    let current = self.resolve_regions();
                    for &(region_id, _, _, _, _) in &current {
                        if !known_region_ids.contains(&region_id) {
                            info!("PCR span_bridge: new region discovered"; "region_id" => region_id);
                            known_region_ids.push(region_id);
                            // Ensure delegate exists for this region
                            let (sink, _rx) = futures::channel::mpsc::unbounded::<Vec<u8>>();
                            let _ = scheduler.schedule(Task::StartPcrStream {
                                region_id, start_ts,
                                event_sink: sink,
                                event_buffer_size,
                                event_flush_interval_ms,
                            });
                            // Full scan the new region
                            if let Some(ref engine) = source_engine {
                                let engine = engine.clone();
                                let tx = merge_tx.clone();
                                let is_full = start_ts == 0;
                                let scan_start_ts = start_ts;
                                let bridge_rt2 = bridge_rt.clone();
                                bridge_rt2.spawn(async move {
                                    let _permit = SCAN_SEM.acquire().await;
                                    Self::scan_region(region_id, is_full, scan_start_ts, engine, tx).await;
                                });
                            }
                        }
                    }
                    // Refresh span subscription: re-register with a fresh
                    // shared_sink + relay so delegates that lost their channel
                    // (gRPC disconnect) get live senders again.
                    {
                        let (fut_tx2, mut fut_rx2) =
                            futures::channel::mpsc::unbounded::<Vec<u8>>();
                        let shared_sink2 = Arc::new(fut_tx2);
                        let relay_tx2 = merge_tx.clone();
                        let r_events = relay_events.clone();
                        let r_ok = relay_parse_ok.clone();
                        let r_fail = relay_parse_fail.clone();
                        let bridge_rt3 = bridge_rt.clone();
                        bridge_rt3.spawn(async move {
                            let mut buf: Vec<PcrEvent> = Vec::with_capacity(64);
                            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
                            loop {
                                tokio::select! {
                                    first_data = fut_rx2.next() => {
                                        let first = match first_data {
                                            Some(data) => data,
                                            None => break,
                                        };
                                        if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&first) {
                                            buf.push(event);
                                            r_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        } else {
                                            r_fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        loop {
                                            match tokio::time::timeout(
                                                std::time::Duration::from_millis(1),
                                                fut_rx2.next(),
                                            ).await
                                            {
                                                Ok(Some(data)) => {
                                                    if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&data) {
                                                        buf.push(event);
                                                        r_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    } else {
                                                        r_fail.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    }
                                                    if buf.len() >= 64 { break; }
                                                }
                                                _ => break,
                                            }
                                        }
                                        if !buf.is_empty() {
                                            let batch = std::mem::take(&mut buf);
                                            r_events.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                            if relay_tx2.send(batch).await.is_err() {
                                                warn!("PCR relay(reconnect): merge_tx send failed, exiting";
                                                    "events_processed" => r_events.load(std::sync::atomic::Ordering::Relaxed),
                                                    "parse_ok" => r_ok.load(std::sync::atomic::Ordering::Relaxed),
                                                    "parse_fail" => r_fail.load(std::sync::atomic::Ordering::Relaxed),
                                                );
                                                break;
                                            }
                                        }
                                    }
                                    _ = heartbeat.tick() => {
                                        info!("PCR relay(reconnect): heartbeat";
                                            "events_processed" => r_events.load(std::sync::atomic::Ordering::Relaxed),
                                            "parse_ok" => r_ok.load(std::sync::atomic::Ordering::Relaxed),
                                            "parse_fail" => r_fail.load(std::sync::atomic::Ordering::Relaxed),
                                        );
                                    }
                                }
                            }
                        });
                        let task = Task::RegisterSpanSubscription {
                            start_key: self.span_start.clone(),
                            end_key: self.span_end.clone(),
                            shared_sink: shared_sink2,
                            event_buffer_size,
                            event_flush_interval_ms,
                        };
                        if let Err(e) = scheduler.schedule(task) {
                            error!("PCR span_bridge: failed to re-register";
                                "error" => ?e);
                        }
                    }
                }
            }
        }
    }

    /// Stream scan: uses pcr_snapshot::stream_cf_full (same RocksSnapshot/iterator
    /// logic as scan_default_cf) with a callback that builds PcrKv batches by
    /// byte count. Memory is O(BATCH_BYTES) — no full region materialization.
    async fn scan_region(
        region_id: u64,
        is_full: bool,
        start_ts: u64,
        source_engine: Arc<engine_rocks::RocksEngine>,
        out_tx: tokio::sync::mpsc::Sender<Vec<PcrEvent>>,
    ) {
        tokio::task::spawn_blocking(move || {
            let engine = &source_engine;
            let mut batch_kvs: Vec<PcrKv> = Vec::with_capacity(4096);
            let mut batch_bytes: usize = 0;
            let mut total: u64 = 0;
            let mut sent_batches: u64 = 0;

            let mut maybe_flush = |kvs: &mut Vec<PcrKv>, bytes: &mut usize| {
                if *bytes >= BATCH_BYTES {
                    let mut pcr_batch = PcrKvBatch::new();
                    for kv in kvs.drain(..) { pcr_batch.mut_kvs().push(kv); }
                    let mut event = PcrEvent::new();
                    event.set_kv_batch(pcr_batch);
                    event.set_is_snapshot(true);
                    let _ = out_tx.blocking_send(vec![event]);
                    *bytes = 0;
                    sent_batches += 1;
                }
            };

            let mut add_kv = |cf: &str, op: OpType, key: Vec<u8>, value: Vec<u8>| {
                let entry = key.len() + value.len();
                let mut kv = PcrKv::new();
                kv.set_key(key); kv.set_value(value); kv.set_op(op); kv.set_cf(cf.to_string());
                batch_bytes += entry;
                total += 1;
                batch_kvs.push(kv);
                maybe_flush(&mut batch_kvs, &mut batch_bytes);
            };

            let (n1, n2);
            if is_full {
                let def_count = std::sync::atomic::AtomicU64::new(0);
                let write_count = std::sync::atomic::AtomicU64::new(0);
                // Count short_value availability for meta key WriteRefs
                let has_sv = std::sync::atomic::AtomicU64::new(0);
                let no_sv = std::sync::atomic::AtomicU64::new(0);
                n1 = crate::pcr_snapshot::stream_cf_full(engine, "default", OpType::Put, |k, v| {
                    def_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    add_kv("default", OpType::Put, k, v);
                });
                n2 = crate::pcr_snapshot::stream_cf_full(engine, "write", OpType::Put, |k, v| {
                    write_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if k.windows(5).any(|w| w == b"mDB:1") {
                        // Parse WriteRef to find short_value and synthesize DEFAULT CF
                        let mut pos = 1; // skip write_type
                        while pos < v.len() && (v[pos] & 0x80) != 0 { pos += 1; }
                        pos += 1; // skip final var_u64 byte
                        if pos < v.len() && v[pos] == 0x76 {
                            has_sv.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            pos += 1; // skip SHORT_VALUE_PREFIX
                            if pos < v.len() {
                                let sv_len = v[pos] as usize;
                                pos += 1;
                                if pos + sv_len <= v.len() {
                                    let short_val = &v[pos..pos + sv_len];
                                    // Synthesize DEFAULT CF key:
                                    // WRITE CF key = z + raw_key + !commit_ts (8B)
                                    // DEFAULT CF key = raw_key + !start_ts
                                    // The WriteRef start_ts is in the first bytes of v (as var_u64),
                                    // compute it as u64 to form !start_ts
                                    let sv_start_ts = {
                                        let mut p = 1; let mut val: u64 = 0;
                                        while p < v.len() {
                                            let b = v[p]; p += 1;
                                            val = (val << 7) | ((b & 0x7F) as u64);
                                            if (b & 0x80) == 0 { break; }
                                        }
                                        !val // inverted TS for DEFAULT CF key
                                    };
                                    if k.len() >= 9 {
                                        // raw_prefix = k without z-prefix and !commit_ts suffix
                                        let raw_data = &k[..k.len()-8];
                                        let mut def_key = Vec::with_capacity(raw_data.len() + 8);
                                        def_key.extend_from_slice(raw_data);
                                        def_key.extend_from_slice(&sv_start_ts.to_be_bytes());
                                        add_kv("default", OpType::Put, def_key, short_val.to_vec());
                                    }
                                }
                            }
                        } else {
                            let idx = no_sv.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // No short_value: read DEFAULT CF from source RocksDB at start_ts
                            if k.len() >= 9 {
                                let raw_data = &k[..k.len()-8];
                                let sv_start_ts = {
                                    let mut p = 1; let mut val: u64 = 0;
                                    while p < v.len() {
                                        let b = v[p]; p += 1;
                                        val = (val << 7) | ((b & 0x7F) as u64);
                                        if (b & 0x80) == 0 { break; }
                                    }
                                    val
                                };
                                let mut def_key = Vec::with_capacity(raw_data.len() + 8);
                                        def_key.extend_from_slice(raw_data);
                                def_key.extend_from_slice(&(!sv_start_ts).to_be_bytes());
                                // Dump first 3 mDB entries for root cause analysis
                                if idx < 3 {
                                    let commit_ts_raw = &k[k.len()-8..];
                                    info!("PCR: mDB key diagnostic";
                                        "idx" => idx,
                                        "region_id" => region_id,
                                        "write_key_prefix" => ?format!("{:02x?}", &k[..std::cmp::min(k.len(), 16)]),
                                        "write_val_prefix" => ?format!("{:02x?}", &v[..std::cmp::min(v.len(), 16)]),
                                        "parsed_start_ts" => sv_start_ts,
                                        "commit_ts_bytes" => ?format!("{:02x?}", commit_ts_raw),
                                        "def_key" => ?format!("{:02x?}", &def_key[..std::cmp::min(def_key.len(), 32)]),
                                    );
                                }
                                if let Ok(Some(def_val)) = engine.get_value_cf("default", &def_key) {
                                    if idx < 3 {
                                        info!("PCR: get_value_cf OK";
                                            "idx" => idx, "vlen" => def_val.len(),
                                            "val_prefix" => ?format!("{:02x?}", &def_val[..std::cmp::min(def_val.len(), 20)]),
                                        );
                                    }
                                    let def_bytes = def_val.to_vec();
                                    add_kv("default", OpType::Put, def_key, def_bytes);
                                } else {
                                    if idx < 3 {
                                        info!("PCR: get_value_cf MISS — source has no DEFAULT CF at this TS";
                                            "idx" => idx);
                                    }
                                    let mut fallback_def_key = Vec::with_capacity(raw_data.len() + 8);
                                    fallback_def_key.extend_from_slice(raw_data);
                                    fallback_def_key.extend_from_slice(&k[k.len()-8..]);
                                    add_kv("default", OpType::Put, fallback_def_key, v.to_vec());
                                }
                            }
                        }
                    }
                    add_kv("write", OpType::Put, k, v);
                });
                info!("PCR full scan: meta key short_value stats";
                    "region_id" => region_id,
                    "default_kvs" => def_count.load(std::sync::atomic::Ordering::Relaxed),
                    "write_kvs" => write_count.load(std::sync::atomic::Ordering::Relaxed),
                    "has_short_value" => has_sv.load(std::sync::atomic::Ordering::Relaxed),
                    "no_short_value" => no_sv.load(std::sync::atomic::Ordering::Relaxed),
                );
            } else {
                n1 = 0; n2 = 0;
                // Delta: use existing scan_delta_entries (not streaming-critical yet)
                let (wr, dr) = crate::pcr_snapshot::scan_delta_entries(engine, start_ts, &[], &[]);
                for (k, v) in &wr { add_kv("write", OpType::Put, k.clone(), v.clone()); }
                for (k, v, op) in &dr { add_kv("default", *op, k.clone(), v.clone()); }
            }
            // Drain residual
            if !batch_kvs.is_empty() {
                let mut pcr_batch = PcrKvBatch::new();
                for kv in batch_kvs.drain(..) { pcr_batch.mut_kvs().push(kv); }
                let mut event = PcrEvent::new();
                event.set_kv_batch(pcr_batch);
                event.set_is_snapshot(true);
                let _ = out_tx.blocking_send(vec![event]);
                sent_batches += 1;
            }

            let residual = batch_kvs.len();
            info!("PCR span_bridge: scan complete";
                "region_id" => region_id, "kvs" => total,
                "cf_default" => n1, "cf_write" => n2,
                "batches_sent" => sent_batches);
        }).await.unwrap_or_default();
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
