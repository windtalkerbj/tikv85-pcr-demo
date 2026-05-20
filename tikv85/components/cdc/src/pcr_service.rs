// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR gRPC service — Producer-side server (SOURCE cluster).
//!
//! Implements the `PcrStream` trait (generated from `pcrpb.proto`) to stream
//! MVCC KV change events, SST chunks, checkpoints, and split notifications
//! to a PCR Consumer (target cluster).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use grpcio::*;
use protobuf::Message;
use slog_global::{error, info, warn};
use stream_ingest::pcrpb::*;
use stream_ingest::pcrpb_grpc::PcrStream;
use tikv_util::worker::Scheduler;
use tokio::sync::Semaphore;

use crate::pcr_metrics::PCR_PRODUCER_METRICS;

use crate::endpoint::Task;

/// Global semaphore to limit concurrent snapshot scans.
lazy_static::lazy_static! {
    static ref SCAN_SEMAPHORE: Semaphore = Semaphore::new(66);
}

/// Prepend the RocksDB data-key prefix ('z') to a raw TiDB key so that
/// a DeleteRange sent to the consumer matches the stored key format.
pub fn build_data_key(raw_key: &[u8]) -> Vec<u8> {
    std::iter::once(b'z')
        .chain(raw_key.iter().copied())
        .collect()
}

/// PCR gRPC service — handles Subscribe RPCs from target TiKV clusters.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    /// Source TiKV RocksDB engine for Snapshot-based scans.
    source_engine: Option<Arc<engine_rocks::RocksEngine>>,
    /// Shared CDC resolved_ts updated by the Endpoint on each on_min_ts cycle.
    pcr_resolved_ts: Arc<AtomicU64>,
    /// PCR event batcher buffer size in bytes (default 1MB).
    event_buffer_size: usize,
    /// PCR event batcher flush interval in ms (default 150ms).
    event_flush_interval_ms: u64,
    /// Shared tokio runtime for PCR bridge tasks.
    bridge_runtime: Arc<tokio::runtime::Runtime>,
    /// Active event sinks for broadcasting DeleteRange events.
    event_sinks: Arc<std::sync::Mutex<Vec<futures::channel::mpsc::UnboundedSender<Vec<u8>>>>>,
}

impl Service {
    pub fn new(
        scheduler: Scheduler<Task>,
        source_engine: Option<Arc<engine_rocks::RocksEngine>>,
        pcr_resolved_ts: Arc<AtomicU64>,
        event_buffer_size: usize,
        event_flush_interval_ms: u64,
    ) -> Self {
        let bridge_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("pcr-bridge")
                .enable_time()
                .build()
                .expect("PCR: failed to create bridge tokio runtime"),
        );
        let event_sinks: Arc<std::sync::Mutex<Vec<futures::channel::mpsc::UnboundedSender<Vec<u8>>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        if source_engine.is_some() {
            info!("PCR: using RocksDB Snapshot for scans (engine direct, no secondary)");
        }

        // PCR UnsafeDestroyRange forwarder: gRPC handler → broadcast to all PCR subscribers.
        // Used for TRUNCATE / DROP TABLE cleanup that bypasses Raft.
        let sinks_for_dr = event_sinks.clone();
        let (tx, rx) = std::sync::mpsc::channel::<(Vec<u8>, Vec<u8>)>();
        tikv::server::service::set_pcr_delete_range_forwarder(tx);
        let dr_rt = bridge_runtime.clone();
        std::thread::spawn(move || {
            while let Ok((start_key, end_key)) = rx.recv() {
                use protobuf::Message;
                use stream_ingest::pcrpb::{PcrDeleteRange, PcrEvent};
                let data_start = build_data_key(&start_key);
                let data_end = build_data_key(&end_key);
                let mut dr = PcrDeleteRange::new();
                dr.set_start_key(data_start);
                dr.set_end_key(data_end);
                dr.set_ts(0);
                let sk_len = dr.get_start_key().len();
                let ek_len = dr.get_end_key().len();
                let mut event = PcrEvent::new();
                event.set_delete_range(dr);
                if let Ok(data) = event.write_to_bytes() {
                    let sinks = sinks_for_dr.lock().unwrap();
                    info!("PCR: broadcasting DeleteRange";
                        "start_key_len" => sk_len,
                        "end_key_len" => ek_len,
                        "subscribers" => sinks.len(),
                    );
                    for sink in sinks.iter() {
                        let _ = sink.unbounded_send(data.clone());
                    }
                }
            }
        });

        Self { scheduler, source_engine, pcr_resolved_ts, event_buffer_size, event_flush_interval_ms, bridge_runtime, event_sinks }
    }
}

impl PcrStream for Service {
    fn subscribe(
        &mut self,
        _ctx: RpcContext,
        req: PcrSubscribeRequest,
        sink: ServerStreamingSink<PcrEvent>,
    ) {
        let partition = req.get_partition();
        let region_id = if req.has_partition() {
            partition.get_region_id()
        } else {
            0
        };
        let start_ts = req.get_start_ts();

        info!(
            "PCR Subscribe request";
            "region_id" => region_id,
            "start_ts" => start_ts,
            "stream_id" => req.get_stream_id(),
        );

        // Span-based subscription: region_id==0 + key range set.
        // Source-side SpanBridge resolves regions, handles split/merge,
        // and merges all per-region sub-bridges transparently.
        let scan_start = partition.get_start_key().to_vec();
        let scan_end = partition.get_end_key().to_vec();
        let is_span = region_id == 0;

        if is_span {
            let span_start = if scan_start.is_empty() { vec![] } else { scan_start };
            let span_end = if scan_end.is_empty() { vec![] } else { scan_end };
            info!("PCR: span subscription";
                "start_key" => ?String::from_utf8_lossy(&span_start),
                "end_key" => ?String::from_utf8_lossy(&span_end),
            );
            let source_pd = "127.0.0.1:3379".to_string();
            let mut sb = crate::span_bridge::SpanBridge::new(
                span_start, span_end, source_pd, self.pcr_resolved_ts.clone(),
            );
            let sched = self.scheduler.clone();
            let engine = self.source_engine.clone();
            let buf_size = self.event_buffer_size;
            let flush_ms = self.event_flush_interval_ms;
            let brt = self.bridge_runtime.clone();
            let brt2 = self.bridge_runtime.clone();
            // SpanBridge::run() takes ownership and runs forever with
            // periodic region refresh. No need for mem::forget — the
            // async task owns the SpanBridge for its entire lifetime.
            brt.spawn(async move {
                sb.run(sched, engine, buf_size, flush_ms, start_ts, brt2, sink).await;
            });
            info!("PCR: span subscription active");
            return;
        }

        // Create a channel: CDC Delegate → gRPC bridge
        let (event_sink, event_rx) = mpsc::unbounded::<Vec<u8>>();

        // Register sink for UnsafeDestroyRange broadcast (TRUNCATE/DROP TABLE)
        self.event_sinks.lock().unwrap().push(event_sink.clone());

        // Schedule PCR stream registration on the CDC endpoint
        let task = Task::StartPcrStream {
            region_id,
            start_ts,
            event_sink,
            event_buffer_size: self.event_buffer_size,
            event_flush_interval_ms: self.event_flush_interval_ms,
        };

        if let Err(e) = self.scheduler.schedule(task) {
            error!("PCR: failed to schedule StartPcrStream"; "region_id" => region_id, "error" => ?e);
            return;
        }

        // Bridge task: snapshot scan → CDC incremental.
        let source_engine = self.source_engine.clone();
        let pcr_resolved_ts = self.pcr_resolved_ts.clone();
        let scan_start_key = partition.get_start_key().to_vec();
        let scan_end_key = partition.get_end_key().to_vec();
        let bridge_rt = self.bridge_runtime.clone();
        let bridge_rt2 = bridge_rt.clone();
        bridge_rt.spawn(async move {
            // bridge discarded
            let mut rx = event_rx;
            let mut sink = sink;

            // Full scan: spawn as background task, send results via channel.
            // This lets live CDC events flow through without waiting for the scan.
            let (scan_tx, mut scan_rx) = tokio::sync::mpsc::unbounded_channel::<PcrEvent>();
            if let Some(ref engine) = source_engine {
                let is_full = start_ts == 0;
                if is_full || start_ts > 0 {
                    let engine = engine.clone();
                    let scan_start = scan_start_key.clone();
                    let scan_end = scan_end_key.clone();
                    let scan_ts = start_ts;
                    bridge_rt2.spawn(async move {
                        let _permit = SCAN_SEMAPHORE.acquire().await;
                        let scan_type = if is_full { "full(default+write_cf)" } else { "delta(write_cf)" };
                        info!("PCR: scan range (background)"; "region_id" => region_id, "scan_type" => scan_type);
                        // (key, value, op, cf) tuples for chunked batch emission.
                        let mut all_kvs: Vec<(Vec<u8>, Vec<u8>, OpType, &str)> = Vec::new();
                        tokio::task::spawn_blocking(move || {
                            if is_full {
                                let dk = crate::pcr_snapshot::scan_default_cf(&engine, &scan_start, &scan_end, usize::MAX);
                                let wk = crate::pcr_snapshot::scan_write_cf_raw(&engine, &scan_start, &scan_end, usize::MAX);
                                (dk, wk, Vec::new(), Vec::new())
                            } else {
                                let (wr, dr) = crate::pcr_snapshot::scan_delta_entries(&engine, scan_ts, &scan_start, &scan_end);
                                (Vec::new(), Vec::new(), wr, dr)
                            }
                        }).await.map(|(dk, wk, wr, dr)| {
                            for (k, v) in &dk { all_kvs.push((k.clone(), v.clone(), OpType::Put, "default")); }
                            for (k, v) in &wk { all_kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
                            for (k, v) in &wr { all_kvs.push((k.clone(), v.clone(), OpType::Put, "write")); }
                            for (k, v, op) in &dr { all_kvs.push((k.clone(), v.clone(), *op, "default")); }
                        }).unwrap_or_default();
                        drop(_permit);
                        let total_kvs: usize = all_kvs.len();
                        if total_kvs > 0 {
                            for chunk in all_kvs.chunks(100) {
                                let mut batch = PcrKvBatch::new();
                                for (k, v, op, cf) in chunk {
                                    let mut kv = PcrKv::new();
                                    kv.set_key(k.clone()); kv.set_value(v.clone());
                                    kv.set_op(*op);
                                    kv.set_cf(cf.to_string());
                                    batch.mut_kvs().push(kv);
                                }
                                let mut event = PcrEvent::new();
                                event.set_kv_batch(batch);
                                event.set_is_snapshot(true);
                                let _ = scan_tx.send(event);
                            }
                        }
                        PCR_PRODUCER_METRICS.snapshot_regions_scanned.inc();
                        info!("PCR: background scan complete"; "region_id" => region_id,
                            "kvs" => total_kvs);
                    });
                }
            }

            // Main loop: interleave scan results, live events, and checkpoints.
            // Unlike the old design, we NEVER switch to a checkpoint-only mode —
            // live CDC events must be forwarded at all times, even if the CDC
            // channel temporarily drains (the endpoint re-registers the sender
            // after split/rebalance).
            let mut cp_interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut sent_bytes: u64 = 0;
            loop {
                tokio::select! {
                    Some(scan_event) = scan_rx.recv() => {
                        sent_bytes += scan_event.compute_size() as u64;
                        if sink.send((scan_event, WriteFlags::default())).await.is_err() { break; }
                    }
                    event = rx.next() => {
                        match event {
                            Some(data) => {
                                let data_len = data.len() as u64;
                                if let Ok(event) = protobuf::parse_from_bytes::<PcrEvent>(&data) {
                                    if sink.send((event, WriteFlags::default())).await.is_err() { break; }
                                }
                                PCR_PRODUCER_METRICS.bridge_bytes_sent.inc_by(data_len);
                            }
                            None => {
                                // All senders dropped — channel is permanently closed.
                                // Keep sending checkpoints so the consumer frontier advances.
                                warn!("PCR bridge: CDC channel permanently closed";
                                    "region_id" => region_id);
                                loop {
                                    cp_interval.tick().await;
                                    let resolved_ts = pcr_resolved_ts.load(Ordering::Acquire);
                                    if resolved_ts > 0 {
                                        let mut cp_event = PcrEvent::new();
                                        let mut cp = PcrCheckpoint::new();
                                        cp.set_resolved_ts(resolved_ts);
                                        cp.mut_region_ids().push(region_id);
                                        cp_event.set_checkpoint(cp);
                                        if sink.send((cp_event, WriteFlags::default())).await.is_err() { break; }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    _ = cp_interval.tick() => {
                        let resolved_ts = pcr_resolved_ts.load(Ordering::Acquire);
                        if resolved_ts > 0 {
                            let mut cp_event = PcrEvent::new();
                            let mut cp = PcrCheckpoint::new();
                            cp.set_resolved_ts(resolved_ts);
                            cp.mut_region_ids().push(region_id);
                            cp_event.set_checkpoint(cp);
                            if sink.send((cp_event, WriteFlags::default())).await.is_err() { break; }
                        }
                    }
                }
            }
            PCR_PRODUCER_METRICS.bridge_bytes_sent.inc_by(sent_bytes);
        });
    }
}

/// Create a gRPC Service for PCR subscription.
pub fn create_pcr_stream_service(service: Service) -> grpcio::Service {
    stream_ingest::pcrpb_grpc::create_pcr_stream(service)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_data_key() {
        let raw = b"t_100_";
        let data = build_data_key(raw);
        assert_eq!(data[0], b'z');
        assert_eq!(&data[1..], raw);
    }

    #[test]
    fn test_build_data_key_empty() {
        let raw: &[u8] = b"";
        let data = build_data_key(raw);
        assert_eq!(data, vec![b'z']);
    }

    #[test]
    fn test_build_data_key_with_null_bytes() {
        let raw = vec![0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x72];
        let data = build_data_key(&raw);
        assert_eq!(data[0], b'z');
        assert_eq!(&data[1..], &raw[..]);
    }
}
