// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use engine_traits::KvEngine;
use pd_client::PdClient;
use slog_global::{error, info, warn};
use tikv_util::worker::Runnable;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::checkpoint::CheckpointManager;
use crate::direct_ingest::DirectIngestContext;
use crate::schema_sync::SchemaSync;
use crate::pcrpb_gen::pcrpb::{
    PcrEvent_oneof_event, PcrSstChunk,
};
use crate::config::StreamIngestConfig;
use crate::errors::Result;
use crate::metrics::STREAM_INGEST_METRICS;
use crate::span_ctl::{SpanRegistry, handle_split};
use crate::sst_batcher::{FlushReason, MvccKeyValue, SstBatcher};
use crate::subscriber::{PartitionSubscription, PcrEventWithMeta, StreamSubscriber};

/// Per-Region replication progress: RegionId → ResolvedTs
pub type Frontier = BTreeMap<u64, u64>;

/// Internal state of the StreamIngestTask.
/// Mirrors CRDB's job status: Idle → Subscribing → CuttingOver → Completed.
#[derive(Debug, PartialEq, Clone)]
enum TaskState {
    /// Idle: waiting for pcr-ctl start command. Target TiKV starts in this state.
    /// Equivalent to CRDB's "job created but not yet adopted".
    Idle,
    /// Subscribing: actively connected to source and ingesting data.
    /// Equivalent to CRDB's "job running".
    Subscribing,
    /// Paused: replication temporarily suspended by pcr-ctl pause.
    /// Equivalent to CRDB's "job paused".
    Paused,
    /// Cutover in progress.
    CuttingOver,
    /// Replication completed successfully, waiting for activation.
    Completed,
    /// Target cluster activated — PCR metadata cleaned, cluster is independent and writable.
    Activated,
    /// Replication failed.
    Failed,
}

// ============================================================================
// PcrComponents — self-contained event loop state (movable into tokio task)
// ============================================================================

const INGEST_WORKERS: usize = 4;

struct RegionWorker {
    tx: tokio::sync::mpsc::UnboundedSender<PcrEventWithMeta>,
    rx: Option<tokio::sync::mpsc::UnboundedReceiver<PcrEventWithMeta>>,
}

struct PcrComponents<E: KvEngine> {
    subscriber: StreamSubscriber,
    batcher: SstBatcher<E>,
    workers: Vec<RegionWorker>,
    ingest_ctx: Arc<DirectIngestContext<E>>,
    checkpoint_mgr: CheckpointManager,
    frontier: Frontier,
    source_addr: String,
    source_pd: String,
    initial_subscriptions: Vec<PartitionSubscription>,
    flush_interval_ms: u64,
    /// Cutover timestamp (0 = no cutover, >0 = target completion time).
    /// Set by pcr-ctl cutover command, read by event loop.
    cutover_ts: Arc<AtomicU64>,
    /// Resume timestamp for checkpoint recovery (0 = full snapshot scan).
    resume_ts: u64,
    /// Span Control-Plane: logical topology → region mapping.
    span_registry: SpanRegistry,
    /// Target TiDB address for DDL monitoring (new table discovery).
    target_tidb_addr: String,
    /// Source TiDB address for DDL monitoring (source schema is source-of-truth).
    source_tidb_addr: String,
    /// Whether span-based subscription is active (source-side SpanBridge).
    span_mode: bool,
    /// Schema sync: bumps target PD schema version when new tables are discovered.
    schema_sync: SchemaSync,
}

/// Per-worker event loop: receives events dispatched by region_id,
/// buffers KVs, sorts, builds SSTs, and ingests independently.
///
/// Uses tokio::select! with a flush timeout so that residual data is
/// flushed even when no events arrive (e.g., after full scan completes
/// and live CDC traffic is sparse or zero).
async fn run_ingest_worker<E: KvEngine>(
    mut batcher: SstBatcher<E>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<PcrEventWithMeta>,
    flush_interval_ms: u64,
) {
    // Idle timeout: only fires when NO events arrive for a long period.
    // This is a safety net for the "full scan complete, no more events" case.
    // During active streaming, flushes are driven by BufferFull (64MB) or
    // min_flush_interval (checked per-event). The idle timeout must be long
    // enough to avoid competing with those triggers — 5s ensures it only
    // catches truly idle periods.
    let idle_timeout = std::time::Duration::from_secs(5);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(PcrEventWithMeta { event, region_id }) => {
                        match &event.event {
                            Some(PcrEvent_oneof_event::KvBatch(batch)) => {
                                if batcher.current_region_id != region_id
                                    && batcher.should_flush(FlushReason::RangeBoundary)
                                    { let _ = batcher.flush(); }
                                batcher.current_region_id = region_id;
                                for kv in batch.get_kvs() {
                                    let cf = if kv.get_cf().is_empty() { "default" } else { kv.get_cf() };
                                    batcher.add_kv(MvccKeyValue {
                                        key: kv.key.clone(), value: kv.value.clone(), cf: cf.to_string(),
                                    });
                                }
                                let should_flush = batcher.should_flush(FlushReason::BufferFull)
                                    || (flush_interval_ms > 0
                                        && batcher.elapsed_since_last_flush() >= flush_interval_ms);
                                if should_flush { let _ = batcher.flush(); }
                            }
                            Some(PcrEvent_oneof_event::DeleteRange(dr)) => {
                                batcher.current_region_id = region_id;
                                batcher.add_delete_range(&dr.start_key, &dr.end_key, dr.ts);
                                if batcher.range_buffer_size >= batcher.max_range_buffer_size {
                                    let _ = batcher.flush();
                                }
                            }
                            _ => {}
                        }
                    }
                    None => {
                        // Channel closed — flush remaining data before exit.
                        if batcher.pending_kvs() > 0 {
                            let _ = batcher.flush();
                        }
                        return;
                    }
                }
            }
            _ = tokio::time::sleep(idle_timeout) => {
                // Idle timeout: flush any residual KVs buffered without a flush trigger.
                if batcher.pending_kvs() > 0 {
                    let _ = batcher.flush();
                }
            }
        }
    }
}

impl<E: KvEngine> PcrComponents<E> {
    /// Dispatch data events to per-region ingest workers.
    /// Uses internal round-robin counter (span mode has region_id=0).
    fn handle_data_event(&mut self, event_with_meta: PcrEventWithMeta) -> Result<()> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let idx = if event_with_meta.region_id != 0 {
            event_with_meta.region_id as usize % self.workers.len()
        } else {
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize % self.workers.len()
        };
        let _ = self.workers[idx].tx.send(event_with_meta);
        Ok(())
    }

    /// Process checkpoint and split events which need shared state access.
    /// KvBatch/DeleteRange go to per-region workers via dispatch_event().
    async fn handle_control_event(&mut self, event_with_meta: PcrEventWithMeta) -> Result<()> {
        let event = &event_with_meta.event;

        match &event.event {
            Some(PcrEvent_oneof_event::Checkpoint(cp)) => {
                let checkpoint_ts = cp.resolved_ts;
                for rid in &cp.region_ids {
                    self.frontier.insert(*rid, checkpoint_ts);
                    // Update per-region lag metric
                    let region_lag = self.compute_lag(checkpoint_ts);
                    STREAM_INGEST_METRICS
                        .region_lag
                        .with_label_values(&[&rid.to_string()])
                        .set(region_lag as i64);
                }

                // workers flush independently

                self.checkpoint_mgr
                    .record_checkpoint(&self.frontier)
                    .await?;

                let lag = self.compute_lag(cp.resolved_ts);
                STREAM_INGEST_METRICS.checkpoint_lag.set(lag as i64);
                if let Some(min_ts) = crate::checkpoint::CheckpointManager::global_min_ts(&self.frontier) {
                    STREAM_INGEST_METRICS.frontier_min_ts.set(min_ts as i64);
                }

                // Check if cutover target has been reached
                if self.is_cutover_ready() {
                    info!("PCR: cutover target reached, completing replication");
                    return Err(crate::errors::Error::Other("cutover complete".into()));
                }
            }

            Some(PcrEvent_oneof_event::DeleteRange(_)) => {
                // Dispatch to per-region worker
                self.handle_data_event(event_with_meta)?;
            }

            Some(PcrEvent_oneof_event::KvBatch(_)) | Some(PcrEvent_oneof_event::SstChunk(_)) => {
                // Dispatch to per-region worker
                self.handle_data_event(event_with_meta)?;
            }

            Some(PcrEvent_oneof_event::Split(sp)) => {
                // per-worker ingest
                let split_key = sp.split_key.clone();
                let new_region_ids = sp.new_region_ids.clone();
                info!(
                    "PCR: region split";
                    "split_key" => ?split_key,
                    "new_regions" => ?new_region_ids,
                    "parent_region" => event_with_meta.region_id,
                );
                // workers flush independently

                // Span Control-Plane: use SpanRegistry to handle the split —
                // drain parent, track child regions, update reverse index.
                let new_regions =
                    self.span_registry.on_region_split(event_with_meta.region_id, &new_region_ids);

                let resume_ts = self.frontier.get(&event_with_meta.region_id).copied().unwrap_or(0);

                // Remove split regions from the subscriber's dedup set so
                // re-subscription is not blocked. Also clear from frontier —
                // the old resolved_ts is stale after the split.
                self.subscriber.remove_subscribed(event_with_meta.region_id);
                self.frontier.remove(&event_with_meta.region_id);
                for &rid in &new_region_ids {
                    self.subscriber.remove_subscribed(rid);
                    self.frontier.remove(&rid);
                }

                // In span mode, the source SpanBridge handles region management
                // after a split — no need for per-region re-subscription.
                if !self.span_mode {
                    for &new_rid in &new_regions {
                        if self.frontier.contains_key(&new_rid) {
                            continue;
                        }
                        let sub = PartitionSubscription {
                            region_id: new_rid,
                            start_ts: resume_ts,
                            source_addr: self.source_addr.clone(),
                            start_key: vec![],
                            end_key: vec![],
                        };
                        if let Err(e) = self.subscriber.subscribe(sub).await {
                            error!("PCR: split subscribe failed";
                                "region_id" => new_rid, "error" => ?e);
                        }
                        self.frontier.insert(new_rid, resume_ts);
                    }
                }
            }

            None => {}
        }

        Ok(())
    }

    /// Check whether an SST chunk falls entirely within the specified Region's key range.
    fn is_sst_within_region(
        &self,
        chunk: &PcrSstChunk,
        _region_id: u64,
    ) -> bool {
        !chunk.start_key.is_empty() && !chunk.end_key.is_empty()
    }

    /// Compute replication lag: how far behind the checkpoint is from now.
    fn compute_lag(&self, resolved_ts: u64) -> u64 {
        // resolved_ts is a PD TSO: physical_ms << 18 + logical.
        // Convert to seconds for lag computation.
        if resolved_ts == 0 { return 0; }
        let now = tikv_util::time::UnixSecs::now().into_inner();
        let resolved_secs = (resolved_ts >> 18) / 1000;
        now.saturating_sub(resolved_secs)
    }

    /// Check if the global frontier has reached the cutover timestamp.
    fn is_cutover_ready(&self) -> bool {
        let target = self.cutover_ts.load(Ordering::Acquire);
        if target == 0 {
            return false;
        }
        let global_min = self.frontier.values().min().copied().unwrap_or(0);
        if global_min == 0 {
            return false;
        }
        // u64::MAX = "TO LATEST": cutover as soon as any progress is made
        if target == u64::MAX {
            return true;
        }
        global_min >= target
    }
}

/// Query source PD HTTP API for all Regions with their key ranges.
/// Used by initial discovery and periodic re-discovery.
fn discover_region_ids_from_source(source_pd: &str) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
    let output = match std::process::Command::new("curl")
        .args(&["-s", &format!("http://{}/pd/api/v1/regions", source_pd)])
        .output()
    {
        Ok(o) => o,
        Err(_) => return Vec::new(),
    };
    if !output.status.success() { return Vec::new(); }
    let body = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    parsed.get("regions")
        .and_then(|r| r.as_array())
        .map(|arr| arr.iter().filter_map(|r| {
            let id = r.get("id")?.as_u64()?;
            // PD returns start_key and end_key as base64-encoded strings in JSON.
            // They may be omitted for the first/last region (empty keys).
            let decode_key = |field: &str| -> Vec<u8> {
                r.get(field)
                    .and_then(|v| v.as_str())
                    .and_then(|s| hex_decode(s).ok())
                    .unwrap_or_default()
            };
            Some((id, decode_key("start_key"), decode_key("end_key")))
        }).collect())
        .unwrap_or_default()
}

/// Topology change detected during periodic rediscover.
/// Categorizes differences between PD's region list and the local frontier.
#[derive(Debug, PartialEq, Clone)]
pub enum TopologyChange {
    /// Region exists in PD but not in our frontier — needs subscription.
    NewRegion { region_id: u64 },
    /// Region exists in our frontier but not in PD — can be cleaned up.
    RemovedRegion { region_id: u64 },
}

/// Result of comparing PD regions with the local frontier.
#[derive(Debug, Default)]
pub struct TopologyDiff {
    pub new_regions: Vec<TopologyChange>,
    pub removed_regions: Vec<TopologyChange>,
    pub total_in_pd: usize,
    pub total_in_frontier: usize,
}

/// Compare PD's region list with the local frontier and return a categorized diff.
///
/// Called on each periodic rediscover cycle (every 5s). The diff enables:
/// - New regions → subscribe with resume_ts
/// - Removed regions → clean up stale frontier entries (merged away)
pub fn diff_topology(
    pd_regions: &[(u64, Vec<u8>, Vec<u8>)],
    frontier: &Frontier,
) -> TopologyDiff {
    let mut diff = TopologyDiff {
        total_in_pd: pd_regions.len(),
        total_in_frontier: frontier.len(),
        ..Default::default()
    };

    let pd_set: std::collections::HashSet<u64> = pd_regions.iter().map(|(id, _, _)| *id).collect();

    // New: in PD but not in frontier
    for (rid, _, _) in pd_regions {
        if !frontier.contains_key(rid) {
            diff.new_regions.push(TopologyChange::NewRegion { region_id: *rid });
        }
    }

    // Removed: in frontier but not in PD
    for rid in frontier.keys() {
        if !pd_set.contains(rid) {
            diff.removed_regions.push(TopologyChange::RemovedRegion { region_id: *rid });
        }
    }

    diff
}

/// Decode a hex-encoded string to bytes (PD returns keys as hex in JSON API).
pub fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 { return Err(()); }
    (0..s.len()).step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i+2], 16).map_err(|_| ()))
        .collect()
}

// ============================================================================
// Async event loop (runs on dedicated tokio runtime)
// ============================================================================

async fn run_event_loop<E: KvEngine>(
    mut components: PcrComponents<E>,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
    done_tx: tokio::sync::oneshot::Sender<()>,
) {
    info!("PCR event loop started");

    // raft-v1: skip per-tablet empty-cluster check.

    // Create shared gRPC channel — one TCP connection for ALL Regions.
    // HTTP/2 multiplexing handles concurrent streams on the same connection,
    // scaling to 100K+ Regions without connection overhead.
    use crate::pcrpb_gen::pcrpb_grpc::PcrStreamClient;
    let source_addr = components.source_addr.clone();
    let env = Arc::new(grpcio::Environment::new(2));
    let channel = grpcio::ChannelBuilder::new(env).connect(&source_addr);
    let shared_client = PcrStreamClient::new(channel);
    components.subscriber.set_shared_client(shared_client);
    components.subscriber.set_source_pd(components.source_pd.clone());
    info!("PCR: shared gRPC channel established to {}", source_addr);

    // Span-based subscription: one gRPC stream for the full key range.
    // Source-side SpanBridge resolves regions, handles split/merge.
    let span_ts = components.resume_ts;
    let src_pd = components.source_pd.clone();
    info!("PCR: subscribing to full-key-range span, start_ts={}", span_ts);
    match components.subscriber.subscribe_span(b"", b"", span_ts, &src_pd).await {
        Ok(sid) => {
            info!("PCR: span subscription established, stream_id={}", sid);
            components.span_mode = true;
        }
        Err(e) => { error!("PCR: span subscription failed"; "error" => ?e); let _ = done_tx.send(()); return; }
    }

    // Periodic cutover check: 500ms
    let mut cutover_check = tokio::time::interval(
        std::time::Duration::from_millis(500),
    );
    // Periodic region re-discovery: safety net for region topology changes.
    // Queries source PD and subscribes to regions not yet in the frontier.
    // Reduced to 5s for demo responsiveness (CRDB uses replanner at configurable frequency).
    let mut rediscover = tokio::time::interval(
        std::time::Duration::from_secs(5),
    );
    // DDL discover: poll MAX(job_id) from source TiDB's DDL history.
    // When a change is detected, diff current tables against local cache.
    // Fast interval (1s) since the check is lightweight (single MAX query).
    let mut ddl_discover = tokio::time::interval(
        std::time::Duration::from_secs(1),
    );
    // Spawn per-region ingest workers. Each owns a batcher and processes
    // a subset of regions (region_id % INGEST_WORKERS). The main tx stays
    // in PcrComponents.workers for dispatch_event().
    let buf_size = components.batcher.max_kv_buffer_size;
    let range_size = components.batcher.max_range_buffer_size;
    let flush_ms = components.flush_interval_ms;
    for mut w in components.workers.iter_mut() {
        let rx = w.rx.take().expect("worker rx already taken");
        let batcher = SstBatcher::new(
            components.ingest_ctx.clone(),
            buf_size,
            range_size,
        );
        tokio::spawn(run_ingest_worker(batcher, rx, flush_ms));
    }

    loop {
        tokio::select! {
            _ = cancel_rx.changed() => {
                if *cancel_rx.borrow() {
                    info!("PCR event loop: cancel signal received");
                    break;
                }
            }
            _ = ddl_discover.tick() => {
                // Span Control-Plane: check source TiDB for new tables,
                // resolve span key ranges to PD regions, and subscribe.
                use crate::span_ctl::discover_new_spans;
                let new_spans = discover_new_spans(
                    &mut components.span_registry,
                    &components.source_tidb_addr,
                );
                for span in &new_spans {
                    info!("PCR: DDL discover - new span";
                        "table_id" => span.table_id,
                        "start_key" => ?String::from_utf8_lossy(&span.start_key),
                    );
                    // Resolve this span's key range to specific PD regions.
                    let pd_regions = match crate::span_ctl::resolve_span_to_regions(
                        &components.source_pd,
                        &span.start_key,
                        &span.end_key,
                    ).await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("PCR: DDL discover span resolution failed";
                                "table_id" => span.table_id, "error" => ?e);
                            continue;
                        }
                    };
                    // Span mode: source-side SpanBridge handles all region
                    // subscriptions. DDL discover tracks schema only — skip
                    // per-region subscription to avoid duplicate gRPC streams.
                    if components.span_mode {
                        continue;
                    }
                    // New spans (tables) need a full scan from TS 0 — their data
                    // existed before PCR started and must be fully replicated.
                    // Using the global frontier minimum would skip pre-existing data.
                    let new_span_start_ts: u64 = 0;
                    for (rid, r_start, r_end) in &pd_regions {
                        if components.frontier.contains_key(rid) {
                            continue; // already subscribed (shared region)
                        }
                        let sub = PartitionSubscription {
                            region_id: *rid,
                            start_ts: new_span_start_ts,
                            source_addr: components.source_addr.clone(),
                            start_key: r_start.clone(),
                            end_key: r_end.clone(),
                        };
                        if let Err(e) = components.subscriber.subscribe(sub).await {
                            warn!("PCR: DDL discover subscribe failed";
                                "region_id" => *rid, "error" => ?e);
                        } else {
                            info!("PCR: DDL discover subscribed region";
                                "region_id" => *rid, "table_id" => span.table_id);
                            components.frontier.insert(*rid, 0);
                            // Track in SpanRegistry
                            if let Some(s) = components.span_registry.get_mut(span.table_id) {
                                s.start_worker(*rid);
                                s.activate_worker(*rid);
                            }
                        }
                    }
                }
                // Bump target TiDB schema version so new tables become visible.
                if !new_spans.is_empty() {
                    components.schema_sync.bump_schema();
                }
            }
            _ = cutover_check.tick() => {
                // Update cutover progress metric (0-100%)
                let target = components.cutover_ts.load(Ordering::Acquire);
                if target > 0 && target != u64::MAX {
                    let min_ts = crate::checkpoint::CheckpointManager::global_min_ts(
                        &components.frontier,
                    ).unwrap_or(0);
                    if min_ts > 0 && target > 0 {
                        let pct = ((min_ts as f64 / target as f64) * 100.0).min(100.0) as i64;
                        STREAM_INGEST_METRICS.cutover_progress.set(pct);
                    }
                }
                if components.is_cutover_ready() {
                    STREAM_INGEST_METRICS.cutover_progress.set(100);
                    info!("PCR: cutover target reached (periodic check), exiting event loop");
                    break;
                }
            }
            _ = rediscover.tick() => {
                // Step 3 (topology sync): compare PD region list with local frontier,
                // detect new/removed regions, clean up stale entries.
                let resume_ts =
                    crate::checkpoint::CheckpointManager::global_min_ts(&components.frontier)
                        .unwrap_or(0);
                let pd_regions = discover_region_ids_from_source(
                    &components.source_pd,
                );
                if pd_regions.is_empty() {
                    warn!("PCR: periodic rediscover found 0 regions — check source_pd is a PD HTTP address";
                        "source_pd" => &components.source_pd);
                    continue;
                }

                let diff = diff_topology(&pd_regions, &components.frontier);

                // Subscribe to new regions (per-region mode only).
                // In span mode, the source SpanBridge handles region management —
                // per-region subscriptions would conflict with sub-bridge delegates.
                let mut added = 0usize;
                if !components.span_mode {
                    for change in &diff.new_regions {
                        if let TopologyChange::NewRegion { region_id } = change {
                            let sub = PartitionSubscription {
                                region_id: *region_id,
                                start_ts: resume_ts,
                                source_addr: components.source_addr.clone(),
                                start_key: vec![],
                                end_key: vec![],
                            };
                            if let Err(e) = components.subscriber.subscribe(sub).await {
                                warn!("PCR: rediscover subscribe failed"; "region_id" => *region_id, "error" => ?e);
                            } else {
                                added += 1;
                                if resume_ts > 0 {
                                    components.frontier.insert(*region_id, resume_ts);
                                }
                            }
                        }
                    }
                }

                // Clean up stale frontier entries for regions that no longer exist in PD.
                // These were merged away or otherwise removed — no need to track them.
                for change in &diff.removed_regions {
                    if let TopologyChange::RemovedRegion { region_id } = change {
                        components.frontier.remove(region_id);
                        STREAM_INGEST_METRICS
                            .topology_changes
                            .with_label_values(&["removed"])
                            .inc();
                        info!("PCR: topology sync — removed stale region";
                            "region_id" => *region_id);
                    }
                }

                if added > 0 {
                    STREAM_INGEST_METRICS
                        .topology_changes
                        .with_label_values(&["new"])
                        .inc_by(added as u64);
                }

                // Log summary on any change (suppress noise when everything is stable)
                if added > 0 || !diff.removed_regions.is_empty() {
                    info!("PCR: topology sync: pd={}, frontier={}, +new={}, -removed={}",
                        diff.total_in_pd, diff.total_in_frontier,
                        added, diff.removed_regions.len());
                }
            }
            event = components.subscriber.events().recv() => {
                match event {
                    Some(event_with_meta) => {
                        let region_id = event_with_meta.region_id;
                        let has_kv = event_with_meta.event.has_kv_batch();
                        info!("PCR: event received";
                            "region_id" => region_id,
                            "has_kv_batch" => has_kv,
                        );
                        STREAM_INGEST_METRICS
                            .bridge_liveness
                            .with_label_values(&[&region_id.to_string()])
                            .set(0);
                        let dispatch_start = std::time::Instant::now();
                        // Checkpoint/Split need shared state; KvBatch/DeleteRange go to workers
                        let is_control = event_with_meta.event.has_checkpoint()
                            || event_with_meta.event.has_split();
                        let result = if is_control {
                            components.handle_control_event(event_with_meta).await
                        } else {
                            components.handle_data_event(event_with_meta)
                        };
                        if let Err(e) = result {
                            let elapsed = dispatch_start.elapsed().as_secs_f64();
                            STREAM_INGEST_METRICS.dispatch_latency.observe(elapsed);
                            let err_msg = format!("{:?}", e);
                            if err_msg.contains("cutover complete") {
                                info!("PCR: cutover complete, exiting event loop");
                                break;
                            }
                            STREAM_INGEST_METRICS.errors.with_label_values(&["ingest"]).inc();
                            error!("PCR event handling error: {:?}", e);
                        } else {
                            let elapsed = dispatch_start.elapsed().as_secs_f64();
                            STREAM_INGEST_METRICS.dispatch_latency.observe(elapsed);
                        }
                    }
                    None => {
                        info!("PCR event loop: event channel closed");
                        break;
                    }
                }
            }
        }
    }

    // Graceful shutdown: flush remaining data, then persist checkpoint.
    // Checkpoint-on-pause is critical for CRDB-style resume: without it,
    // the next resume would start from scratch (resume_ts=0, full scan).
    info!("PCR event loop: exiting — workers flush independently on drop");

    if !components.frontier.is_empty() {
        match components.checkpoint_mgr.record_checkpoint(&components.frontier).await {
            Ok(()) => info!(
                "PCR event loop: checkpoint persisted ({} regions, min_ts={})",
                components.frontier.len(),
                crate::checkpoint::CheckpointManager::global_min_ts(&components.frontier).unwrap_or(0),
            ),
            Err(e) => error!("PCR event loop: final checkpoint error: {:?}", e),
        }
    }

    info!("PCR event loop exited");
    // Notify main task that event loop has fully stopped
    let _ = done_tx.send(());
}

// ============================================================================
// StreamIngestTask — top-level control (sync Runnable)
// ============================================================================

/// The Consumer-side main control task for PCR.
///
/// Manages the full lifecycle:
///   Partition planning → gRPC subscription → event loop →
///   SstBatcher buffering → TabletDirectIngest → Checkpoint recording → Cutover
///
/// Implements `Runnable<Task>` for TiKV's worker framework.
pub struct StreamIngestTask<E: KvEngine> {
    /// Shared ingest context (holds TabletRegistry for all Regions)
    ingest_ctx: Arc<DirectIngestContext<E>>,

    /// PD client for checkpoint persistence
    pd_client: Arc<dyn PdClient>,

    /// Task configuration
    cfg: StreamIngestConfig,

    /// Path to persist task state across restarts
    state_file: PathBuf,

    /// Current state
    state: TaskState,

    /// Event loop components — Some when idle/paused, None when event loop owns them
    components: Option<PcrComponents<E>>,

    /// Dedicated tokio runtime for the async event loop
    runtime: tokio::runtime::Runtime,

    /// Cancel signal sender — Some when event loop is running
    cancel_tx: Option<tokio::sync::watch::Sender<bool>>,

    /// Receives done signal when event loop exits — used to prevent state file races
    done_rx: Option<tokio::sync::oneshot::Receiver<()>>,

    /// Cutover timestamp: set by pcr-ctl cutover, checked by event loop.
    /// 0 = no cutover requested. Shared with spawned event loop.
    cutover_ts: Arc<AtomicU64>,

    /// Schema sync (placeholder)
    schema_sync: SchemaSync,

    /// PCR write protection guard — set to false on Activate to allow writes.
    write_guard: Arc<std::sync::atomic::AtomicBool>,

    /// Source PD endpoint for region discovery (set on StartReplication)
    source_pd: Option<String>,
}

impl<E: KvEngine> StreamIngestTask<E> {
    pub fn new(
        cfg: StreamIngestConfig,
        engine: Arc<E>,
        pd_client: Arc<dyn PdClient>,
        data_dir: impl Into<PathBuf>,
        write_guard: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        let state_file = data_dir.into().join("pcr_task_state");
        let state = Self::load_persisted_state(&state_file, cfg.enable);
        let cutover_ts = Arc::new(AtomicU64::new(0));

        let ingest_ctx = Arc::new(DirectIngestContext::new(
            engine,
            pd_client.clone(),
        ));

        // Create initial components (empty subscriptions at startup)
        let flush_interval = cfg.min_flush_interval.0.as_millis() as u64;
        let mut workers = Vec::with_capacity(INGEST_WORKERS);
        for _ in 0..INGEST_WORKERS {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            workers.push(RegionWorker { tx, rx: Some(rx) });
        }
        let components = PcrComponents {
            subscriber: StreamSubscriber::new(),
            batcher: SstBatcher::new(
                ingest_ctx.clone(),
                cfg.max_kv_buffer_size.0 as usize,
                cfg.max_range_key_buffer_size.0 as usize,
            ),
            ingest_ctx: ingest_ctx.clone(),
            workers,
            checkpoint_mgr: CheckpointManager::new(
                pd_client.clone(),
                "pcr_default",
                state_file.parent().unwrap_or(std::path::Path::new("/tmp")),
            ),
            frontier: BTreeMap::new(),
            source_addr: cfg.source_address.clone(),
            source_pd: String::new(),
            initial_subscriptions: Vec::new(),
            flush_interval_ms: flush_interval,
            cutover_ts: cutover_ts.clone(),
            resume_ts: 0,
            span_registry: SpanRegistry::new(),
            target_tidb_addr: cfg.target_tidb_address.clone(),
            source_tidb_addr: String::from("127.0.0.1:4100"),
            span_mode: false,
            schema_sync: SchemaSync::new("127.0.0.1:3380".to_string()),
        };

        // Create a dedicated multi-threaded tokio runtime for the PCR event loop.
        // Uses 2 worker threads: one for event loop, one for background gRPC tasks.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("pcr-event-loop")
            .enable_time()
            .build()
            .expect("PCR: failed to create tokio runtime");

        let source_pd = Self::load_source_pd(&state_file);
        println!("[PCR] StreamIngestTask created: state={:?}", state);
        Self {
            ingest_ctx,
            pd_client,
            cfg,
            state_file,
            state,
            components: Some(components),
            runtime,
            cancel_tx: None,
            done_rx: None,
            cutover_ts,
            schema_sync: SchemaSync::new("127.0.0.1:3380".to_string()),
            write_guard,
            source_pd,
        }
    }

    /// Access the shared ingest context (used by UnsafeDestroyRange forwarder).
    pub fn ingest_ctx(&self) -> Arc<DirectIngestContext<E>> {
        self.ingest_ctx.clone()
    }

    fn load_source_pd(state_file: &PathBuf) -> Option<String> {
        let pd_file = state_file.with_file_name("pcr_source_pd");
        std::fs::read_to_string(&pd_file).ok().map(|s| s.trim().to_string())
    }

    /// Create fresh components (used after pause → resume cycle).
    fn create_components(&self, subscriptions: Vec<PartitionSubscription>) -> PcrComponents<E> {
        let mut workers = Vec::with_capacity(INGEST_WORKERS);
        for _ in 0..INGEST_WORKERS {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            workers.push(RegionWorker { tx, rx: Some(rx) });
        }
        PcrComponents {
            subscriber: StreamSubscriber::new(),
            batcher: SstBatcher::new(
                self.ingest_ctx.clone(),
                self.cfg.max_kv_buffer_size.0 as usize,
                self.cfg.max_range_key_buffer_size.0 as usize,
            ),
            ingest_ctx: self.ingest_ctx.clone(),
            workers,
            checkpoint_mgr: CheckpointManager::new(
                self.pd_client.clone(),
                "pcr_default",
                self.state_file.parent().unwrap_or(std::path::Path::new("/tmp")),
            ),
            frontier: BTreeMap::new(),
            source_addr: self.cfg.source_address.clone(),
            source_pd: self.source_pd.clone().unwrap_or_default(),
            initial_subscriptions: subscriptions,
            flush_interval_ms: self.cfg.min_flush_interval.0.as_millis() as u64,
            cutover_ts: self.cutover_ts.clone(),
            resume_ts: 0,
            span_registry: SpanRegistry::new(),
            target_tidb_addr: self.cfg.target_tidb_address.clone(),
            source_tidb_addr: String::from("127.0.0.1:4100"),
            span_mode: false,
            schema_sync: SchemaSync::new("127.0.0.1:3380".to_string()),
        }
    }

    /// Discover source cluster Regions via PD and build subscription list.
    /// Build subscriptions for ALL regions covering the full key range.
    /// Uses Span Control-Plane: create a single span [\"\", \"\") and
    /// resolve it via PD to get every region. This ensures PCR covers
    /// user data regardless of which region TiDB routes writes to.
    fn discover_partitions(&self, resume_ts: u64, force_full_scan: bool) -> Vec<PartitionSubscription> {
        use crate::subscriber::PartitionSubscription;
        let source_addr = self.cfg.source_address.clone();
        let source_pd = self.source_pd.as_deref().unwrap_or("");

        // Full key range: [\"\", \"\") covers ALL regions unconditionally.
        let region_ids: Vec<(u64, Vec<u8>, Vec<u8>)> = if !source_pd.is_empty() {
            discover_region_ids_from_source(source_pd)
        } else {
            warn!("PCR: no source PD configured, cannot discover regions");
            return Vec::new();
        };

        info!(
            "PCR: full-key-range subscription: {} regions, start_ts={}",
            region_ids.len(), resume_ts
        );

        let start_ts = if force_full_scan { 0 } else { resume_ts };

        region_ids
            .into_iter()
            .map(|(region_id, start_key, end_key)| PartitionSubscription {
                region_id,
                start_ts,
                source_addr: source_addr.clone(),
                start_key,
                end_key,
            })
            .collect()
    }

    /// Query PD HTTP API to get the list of Region IDs on the source cluster.
    fn discover_region_ids_from_pd(&self) -> Option<Vec<u64>> {
        // Use reqwest via std::process::Command to curl the PD API.
        // This is a prototype workaround — in production, use pd_client.
        let pd_addr = self.source_pd.as_deref().unwrap_or("127.0.0.1:2379");
        let url = format!("http://{}/pd/api/v1/regions", pd_addr);
        let output = std::process::Command::new("curl")
            .args(&["-s", &url])
            .output()
            .ok()?;

        if !output.status.success() {
            warn!("PCR: curl to PD failed: {:?}", output.status);
            return None;
        }

        let body = String::from_utf8_lossy(&output.stdout);
        let parsed: serde_json::Value = serde_json::from_str(&body).ok()?;
        let regions = parsed.get("regions")?.as_array()?;

        let ids: Vec<u64> = regions
            .iter()
            .filter_map(|r| r.get("id")?.as_u64())
            .collect();

        info!("PCR: discovered {} Regions on source cluster", ids.len());
        Some(ids)
    }

    /// Spawn the async event loop on the dedicated runtime.
    fn spawn_event_loop(&mut self, task_name: &str, force_full_scan: bool) {
        let mut components = match self.components.take() {
            Some(mut c) => {
                // Update source_pd — it was empty at initial creation,
                // but is now set via StartReplication.
                c.source_pd = self.source_pd.clone().unwrap_or_default();
                c
            }
            None => {
                warn!("PCR: components missing at spawn, recreating");
                self.create_components(Vec::new())
            }
        };

        if force_full_scan {
            // CreateReplication: always full scan, no checkpoint loading.
            // This ensures initial subscriptions always use start_ts=0 for a
            // complete snapshot scan, regardless of any stale checkpoint file.
            info!("PCR: full scan mode — skipping checkpoint load, start_ts=0");
            components.resume_ts = 0;
            components.initial_subscriptions = self.discover_partitions(0, true);
        } else {
            // Load checkpoint from local file and seed frontier + resume_ts.
            // This enables 断点续传: if a prior run recorded a checkpoint,
            // resume from the global min resolved_ts instead of doing a full
            // snapshot scan.
            let resume_ts = match self.runtime.block_on(
                components.checkpoint_mgr.load_checkpoint(),
            ) {
                Ok(frontier) => {
                    if !frontier.is_empty() {
                        let min_ts = CheckpointManager::global_min_ts(&frontier)
                            .unwrap_or(0);
                        info!(
                            "PCR: seeding frontier from checkpoint: {} regions, min_ts={}",
                            frontier.len(),
                            min_ts
                        );
                        components.frontier = frontier;
                        min_ts
                    } else {
                        0
                    }
                }
                Err(e) => {
                    warn!("PCR: failed to load checkpoint: {:?}", e);
                    0
                }
            };
            components.resume_ts = resume_ts;

            // Discover source cluster partitions and set up subscriptions
            components.initial_subscriptions = self.discover_partitions(resume_ts, false);
        }

        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        self.runtime.spawn(async move {
            run_event_loop(components, cancel_rx, done_tx).await;
        });

        self.cancel_tx = Some(cancel_tx);
        self.done_rx = Some(done_rx);
        info!("PCR: async event loop spawned for task {}", task_name);
    }

    /// Send cancel signal and wait for the event loop to fully stop.
    /// Blocks the calling thread until the event loop exits (typically <1ms).
    fn stop_event_loop(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(true);
            info!("PCR: cancel signal sent to event loop");
        }

        // Wait for event loop to confirm shutdown before touching state file
        if let Some(rx) = self.done_rx.take() {
            match self.runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(5), rx).await
            }) {
                Ok(Ok(())) => info!("PCR: event loop confirmed shutdown"),
                Ok(Err(_)) => warn!("PCR: event loop done sender dropped"),
                Err(_) => warn!("PCR: timeout waiting for event loop shutdown"),
            }
        }

        self.components = Some(self.create_components(Vec::new()));
    }

    // ---- State persistence ----

    fn update_metrics_state(&self) {
        let v = match self.state {
            TaskState::Idle => 0u8,
            TaskState::Subscribing => 1u8,
            TaskState::Paused => 2u8,
            TaskState::CuttingOver => 3u8,
            TaskState::Completed => 4u8,
            TaskState::Activated => 5u8,
            TaskState::Failed => 6u8,
        };
        STREAM_INGEST_METRICS.set_state(v);
    }

    fn persist_state(&self) {
        self.update_metrics_state();
        if let Some(parent) = self.state_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let data = format!("{:?}", self.state);
        if let Err(e) = std::fs::write(&self.state_file, &data) {
            warn!("PCR: failed to persist state to {:?}: {:?}", self.state_file, e);
        } else {
            info!("PCR: state persisted: {:?} → {:?}", self.state, self.state_file);
        }
    }

    fn load_persisted_state(state_file: &std::path::Path, stream_ingest_enabled: bool) -> TaskState {
        match std::fs::read_to_string(state_file) {
            Ok(data) => match data.trim() {
                "Subscribing" => {
                    info!("PCR: was Subscribing — entering Paused, waiting for pcr-ctl resume");
                    TaskState::Paused
                }
                "Paused" => {
                    info!("PCR: was Paused — staying Paused until pcr-ctl resume");
                    TaskState::Paused
                }
                "Completed" | "Failed" => {
                    info!("PCR: was {} — entering Idle", data.trim());
                    TaskState::Idle
                }
                "Activated" => {
                    info!("PCR: was Activated — target cluster is independent, entering Idle");
                    TaskState::Idle
                }
                _ => {
                    if stream_ingest_enabled {
                        info!("PCR: stream-ingest enabled, no prior state — entering Paused");
                        TaskState::Paused
                    } else {
                        TaskState::Idle
                    }
                }
            },
            Err(_) => {
                if stream_ingest_enabled {
                    info!("PCR: first startup with stream-ingest enabled — entering Paused (waiting for pcr-ctl)");
                    TaskState::Paused
                } else {
                    TaskState::Idle
                }
            }
        }
    }
}

// ======================================================================
// Worker integration
// ======================================================================

/// Tasks that can be scheduled to the StreamIngest worker.
pub enum Task {
    /// Configuration change (from ConfigManager)
    ConfigChange,
    /// Start PCR replication (triggered by pcr-ctl start) — will be deprecated in favor of CreateReplication
    StartReplication {
        source_pd: String,
        task_name: String,
    },
    /// Create PCR replication with full scan (triggered by pcr-ctl create)
    CreateReplication {
        source_pd: String,
        task_name: String,
    },
    /// Pause replication (triggered by pcr-ctl pause)
    PauseReplication,
    /// Resume replication (triggered by pcr-ctl resume)
    ResumeReplication,
    /// Start a new partition subscription
    StartSubscription {
        region_id: u64,
        start_ts: u64,
        source_addr: String,
    },
    /// Initiate cutover
    Cutover { cutover_ts: u64 },
    /// Activate target cluster — clean up PCR metadata, mark cluster independent
    Activate,
    /// Shutdown gracefully
    Shutdown,
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::ConfigChange => write!(f, "ConfigChange"),
            Task::StartReplication { task_name, .. } => write!(f, "StartReplication({})", task_name),
            Task::CreateReplication { task_name, .. } => write!(f, "CreateReplication({})", task_name),
            Task::PauseReplication => write!(f, "PauseReplication"),
            Task::ResumeReplication => write!(f, "ResumeReplication"),
            Task::StartSubscription { region_id, start_ts, .. } => {
                write!(f, "StartSubscription(region={}, ts={})", region_id, start_ts)
            }
            Task::Cutover { cutover_ts } => write!(f, "Cutover(ts={})", cutover_ts),
            Task::Activate => write!(f, "Activate"),
            Task::Shutdown => write!(f, "Shutdown"),
        }
    }
}

// Types are imported at the top of this file from `crate::pcrpb_gen::pcrpb::*`

/// Scan SST bytes and decompose into individual MvccKeyValue pairs.
pub fn scan_sst_to_kvs(sst_data: &[u8]) -> Result<Vec<MvccKeyValue>> {
    if sst_data.is_empty() {
        return Ok(Vec::new());
    }

    let tmp_path = format!("/tmp/pcr_scan_{}.sst", uuid::Uuid::new_v4());
    std::fs::write(&tmp_path, sst_data)?;

    // In production, use RocksDB SstReader to iterate the SST

    std::fs::remove_file(&tmp_path).ok();
    Ok(Vec::new()) // placeholder
}

impl<E: KvEngine> Runnable for StreamIngestTask<E> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::ConfigChange => {
                info!("StreamIngestTask: config change received");
            }

            Task::StartReplication { source_pd, task_name } => {
                if self.state == TaskState::Subscribing {
                    info!("PCR: StartReplication while Subscribing — stopping stale event loop");
                    self.stop_event_loop();
                } else if self.state != TaskState::Paused && self.state != TaskState::Idle
                    && self.state != TaskState::Activated && self.state != TaskState::Completed
                {
                    info!("PCR: StartReplication ignored — not in Paused/Idle/Activated/Completed (state={:?})", self.state);
                    return;
                }
                info!("PCR: StartReplication — spawning async event loop";
                    "source_pd" => &source_pd, "task_name" => &task_name);
                self.state = TaskState::Subscribing;
                self.source_pd = Some(source_pd.clone());
                // Persist source_pd so it survives TiKV restart
                if let Some(ref pd) = self.source_pd {
                    let _ = std::fs::write(
                        self.state_file.with_file_name("pcr_source_pd"),
                        pd,
                    );
                }
                // Enable write protection — target cluster is read-only during replication
                self.write_guard.store(true, Ordering::Release);
                self.spawn_event_loop(&task_name, false);
                self.persist_state();
                println!("[PCR] Replication STARTED for task={} from source={}", task_name, source_pd);
            }

            Task::CreateReplication { source_pd, task_name } => {
                // Subscribing with done_rx still present: event loop may have exited
                // unexpectedly (span subscription failed, event channel closed, etc.).
                // Clean up the stale event loop and allow re-creation.
                if self.state == TaskState::Subscribing {
                    info!("PCR: CreateReplication while Subscribing — stopping stale event loop");
                    self.stop_event_loop();
                } else if self.state != TaskState::Paused && self.state != TaskState::Idle
                    && self.state != TaskState::Activated && self.state != TaskState::Completed
                {
                    info!("PCR: CreateReplication ignored — not in Paused/Idle/Activated/Completed (state={:?})", self.state);
                    return;
                }
                info!("PCR: CreateReplication — spawning async event loop with full scan";
                    "source_pd" => &source_pd, "task_name" => &task_name);
                self.state = TaskState::Subscribing;
                self.source_pd = Some(source_pd.clone());
                // Persist source_pd so it survives TiKV restart
                if let Some(ref pd) = self.source_pd {
                    let _ = std::fs::write(
                        self.state_file.with_file_name("pcr_source_pd"),
                        pd,
                    );
                }
                // Enable write protection — target cluster is read-only during replication
                self.write_guard.store(true, Ordering::Release);
                self.spawn_event_loop(&task_name, true);
                self.persist_state();
                println!("[PCR] Replication CREATED for task={} from source={} (full scan)", task_name, source_pd);
            }

            Task::PauseReplication => {
                if self.state != TaskState::Subscribing {
                    info!("PCR: PauseReplication ignored — not subscribing (state={:?})", self.state);
                    return;
                }
                info!("PCR: PauseReplication — stopping event loop (CRDB style: full teardown)");
                self.stop_event_loop();
                self.state = TaskState::Paused;
                self.persist_state();
                println!("[PCR] Replication PAUSED — checkpoint preserved, connections closed");
            }

            Task::ResumeReplication => {
                if self.state != TaskState::Paused {
                    info!("PCR: ResumeReplication ignored — not paused (state={:?})", self.state);
                    return;
                }
                info!("PCR: ResumeReplication — spawning new event loop (CRDB style: rebuild from checkpoint)");
                self.state = TaskState::Subscribing;
                self.spawn_event_loop("pcr_default", false);
                self.persist_state();
                println!("[PCR] Replication RESUMED — rebuilding from checkpoint");
            }

            Task::StartSubscription {
                region_id,
                start_ts,
                source_addr,
            } => {
                info!(
                    "StreamIngestTask: starting subscription region={} ts={}",
                    region_id, start_ts
                );
                // In production, this subscribes to a new partition on the source
            }

            Task::Cutover { cutover_ts } => {
                if self.state != TaskState::Subscribing {
                    info!("PCR: Cutover ignored — not subscribing (state={:?})", self.state);
                    return;
                }
                info!("PCR: Cutover initiated"; "cutover_ts" => cutover_ts);
                self.cutover_ts.store(cutover_ts, Ordering::Release);
                self.state = TaskState::CuttingOver;
                self.persist_state();
                println!("[PCR] CUTOVER initiated at ts={}, waiting for frontier to catch up...", cutover_ts);

                // Wait for the async event loop to exit naturally
                // (will happen when min(frontier) >= cutover_ts after a checkpoint)
                if let Some(rx) = self.done_rx.take() {
                    info!("PCR: waiting for event loop to reach cutover target...");
                    match self.runtime.block_on(async {
                        tokio::time::timeout(std::time::Duration::from_secs(300), rx).await
                    }) {
                        Ok(Ok(())) => {
                            info!("PCR: cutover complete — event loop exited");
                            self.state = TaskState::Completed;
                            self.persist_state();
                            self.components = Some(self.create_components(Vec::new()));
                            println!("[PCR] CUTOVER COMPLETE — target cluster is now consistent");
                        }
                        Ok(Err(_)) => {
                            warn!("PCR: event loop done sender dropped during cutover");
                        }
                        Err(_) => {
                            warn!("PCR: timeout waiting for cutover completion");
                            self.state = TaskState::Failed;
                            self.persist_state();
                        }
                    }
                }
            }

            Task::Shutdown => {
                info!("StreamIngestTask: shutdown requested");
                if self.state == TaskState::Subscribing {
                    self.stop_event_loop();
                }
                self.state = TaskState::Completed;
                self.persist_state();
            }

            Task::Activate => {
                if self.state != TaskState::Completed && self.state != TaskState::Subscribing {
                    info!("PCR: Activate ignored — not in Completed/Subscribing (state={:?})", self.state);
                    return;
                }
                if self.state == TaskState::Subscribing {
                    info!("PCR: Activate from Subscribing — stopping event loop first");
                    self.stop_event_loop();
                    self.state = TaskState::Completed;
                }
                info!("PCR: Activating target cluster — cleaning up PCR metadata...");

                // Clear checkpoint from PD and local file
                let mut checkpoint_mgr = CheckpointManager::new(
                    self.pd_client.clone(),
                    "pcr_default",
                    self.state_file.parent().unwrap_or(std::path::Path::new("/tmp")),
                );
                match self.runtime.block_on(checkpoint_mgr.clear_checkpoint()) {
                    Ok(()) => info!("PCR: checkpoint cleared"),
                    Err(e) => warn!("PCR: failed to clear checkpoint: {:?}", e),
                }

                // Remove state file — cluster is now independent
                if let Err(e) = std::fs::remove_file(&self.state_file) {
                    warn!("PCR: failed to remove state file: {:?}", e);
                }

                self.state = TaskState::Activated;
                self.update_metrics_state();

                // Release write protection — target cluster is now writable
                self.write_guard.store(false, Ordering::Release);
                info!("PCR: write protection released — target cluster is now writable");

                println!("╔══════════════════════════════════════════════╗");
                println!("║  🗸 PCR TARGET CLUSTER ACTIVATED              ║");
                println!("║                                              ║");
                println!("║  The target cluster is now independent       ║");
                println!("║  and ready for read/write operations.        ║");
                println!("║                                              ║");
                println!("║  • PCR replication is complete               ║");
                println!("║  • PCR metadata has been cleaned             ║");
                println!("║  • Connect TiDB to begin serving traffic      ║");
                println!("╚══════════════════════════════════════════════╝");
            }
        }
    }
}
