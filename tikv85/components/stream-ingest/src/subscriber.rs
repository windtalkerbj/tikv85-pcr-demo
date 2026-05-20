// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use futures::StreamExt;
use slog_global::{debug, error, info, warn};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::errors::{Error, Result};
use crate::metrics::STREAM_INGEST_METRICS;
use crate::pcrpb_gen::pcrpb::PcrEvent;

/// Subscription metadata for a single partition (source Region).
#[derive(Clone, Debug)]
pub struct PartitionSubscription {
    pub region_id: u64,
    pub start_ts: u64,
    pub source_addr: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

/// A PCR event with associated Region metadata.
#[derive(Debug)]
pub struct PcrEventWithMeta {
    pub event: PcrEvent,
    pub region_id: u64,
}

/// Classification of gRPC stream errors for retry policy decisions.
///
/// Mirrors CRDB's rangefeed error classification: transient disconnects
/// re-resolve the same range, epoch changes trigger span re-resolution.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StreamErrorKind {
    /// Transient network issue or leader change — retry same region.
    Disconnect,
    /// Region epoch mismatch (split/merge) — must re-resolve the span.
    EpochChanged,
    /// Permanent error — stop retrying this subscription.
    Fatal,
}

/// Per-subscription health state for observability.
#[derive(Debug, PartialEq, Clone)]
pub enum SubscriptionState {
    Connected,
    Reconnecting { retry: u32 },
    Failed,
}

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// Manages gRPC subscriptions to multiple source Region partitions,
/// merging their event streams into a single channel consumed by
/// `StreamIngestTask`.
///
/// Uses a single shared gRPC channel (one TCP connection) with HTTP/2
/// multiplexing for all Region subscriptions, scaling to 100K+ Regions.
///
/// Step 2 (CRDB-style span-aware re-subscription): when a stream fails with
/// an epoch error (region split/merge), the subscriber queries PD for the
/// current region(s) covering the span and re-subscribes to them with the
/// original frontier timestamp.
pub struct StreamSubscriber {
    /// Channel sender — cloned to each partition's gRPC stream handler
    sender: UnboundedSender<PcrEventWithMeta>,

    /// Channel receiver — consumed by the main event loop
    receiver: UnboundedReceiver<PcrEventWithMeta>,

    /// Shared gRPC client — one TCP connection for all Region streams
    shared_client: Option<crate::pcrpb_gen::pcrpb_grpc::PcrStreamClient>,

    /// Currently active subscriptions count
    active_count: usize,

    /// Source PD HTTP address for span re-resolution on epoch errors.
    source_pd: Option<String>,

    /// Set of region IDs that already have an active gRPC subscription.
    /// Prevents duplicate streams when DDL discover re-discovers the
    /// same region (10s cycle × all known tables → 1000+ streams).
    subscribed_regions: std::collections::HashSet<u64>,
}

impl StreamSubscriber {
    /// Create a new subscriber with an unbounded channel for event merging.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            sender: tx,
            receiver: rx,
            shared_client: None,
            active_count: 0,
            source_pd: None,
            subscribed_regions: std::collections::HashSet::new(),
        }
    }

    /// Set the shared gRPC client for all subscriptions.
    /// Must be called before any `subscribe()` calls.
    pub fn set_shared_client(
        &mut self,
        client: crate::pcrpb_gen::pcrpb_grpc::PcrStreamClient,
    ) {
        self.shared_client = Some(client);
    }

    /// Set the source PD address for span re-resolution.
    pub fn set_source_pd(&mut self, pd_addr: String) {
        self.source_pd = Some(pd_addr);
    }

    /// Subscribe to a source Region's PCR event stream.
    ///
    /// Uses the shared gRPC client (one TCP connection, HTTP/2 multiplexed).
    /// Spawns a background tokio task that maintains the gRPC stream,
    /// handles reconnection, and forwards events to the merged channel.
    ///
    /// Step 2: on epoch errors, re-resolves the span via PD and creates
    /// new subscriptions for the replacement regions.
    pub async fn subscribe(&mut self, partition: PartitionSubscription) -> Result<()> {
        let region_id = partition.region_id;

        // Dedup: skip if this region already has an active gRPC stream.
        // DDL discover (10s cycle) re-discovers the same system-table regions
        // repeatedly, creating duplicate streams that exhaust grpcio's HTTP/2
        // stream limit (~1000) and silently drop legitimate re-subscriptions
        // after region splits.
        if self.subscribed_regions.contains(&region_id) {
            info!("PCR subscriber: region already subscribed, skipping";
                "region_id" => region_id);
            return Ok(());
        }

        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
        let start_ts = partition.start_ts;
        let sender = self.sender.clone();
        let source_pd = self.source_pd.clone();

        let client = self.shared_client.clone().ok_or_else(|| {
            Error::SubscriptionError("shared gRPC client not set".to_string())
        })?;

        info!(
            "Subscribing to partition: stream_id={}, region={}, start_ts={}",
            stream_id, region_id, start_ts
        );

        // Background task for this partition's gRPC stream
        tokio::spawn(async move {
            let mut retry_count = 0u32;
            let max_retries = 5;
            let mut state = SubscriptionState::Connected;
            // Track the current region_id — may change after span re-resolution.
            let mut current_region_id = region_id;

            loop {
                if sender.is_closed() {
                    info!(
                        "Partition stream {} stopped: merged channel closed, region={}",
                        stream_id, current_region_id
                    );
                    break;
                }

                let start_key = partition.start_key.clone();
                let end_key = partition.end_key.clone();
                let result = connect_and_stream(
                    &client,
                    current_region_id,
                    start_ts,
                    start_key.clone(),
                    end_key.clone(),
                    sender.clone(),
                )
                .await;

                match result {
                    Ok(()) => {
                        info!(
                            "Partition stream {} completed normally: region={}",
                            stream_id, current_region_id
                        );
                        break;
                    }
                    Err(e) => {
                        let kind = classify_error(&e);

                        match kind {
                            StreamErrorKind::Disconnect => {
                                // Leader change or transient network issue —
                                // retry the same region with backoff.
                                state = SubscriptionState::Reconnecting { retry: retry_count + 1 };
                                STREAM_INGEST_METRICS.errors.with_label_values(&["disconnect"]).inc();

                                if retry_count >= max_retries {
                                    error!(
                                        "Partition stream {} failed after {} retries: region={}",
                                        stream_id, retry_count, current_region_id
                                    );
                                    state = SubscriptionState::Failed;
                                    break;
                                }
                                retry_count += 1;
                                STREAM_INGEST_METRICS
                                    .reconnects
                                    .with_label_values(&[&current_region_id.to_string()])
                                    .inc();
                                warn!(
                                    "Partition stream {} reconnecting ({}/{}): region={}",
                                    stream_id, retry_count, max_retries, current_region_id
                                );
                                tokio::time::sleep(std::time::Duration::from_secs(
                                    2u64.pow(retry_count.min(5)),
                                ))
                                .await;
                            }

                            StreamErrorKind::EpochChanged => {
                                // Region split or merged — the old region no longer
                                // covers this span. Re-resolve via PD and subscribe
                                // to the replacement regions.
                                STREAM_INGEST_METRICS.errors.with_label_values(&["epoch"]).inc();
                                warn!(
                                    "Partition stream {} epoch changed: region={}, re-resolving span [{:?}, {:?})",
                                    stream_id, current_region_id,
                                    partition.start_key.len(),
                                    partition.end_key.len(),
                                );

                                if let Some(ref pd) = source_pd {
                                    match resolve_span(pd, &partition.start_key, &partition.end_key).await {
                                        Ok(new_regions) => {
                                            let new_count = new_regions.len();
                                            info!(
                                                "Partition stream {} span resolved: {} new region(s) for [{:?}, {:?})",
                                                stream_id, new_count,
                                                partition.start_key.first(),
                                                partition.end_key.first(),
                                            );
                                            for (new_rid, new_start, new_end) in new_regions {
                                                if new_rid == current_region_id {
                                                    // Same region — epoch was stale, retry
                                                    current_region_id = new_rid;
                                                    continue;
                                                }
                                                // Subscribe to the replacement region
                                                // with the same start_ts (parent frontier)
                                                let sub = PartitionSubscription {
                                                    region_id: new_rid,
                                                    start_ts,
                                                    source_addr: String::new(),
                                                    start_key: new_start,
                                                    end_key: new_end,
                                                };
                                                let sub_sender = sender.clone();
                                                let sub_client = client.clone();
                                                let sub_stream_id =
                                                    NEXT_STREAM_ID.fetch_add(1, Ordering::SeqCst);
                                                info!(
                                                    "Partition stream {}: spawning replacement stream {} for region {}",
                                                    stream_id, sub_stream_id, new_rid
                                                );
                                                // Spawn a new stream for the replacement region
                                                tokio::spawn(async move {
                                                    run_single_stream(
                                                        sub_stream_id,
                                                        &sub_client,
                                                        &sub,
                                                        sub_sender,
                                                        start_ts,
                                                    )
                                                    .await;
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Partition stream {} span resolution failed: {:?}",
                                                stream_id, e
                                            );
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Partition stream {} epoch changed but no source_pd configured — relying on periodic rediscover",
                                        stream_id
                                    );
                                }
                                // Stop the old stream — replacement(s) are active.
                                state = SubscriptionState::Connected;
                                break;
                            }

                            StreamErrorKind::Fatal => {
                                state = SubscriptionState::Failed;
                                STREAM_INGEST_METRICS.errors.with_label_values(&["fatal"]).inc();
                                error!(
                                    "Partition stream {} fatal error: region={}, error={:?}",
                                    stream_id, current_region_id, e
                                );
                                break;
                            }
                        }
                    }
                }
            }

            debug!(
                "Partition stream {} exited: final_state={:?}",
                stream_id, state
            );
        });

        self.subscribed_regions.insert(region_id);
        self.active_count += 1;
        STREAM_INGEST_METRICS
            .active_subscriptions
            .set(self.active_count as i64);

        Ok(())
    }

    /// Subscribe to a ReplicationSpan by resolving it to regions, then
    /// subscribing to each. Already-subscribed regions are skipped.
    /// Returns the region IDs that were newly subscribed.
    /// Subscribe to a key range span. Sends a SINGLE gRPC subscription with
    /// region_id=0 and the key range set. The source side SpanBridge handles
    /// region resolution, split/merge, and event merging transparently.
    /// Returns the gRPC stream_id.
    pub async fn subscribe_span(
        &mut self,
        span_key: &[u8],
        span_end: &[u8],
        start_ts: u64,
        _pd_addr: &str,
    ) -> Result<u64> {
        let stream_id = NEXT_STREAM_ID.fetch_add(1, Ordering::SeqCst);

        let client = self.shared_client.clone().ok_or_else(|| {
            Error::SubscriptionError("shared gRPC client not set".to_string())
        })?;

        let sender = self.sender.clone();
        let sk = span_key.to_vec();
        let se = span_end.to_vec();

        info!(
            "PCR gRPC: subscribing to SPAN on shared channel, stream_id={}, start_ts={}, key_range=[{}, {})",
            stream_id, start_ts, sk.len(), se.len()
        );

        let c = client.clone();
        tokio::spawn(async move {
            connect_and_stream(&c, 0, start_ts, sk, se, sender).await.ok();
        });

        self.active_count += 1;
        STREAM_INGEST_METRICS.active_subscriptions.set(self.active_count as i64);

        Ok(stream_id)
    }

    /// Replace all current subscriptions with subscriptions covering the
    /// given spans. Used during PCR initialization and full topology refresh.
    pub async fn subscribe_all_spans(
        &mut self,
        spans: &[crate::span_ctl::registry::ReplicationSpan],
        start_ts: u64,
        pd_addr: &str,
    ) -> Result<Vec<u64>> {
        let mut all_new = Vec::new();
        for span in spans {
            match self
                .subscribe_span(&span.start_key, &span.end_key, start_ts, pd_addr)
                .await
            {
                Ok(new) => all_new.push(new),
                Err(e) => {
                    warn!(
                        "PCR subscriber: failed to subscribe span table_id={}: {:?}",
                        span.table_id, e
                    );
                }
            }
        }
        info!(
            "PCR subscriber: subscribed {} new regions across {} spans",
            all_new.len(),
            spans.len()
        );
        Ok(all_new)
    }

    /// Remove a region from the dedup set (call after region split/deregister).
    pub fn remove_subscribed(&mut self, region_id: u64) {
        self.subscribed_regions.remove(&region_id);
    }

    /// Get the merged event receiver for the main event loop.
    pub fn events(&mut self) -> &mut UnboundedReceiver<PcrEventWithMeta> {
        &mut self.receiver
    }

    /// Shut down all active subscriptions.
    pub fn shutdown(&mut self) {
        self.active_count = 0;
        self.subscribed_regions.clear();
        STREAM_INGEST_METRICS.active_subscriptions.set(0);
        info!("StreamSubscriber shutdown: all subscriptions closed");
    }
}

/// Run a single subscription stream with retry logic.
/// Used both for initial subscriptions and span-re-resolved replacement streams.
async fn run_single_stream(
    stream_id: u64,
    client: &crate::pcrpb_gen::pcrpb_grpc::PcrStreamClient,
    partition: &PartitionSubscription,
    sender: UnboundedSender<PcrEventWithMeta>,
    start_ts: u64,
) {
    let max_retries = 3u32;
    let mut retry = 0u32;

    loop {
        if sender.is_closed() {
            break;
        }
        match connect_and_stream(
            client,
            partition.region_id,
            start_ts,
            partition.start_key.clone(),
            partition.end_key.clone(),
            sender.clone(),
        )
        .await
        {
            Ok(()) => break,
            Err(e) => {
                let kind = classify_error(&e);
                if kind != StreamErrorKind::Disconnect || retry >= max_retries {
                    warn!(
                        "Replacement stream {} giving up: region={}, kind={:?}, retry={}",
                        stream_id, partition.region_id, kind, retry
                    );
                    break;
                }
                retry += 1;
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(retry))).await;
            }
        }
    }
}

/// Connect to a source TiKV node and stream PCR events via gRPC.
async fn connect_and_stream(
    client: &crate::pcrpb_gen::pcrpb_grpc::PcrStreamClient,
    region_id: u64,
    start_ts: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    sender: UnboundedSender<PcrEventWithMeta>,
) -> Result<()> {
    use crate::pcrpb_gen::pcrpb::PcrPartitionSpec;
    use crate::pcrpb_gen::pcrpb::PcrSubscribeRequest;

    info!(
        "PCR gRPC: subscribing to region {} on shared channel, start_ts={}, key_range_len=[{}, {})",
        region_id, start_ts,
        start_key.len(),
        end_key.len()
    );

    let mut partition = PcrPartitionSpec::new();
    partition.set_region_id(region_id);
    partition.set_start_key(start_key);
    partition.set_end_key(end_key);

    let mut req = PcrSubscribeRequest::new();
    req.set_partition(partition);
    req.set_start_ts(start_ts);

    let rx_result = client.subscribe(&req);
    match &rx_result {
        Ok(_rx) => {
            info!(
                "PCR gRPC: subscription established (gRPC call OK)";
                "region_id" => region_id,
            );
        }
        Err(e) => {
            warn!(
                "PCR gRPC: subscription FAILED";
                "region_id" => region_id,
                "error" => ?e,
            );
        }
    }
    let mut rx = rx_result.map_err(|e| {
        Error::SubscriptionError(format!("Subscribe RPC failed: {:?}", e))
    })?;

    loop {
        match rx.next().await {
            Some(Ok(event)) => {
                if sender.send(PcrEventWithMeta { event, region_id }).is_err() {
                    info!("PCR gRPC: merged channel closed";
                        "region_id" => region_id);
                    break;
                }
            }
            Some(Err(e)) => {
                warn!("PCR gRPC: stream error"; "region_id" => region_id, "error" => ?e);
                return Err(Error::SubscriptionError(format!("Stream error: {:?}", e)));
            }
            None => {
                info!("PCR gRPC: stream ended"; "region_id" => region_id);
                break;
            }
        }
    }

    Ok(())
}

/// Classify a subscription error to determine the retry strategy.
///
/// - `Disconnect`: transient network/leader issues — retry same region
/// - `EpochChanged`: region split/merge — must re-resolve the span
/// - `Fatal`: permanent — give up
pub fn classify_error(err: &Error) -> StreamErrorKind {
    let msg = format!("{:?}", err);
    let msg_lower = msg.to_lowercase();

    if msg_lower.contains("epoch")
        || msg_lower.contains("region not found")
        || msg_lower.contains("stale command")
        || msg_lower.contains("stale_epoch")
    {
        return StreamErrorKind::EpochChanged;
    }

    if msg_lower.contains("rpc failure")
        || msg_lower.contains("unavailable")
        || msg_lower.contains("connection")
        || msg_lower.contains("timeout")
        || msg_lower.contains("transport")
        || msg_lower.contains("disconnect")
        || msg_lower.contains("broken pipe")
    {
        return StreamErrorKind::Disconnect;
    }

    StreamErrorKind::Fatal
}

/// Resolve a key span to current Region IDs by querying the source PD HTTP API.
///
/// Returns a list of `(region_id, start_key, end_key)` for all regions that
/// overlap with the given key range.
pub async fn resolve_span(
    source_pd: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<(u64, Vec<u8>, Vec<u8>)>> {
    // Query PD regions API — returns all regions, filter by key range overlap.
    let url = format!("http://{}/pd/api/v1/regions", source_pd);
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("curl")
            .args(&["-s", &url])
            .output()
    })
    .await
    .map_err(|e| Error::SubscriptionError(format!("spawn_blocking failed: {:?}", e)))?;

    let output = output.map_err(|e| {
        Error::SubscriptionError(format!("curl failed: {:?}", e))
    })?;

    if !output.status.success() {
        return Err(Error::SubscriptionError(format!(
            "PD API returned non-zero: {:?}",
            output.status
        )));
    }

    let body = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
        Error::SubscriptionError(format!("PD JSON parse error: {:?}", e))
    })?;

    let regions = parsed
        .get("regions")
        .and_then(|r| r.as_array())
        .ok_or_else(|| Error::SubscriptionError("PD response missing 'regions'".into()))?;

    let decode_key = |field: &str, obj: &serde_json::Value| -> Vec<u8> {
        obj.get(field)
            .and_then(|v| v.as_str())
            .and_then(|s| crate::task::hex_decode(s).ok())
            .unwrap_or_default()
    };

    let mut results = Vec::new();
    for r in regions {
        let rid = r.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        if rid == 0 {
            continue;
        }
        let r_start = decode_key("start_key", r);
        let r_end = decode_key("end_key", r);

        // Check key range overlap with our span.
        // Region covers [r_start, r_end). Our span is [start_key, end_key).
        // Overlap if: r_start < end_key && start_key < r_end
        let overlaps = if start_key.is_empty() && end_key.is_empty() {
            // No span specified — match all regions
            true
        } else if r_start.is_empty() && r_end.is_empty() {
            // Region with no key range — include it
            true
        } else {
            let before_end = r_end.is_empty() || &r_start[..] < end_key;
            let after_start = start_key.is_empty() || start_key < &r_end[..];
            before_end && after_start
        };

        if overlaps {
            results.push((rid, r_start, r_end));
        }
    }

    if results.is_empty() {
        warn!(
            "resolve_span: no regions found for span [{:?}, {:?}) out of {} total",
            start_key.first(),
            end_key.first(),
            regions.len()
        );
    }

    Ok(results)
}
