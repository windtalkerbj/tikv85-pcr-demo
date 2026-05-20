// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref STREAM_INGEST_METRICS: StreamIngestMetrics =
        StreamIngestMetrics::default();
}

#[derive(Clone)]
pub struct StreamIngestMetrics {
    // Consumer-side
    pub ingested_bytes: IntCounter,
    pub ingested_kvs: IntCounter,
    pub ingested_ssts: IntCounter,
    pub flush_count: IntCounter,
    pub flush_latency: Histogram,
    pub buffer_size: IntGauge,
    pub active_subscriptions: IntGauge,
    pub checkpoint_lag: IntGauge,
    pub frontier_min_ts: IntGauge,
    pub state: IntGauge,
    // Producer-side
    pub bridge_bytes_sent: IntCounter,
    pub snapshot_regions_scanned: IntGauge,
    pub source_batcher_bytes: IntGauge,
    pub cdc_events_generated: IntCounter,
    // Cutover progress: 0-100 (percentage)
    pub cutover_progress: IntGauge,
    // Per-region checkpoint lag
    pub region_lag: IntGaugeVec,
    // P0: error counters by type
    pub errors: IntCounterVec,
    // P0: reconnect tracking per region
    pub reconnects: IntCounterVec,
    // P0: bridge liveness per region (seconds since last event)
    pub bridge_liveness: IntGaugeVec,
    // P1: event dispatch latency (channel recv → handle_event complete)
    pub dispatch_latency: Histogram,
    // P1: per-CF ingest counters
    pub cf_ingested_bytes: IntCounterVec,
    pub cf_ingested_kvs: IntCounterVec,
    // Step 3: topology change counters (new regions discovered, removed regions cleaned)
    pub topology_changes: IntCounterVec,
    // P1: SQL-based data consistency verification (1=consistent, 0=inconsistent, -1=not run)
    pub sql_consistency: IntGauge,
    // P1: SST phased latency (sort / generate / ingest)
    pub sst_sort_latency: Histogram,
    pub sst_generate_latency: Histogram,
    pub sst_ingest_latency: Histogram,
}

impl Default for StreamIngestMetrics {
    fn default() -> Self {
        Self {
            ingested_bytes: register_int_counter!(
                "pcr_ingested_bytes_total",
                "Total bytes ingested via PCR"
            )
            .unwrap(),
            ingested_kvs: register_int_counter!(
                "pcr_ingested_kvs_total",
                "Total KV pairs ingested via PCR"
            )
            .unwrap(),
            ingested_ssts: register_int_counter!(
                "pcr_ingested_ssts_total",
                "Total SST files ingested via PCR"
            )
            .unwrap(),
            flush_count: register_int_counter!(
                "pcr_flush_count_total",
                "Total number of SstBatcher flushes"
            )
            .unwrap(),
            flush_latency: register_histogram!(
                "pcr_flush_latency_seconds",
                "SstBatcher flush latency in seconds",
                vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
            )
            .unwrap(),
            buffer_size: register_int_gauge!(
                "pcr_buffer_size_bytes",
                "Current SstBatcher KV buffer size in bytes"
            )
            .unwrap(),
            active_subscriptions: register_int_gauge!(
                "pcr_active_subscriptions",
                "Number of active PCR subscriptions"
            )
            .unwrap(),
            checkpoint_lag: register_int_gauge!(
                "pcr_checkpoint_lag_seconds",
                "Lag between current time and latest checkpoint resolved_ts in seconds"
            )
            .unwrap(),
            frontier_min_ts: register_int_gauge!(
                "pcr_frontier_min_ts",
                "Minimum resolved_ts across all tracked regions (physical timestamp >> 18)"
            )
            .unwrap(),
            state: register_int_gauge!(
                "pcr_state",
                "PCR task state: 0=Idle, 1=Subscribing, 2=Paused, 3=CuttingOver, 4=Completed, 5=Activated, 6=Failed"
            )
            .unwrap(),
            // Producer-side metrics
            bridge_bytes_sent: register_int_counter!(
                "pcr_bridge_bytes_sent_total",
                "Total bytes sent via gRPC by PCR bridge tasks (producer side)"
            )
            .unwrap(),
            snapshot_regions_scanned: register_int_gauge!(
                "pcr_snapshot_regions_scanned",
                "Number of regions whose snapshot scan has completed (producer side)"
            )
            .unwrap(),
            source_batcher_bytes: register_int_gauge!(
                "pcr_source_batcher_bytes",
                "Current total bytes across all source-side PCR event batchers (producer side)"
            )
            .unwrap(),
            cdc_events_generated: register_int_counter!(
                "pcr_cdc_events_generated_total",
                "Total CDC events generated for PCR (producer side, not tikv_cdc_*)"
            )
            .unwrap(),
            cutover_progress: register_int_gauge!(
                "pcr_cutover_progress",
                "Cutover progress 0-100: (frontier_min_ts / cutover_ts)*100"
            )
            .unwrap(),
            region_lag: register_int_gauge_vec!(
                "pcr_region_lag_seconds",
                "Per-region checkpoint lag in seconds",
                &["region_id"]
            )
            .unwrap(),
            errors: register_int_counter_vec!(
                "pcr_errors_total",
                "PCR errors by type (subscription, ingest, flush, disconnect, epoch)",
                &["type"]
            )
            .unwrap(),
            reconnects: register_int_counter_vec!(
                "pcr_subscriber_reconnects_total",
                "PCR subscription reconnects per region",
                &["region_id"]
            )
            .unwrap(),
            bridge_liveness: register_int_gauge_vec!(
                "pcr_bridge_liveness_seconds",
                "Seconds since last event from bridge (per region)",
                &["region_id"]
            )
            .unwrap(),
            dispatch_latency: register_histogram!(
                "pcr_dispatch_latency_seconds",
                "Event dispatch latency: channel recv to handle_event complete",
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
            ).unwrap(),
            topology_changes: register_int_counter_vec!(
                "pcr_topology_changes_total",
                "Topology changes detected by periodic rediscover (new, removed)",
                &["type"]
            ).unwrap(),
            cf_ingested_bytes: register_int_counter_vec!(
                "pcr_cf_ingested_bytes_total",
                "Ingested bytes per column family",
                &["cf"]
            ).unwrap(),
            cf_ingested_kvs: register_int_counter_vec!(
                "pcr_cf_ingested_kvs_total",
                "Ingested KVs per column family",
                &["cf"]
            ).unwrap(),
            sql_consistency: register_int_gauge!(
                "pcr_sql_consistency",
                "SQL-based data consistency: 1=consistent, 0=inconsistent, -1=not run"
            ).unwrap(),
            sst_sort_latency: register_histogram!(
                "pcr_sst_sort_latency_seconds",
                "SST generation: key sorting latency",
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
            ).unwrap(),
            sst_generate_latency: register_histogram!(
                "pcr_sst_generate_latency_seconds",
                "SST generation: SST file write latency",
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
            ).unwrap(),
            sst_ingest_latency: register_histogram!(
                "pcr_sst_ingest_latency_seconds",
                "SST generation: RocksDB ingest latency",
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5]
            ).unwrap(),
        }
    }
}

impl StreamIngestMetrics {
    /// Update the state gauge from a TaskState value.
    pub fn set_state(&self, state_val: u8) {
        self.state.set(state_val as i64);
    }
}
