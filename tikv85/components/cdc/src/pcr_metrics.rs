// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR Producer-side metrics — independent from stream-ingest.
//! Registered in the cdc crate so they're visible on the source TiKV
//! (which doesn't initialize stream-ingest metrics on its Prometheus endpoint).

use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

#[derive(Clone)]
pub struct PcrProducerMetrics {
    pub bridge_bytes_sent: IntCounter,
    pub snapshot_regions_scanned: IntCounter,
    pub cdc_events_generated: IntCounter,
    pub batcher_buffer_bytes: IntGauge,
    /// Counts how often WriteRef parsing fails in LogicalMutation::from_write_cf,
    /// causing a fallback to raw WRITE CF byte forwarding.
    pub write_ref_parse_fallbacks: IntCounter,
}

lazy_static! {
    pub static ref PCR_PRODUCER_METRICS: PcrProducerMetrics =
        PcrProducerMetrics {
            bridge_bytes_sent: register_int_counter!(
                "pcr_producer_bridge_bytes_sent_total",
                "Total bytes sent via gRPC by PCR bridge tasks (producer side)"
            ).unwrap(),
            snapshot_regions_scanned: register_int_counter!(
                "pcr_producer_snapshot_regions_scanned",
                "Total regions scanned for PCR snapshot (producer side)"
            ).unwrap(),
            cdc_events_generated: register_int_counter!(
                "pcr_producer_cdc_events_generated_total",
                "Total CDC events generated for PCR (producer side)"
            ).unwrap(),
            batcher_buffer_bytes: register_int_gauge!(
                "pcr_producer_batcher_buffer_bytes",
                "PCR event batcher buffer size in bytes (producer side)"
            ).unwrap(),
            write_ref_parse_fallbacks: register_int_counter!(
                "pcr_producer_write_ref_parse_fallbacks_total",
                "Number of times WriteRef parsing failed, falling back to raw WRITE CF forwarding"
            ).unwrap(),
        };
}
