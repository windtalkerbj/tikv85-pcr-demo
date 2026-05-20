// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! # Stream Ingest — PCR Consumer Crate
//!
//! This crate implements the **Consumer side** of Physical Cluster Replication (PCR)
//! for TiKV. It receives MVCC key-value change events from a source TiKV cluster's
//! CDC subsystem via gRPC, buffers and sorts them by key, generates SST files,
//! and ingests them directly into the target TiKV's per-Region RocksDB tablets,
//! bypassing the Raft consensus layer.
//!
//! ## Architecture
//!
//! ```text
//! gRPC Streams → StreamSubscriber → StreamIngestTask → SstBatcher → DirectIngest
//!                   (merge)          (event loop)     (sort+SST)   (RocksDB)
//! ```
//!
//! ## Key Components
//!
//! - [`SstBatcher`]: Buffers MVCC KVs, sorts by key, generates SSTs via RocksDB
//!   `SstFileWriter`, and calls [`direct_ingest::ingest_sst`].
//! - [`direct_ingest::ingest_sst`]: Writes SST files directly into a target Region's
//!   RocksDB tablet via `IngestExternalFile`, completely bypassing Raft.
//! - [`StreamSubscriber`]: Manages gRPC subscriptions to multiple source Region
//!   partitions, merging their event streams.
//! - [`StreamIngestTask`]: Top-level control task implementing TiKV's `Runnable`
//!   trait. Orchestrates the full Consumer lifecycle.
//! - [`CheckpointManager`]: Persists replication progress to PD MetaStore for
//!   crash recovery and cutover safety.
//!
//! ## Configuration
//!
//! ```toml
//! [stream-ingest]
//! enable = true
//! max-kv-buffer-size = "128MB"
//! max-range-key-buffer-size = "32MB"
//! min-flush-interval = "5s"
//! source-address = "source-pd:2379"
//! checkpoint-interval = "10s"
//! ```
//!
//! ## Integration
//!
//! This crate is registered in `server2.rs` similarly to `backup-stream` and
//! `cdc`. It is only initialized when `stream-ingest.enable = true` in the
//! TiKV configuration.

pub mod checkpoint;
pub mod config;
pub mod direct_ingest;
pub mod errors;
pub mod http_control;
pub mod metrics;
pub mod pcrpb_gen;
pub mod schema_sync;
pub mod span_ctl;
pub mod sst_batcher;
pub mod subscriber;
pub mod task;

pub use config::{StreamIngestConfig, StreamIngestConfigManager};
pub use direct_ingest::DirectIngestContext;
pub use errors::{Error, Result};
pub use metrics::STREAM_INGEST_METRICS;
pub use pcrpb_gen::pcrpb;
pub use pcrpb_gen::pcrpb_grpc;
pub use sst_batcher::SstBatcher;
pub use subscriber::{StreamErrorKind, SubscriptionState};
pub use task::{diff_topology, TopologyChange, TopologyDiff, StreamIngestTask, Task};

use std::sync::Arc;

use engine_traits::KvEngine;
use pd_client::PdClient;

/// Factory function — takes individual config params to avoid
/// type mismatch between tikv::config and stream_ingest::config.
pub fn create_stream_ingest_task<E: KvEngine>(
    enable: bool,
    source_address: String,
    target_tidb_address: String,
    max_kv_buffer_size: usize,
    max_range_buffer_size: usize,
    checkpoint_interval_secs: u64,
    num_threads: usize,
    engine: Arc<E>,
    pd_client: Arc<dyn PdClient>,
    data_dir: impl Into<std::path::PathBuf>,
    write_guard: Arc<std::sync::atomic::AtomicBool>,
) -> StreamIngestTask<E> {
    let cfg = StreamIngestConfig {
        enable,
        source_address,
        target_tidb_address,
        max_kv_buffer_size: tikv_util::config::ReadableSize(max_kv_buffer_size as u64),
        max_range_key_buffer_size: tikv_util::config::ReadableSize(max_range_buffer_size as u64),
        min_flush_interval: tikv_util::config::ReadableDuration::millis(200),
        checkpoint_interval: tikv_util::config::ReadableDuration::secs(checkpoint_interval_secs),
        num_subscription_threads: num_threads,
    };
    StreamIngestTask::new(cfg, engine, pd_client, data_dir, write_guard)
}
