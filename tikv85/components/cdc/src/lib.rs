// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(assert_matches)]

mod channel;
mod config;
mod delegate;
mod endpoint;
mod errors;
mod initializer;
mod logical_mutation;
pub mod metrics;
mod observer;
mod pcr_registry;
mod old_value;
mod pcr_event_batcher;
pub mod pcr_metrics;
pub mod pcr_service;
mod pcr_snapshot;
pub mod span_bridge;
// pcr_types replaced by stream_ingest::pcrpb (generated via protobuf-build)
mod service;
mod txn_source;

pub use channel::{recv_timeout, CdcEvent};
pub use config::CdcConfigManager;
pub use delegate::Delegate;
pub use endpoint::{CdcTxnExtraScheduler, Endpoint, Task, Validate};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use old_value::OldValueCache;
pub use pcr_event_batcher::PcrEventBatcher;
pub use stream_ingest::pcrpb::{OpType, PcrEvent, PcrKv, PcrKvBatch};
pub use service::{FeatureGate, Service};
