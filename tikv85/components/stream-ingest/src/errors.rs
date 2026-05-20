// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("tablet not found for region {0}")]
    TabletNotFound(u64),

    #[error("tablet not available for region {0}")]
    TabletNotAvailable(u64),

    #[error(
        "region epoch mismatch: expected conf_ver={0}, version={1}, got conf_ver={2}, version={3}"
    )]
    RegionEpochMismatch(u64, u64, u64, u64),

    #[error("SST ingest failed: {0}")]
    IngestError(String),

    #[error("SST writer error: {0}")]
    SstWriterError(String),

    #[error("gRPC subscription error: {0}")]
    SubscriptionError(String),

    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    #[error("config error: {0}")]
    ConfigError(String),

    #[error("serde error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
