// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR stream-ingest configuration — local definition for prototype.
//!
//! This struct mirrors `src/config/mod.rs::StreamIngestConfig` to avoid
//! a circular dependency between the `tikv` and `stream-ingest` crates.
//! In production, use `tikv::config::StreamIngestConfig` after the crate
//! dependency graph is restructured.

use online_config::{ConfigManager, OnlineConfig};
use serde::{Deserialize, Serialize};
use tikv_util::config::{ReadableDuration, ReadableSize};
use tikv_util::worker::Scheduler;

use crate::Task;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StreamIngestConfig {
    #[online_config(skip)]
    pub enable: bool,
    pub max_kv_buffer_size: ReadableSize,
    pub max_range_key_buffer_size: ReadableSize,
    pub min_flush_interval: ReadableDuration,
    #[online_config(skip)]
    pub source_address: String,
    #[online_config(skip)]
    pub target_tidb_address: String,
    pub checkpoint_interval: ReadableDuration,
    #[online_config(skip)]
    pub num_subscription_threads: usize,
}

impl Default for StreamIngestConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_kv_buffer_size: ReadableSize::mb(64),
            max_range_key_buffer_size: ReadableSize::mb(32),
            min_flush_interval: ReadableDuration::millis(200),
            source_address: String::new(),
            target_tidb_address: String::new(),
            checkpoint_interval: ReadableDuration::secs(10),
            num_subscription_threads: num_cpus::get(),
        }
    }
}

/// ConfigManager for dynamic hot-reload.
pub struct StreamIngestConfigManager {
    scheduler: Scheduler<Task>,
}

impl StreamIngestConfigManager {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl ConfigManager for StreamIngestConfigManager {
    fn dispatch(
        &mut self,
        _change: online_config::ConfigChange,
    ) -> online_config::Result<()> {
        self.scheduler
            .schedule(Task::ConfigChange)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        Ok(())
    }
}
