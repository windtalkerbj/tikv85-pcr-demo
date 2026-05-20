// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pd_client::PdClient;
use slog_global::{debug, info, warn};
use txn_types::TimeStamp;

use crate::errors::{Error, Result};
use crate::task::Frontier;

/// Manages checkpoint persistence and recovery.
///
/// Checkpoints are stored as PD service safe points for external observability
/// and TTL-based cleanup. The full frontier is also persisted to a local file
/// so it can be restored on resume (PD has no read API for service safe points).
pub struct CheckpointManager {
    pd_client: Arc<dyn PdClient>,
    task_id: String,
    last_recorded_ts: Option<u64>,
    checkpoint_file: PathBuf,
}

impl CheckpointManager {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        task_id: impl Into<String>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        let tid: String = task_id.into();
        let cp_file = data_dir.into().join(format!("pcr_checkpoint_{}.json", tid));
        Self {
            pd_client,
            task_id: tid,
            last_recorded_ts: None,
            checkpoint_file: cp_file,
        }
    }

    /// Record the current frontier to PD and local file.
    pub async fn record_checkpoint(&mut self, frontier: &Frontier) -> Result<()> {
        if frontier.is_empty() { return Ok(()); }

        let min_ts = frontier.values().min().copied().unwrap_or(0);

        if self.last_recorded_ts == Some(min_ts) {
            debug!("Checkpoint unchanged: min_ts={}", min_ts);
            return Ok(());
        }
        self.last_recorded_ts = Some(min_ts);

        // Persist full frontier to local file (for resume)
        if let Ok(json) = serde_json::to_string(frontier) {
            if let Some(parent) = self.checkpoint_file.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(&self.checkpoint_file, &json) {
                warn!("Checkpoint: failed to write local file: {:?}", e);
            }
        }

        // Store as PD service safe point with TTL (for observability)
        let checkpoint_label = format!("pcr-{}", self.task_id);
        self.pd_client
            .update_service_safe_point(checkpoint_label, min_ts.into(), Duration::from_secs(3600))
            .await
            .map_err(|e| Error::CheckpointError(format!("failed to update safe point: {:?}", e)))?;

        info!("Checkpoint recorded: regions={}, min_ts={}", frontier.len(), min_ts);
        Ok(())
    }

    /// Load the most recent checkpoint from local file.
    /// Returns the full frontier (BTreeMap<region_id, resolved_ts>).
    pub async fn load_checkpoint(&self) -> Result<Frontier> {
        match std::fs::read_to_string(&self.checkpoint_file) {
            Ok(data) => {
                let frontier: BTreeMap<u64, u64> = serde_json::from_str(&data).map_err(|e| {
                    Error::CheckpointError(format!("failed to parse checkpoint: {:?}", e))
                })?;
                info!(
                    "Checkpoint loaded: {} regions from {:?}",
                    frontier.len(),
                    self.checkpoint_file
                );
                Ok(frontier)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("Checkpoint: no prior checkpoint file, starting fresh");
                Ok(BTreeMap::new())
            }
            Err(e) => {
                warn!("Checkpoint: failed to read local file: {:?}", e);
                Ok(BTreeMap::new())
            }
        }
    }

    /// Clear checkpoint after cutover.
    pub async fn clear_checkpoint(&self) -> Result<()> {
        let checkpoint_label = format!("pcr-{}", self.task_id);
        self.pd_client
            .update_service_safe_point(checkpoint_label, 0.into(), Duration::from_secs(0))
            .await
            .map_err(|e| Error::CheckpointError(format!("failed to clear safe point: {:?}", e)))?;
        // Remove local checkpoint file
        let _ = std::fs::remove_file(&self.checkpoint_file);
        info!("Checkpoint cleared for task {}", self.task_id);
        Ok(())
    }

    pub fn global_min_ts(frontier: &Frontier) -> Option<u64> {
        frontier.values().min().copied()
    }
}
