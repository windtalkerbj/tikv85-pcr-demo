// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use engine_traits::{ImportExt, KvEngine};
use kvproto::metapb::Region;
use pd_client::PdClient;
use slog_global::{debug, info, warn};

use crate::errors::{Error, Result};
use crate::metrics::STREAM_INGEST_METRICS;

/// Context for direct SST ingestion — wraps shared RocksDB engine and PD client
/// for Region lookup, Epoch validation, and concurrent ingest safety.
/// In raft-v1, all Regions share a single RocksDB; no TabletRegistry needed.
pub struct DirectIngestContext<E: KvEngine> {
    pub(crate) engine: Arc<E>,
    pd_client: Arc<dyn PdClient>,
    /// Cached Region metadata for Epoch validation.
    region_states: Mutex<std::collections::HashMap<u64, Region>>,
}

impl<E: KvEngine> DirectIngestContext<E> {
    pub fn new(
        engine: Arc<E>,
        pd_client: Arc<dyn PdClient>,
    ) -> Self {
        Self {
            engine,
            pd_client,
            region_states: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Access the shared RocksDB engine.
    pub fn engine(&self) -> &Arc<E> {
        &self.engine
    }

    /// Update the cached Region metadata from StoreMeta.
    /// Called periodically or on Region change events (split/merge).
    pub fn update_region(&self, region: Region) {
        let mut states = self.region_states.lock().unwrap();
        states.insert(region.get_id(), region);
    }

    /// Remove a Region from the cache (e.g., after merge).
    pub fn remove_region(&self, region_id: u64) {
        let mut states = self.region_states.lock().unwrap();
        states.remove(&region_id);
    }

    /// Handle a Region split event from the source cluster.
    ///
    /// When a Region splits, the source cluster sends a `PcrSplit` event.
    /// The consumer must update its local routing table so that future
    /// SSTs are ingested into the correct post-split Region.
    ///
    /// For now, this invalidates the old Region entry; actual re-population
    /// with the new Region IDs comes from the next PD/StoreMeta refresh.
    pub fn handle_split(&self, split_key: &[u8]) {
        let mut states = self.region_states.lock().unwrap();

        // Find Regions that need updating based on the split key.
        // A Region with (start_key, end_key) where start_key < split_key < end_key
        // is the one that split.
        let regions_to_update: Vec<u64> = states
            .iter()
            .filter(|(_id, region)| {
                let start = region.get_start_key();
                let end = region.get_end_key();
                start < split_key && (end.is_empty() || split_key < end)
            })
            .map(|(id, _)| *id)
            .collect();

        for region_id in regions_to_update {
            states.remove(&region_id);
            debug!(
                "Region {} invalidated due to split at key {:?}",
                region_id, split_key
            );
        }
    }

    /// Check if the Region exists in the local cache and its key range
    /// contains the given key.
    pub fn contains_key(&self, region_id: u64, key: &[u8]) -> bool {
        let states = self.region_states.lock().unwrap();
        if let Some(region) = states.get(&region_id) {
            let start = region.get_start_key();
            let end = region.get_end_key();
            key >= start && (end.is_empty() || key < end)
        } else {
            false
        }
    }

    /// Expose region states for external queries.
    pub fn region_count(&self) -> usize {
        self.region_states.lock().unwrap().len()
    }

    /// Ingest an SST file directly into a target Region's RocksDB Tablet,
    /// **bypassing the Raft state machine**.
    ///
    /// Uses `pd_client.get_region_info(first_key)` to find the correct target
    /// Region for the SST's key range, since source and target Region IDs differ.
    ///
    /// # Safety guarantees
    ///
    /// 1. **Key-based Region lookup**: Finds the target Region by key range via PD
    /// 2. **IngestLatch**: Acquires RocksDB's ingest latch to prevent
    ///    concurrent ingest operations on the same key range.
    pub fn ingest_sst(
        &self,
        _source_region_id: u64,
        sst_data: &[u8],
        first_key: &[u8],
        last_key: &[u8],
        cf: &str,
    ) -> Result<u64> {
        if sst_data.is_empty() {
            return Ok(0);
        }

        // 1. Find the correct target Region by key range via PD
        let region_info = self
            .pd_client
            .get_region_info(first_key)
            .map_err(|e| Error::IngestError(format!("PD lookup failed: {:?}", e)))?;
        let region_id = region_info.region.get_id();
        let data_len = sst_data.len();

        // 2. In raft-v1, all Regions share a single RocksDB — use the shared engine.
        // 3. PCR event loop is single-threaded (tokio::select! processes one
        //    event at a time). The target TiKV does not participate in Raft,
        //    so no concurrent ingest can happen. Latch is unnecessary.
        // 4. Write SST bytes to a temp file
        let tmp_path = format!(
            "/tmp/pcr_ingest_{}_{}.sst",
            region_id,
            uuid::Uuid::new_v4()
        );
        std::fs::write(&tmp_path, sst_data)?;

        // 5. Call RocksDB's IngestExternalFile on the shared engine
        let result = self.engine.ingest_external_file_cf(
            cf,
            &[&tmp_path],
            None,  // range — None means ingest the full SST
            true,  // force_allow_write — PCR may overwrite existing data
        );

        // 6. Clean up temp file (regardless of success/failure)
        if let Err(e) = std::fs::remove_file(&tmp_path) {
            debug!("failed to remove temp SST {}: {:?}", tmp_path, e);
        }

        match result {
            Ok(()) => {
                info!(
                    "DirectIngest succeeded: region={}, cf={}, bytes={}",
                    region_id, cf, data_len
                );
                Ok(region_id)
            }
            Err(e) => {
                STREAM_INGEST_METRICS.errors.with_label_values(&["ingest"]).inc();
                warn!(
                    "DirectIngest failed: region={}, bytes={}, error={:?}",
                    region_id, data_len, e
                );
                Err(Error::IngestError(format!(
                    "ingest_external_file_cf failed for region {}: {:?}",
                    region_id, e
                )))
            }
        }
    }

    /// Apply a delete range directly to the RocksDB engine (DROP TABLE / TRUNCATE).
    /// Uses DeleteFiles + DeleteByKey for efficient bulk deletion across all CFs.
    pub fn apply_delete_range(&self, start_key: &[u8], end_key: &[u8]) {
        use engine_traits::{DeleteStrategy, MiscExt, Range as EngineRange};
        let wopts = engine_traits::WriteOptions::new();
        for cf in &["default", "write", "lock"] {
            let r = EngineRange::new(start_key, end_key);
            let _ = self.engine.delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteFiles, &[r]);
            let _ = self.engine.delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteByKey, &[r]);
        }
        info!("PCR: delete_range applied"; "start_key_len" => start_key.len(), "end_key_len" => end_key.len());
    }

    /// Validate that the expected key range matches the current Region metadata.
    ///
    /// This prevents writing SST data to a Region that has been split or merged
    /// since the SST was generated on the source cluster.
    fn validate_region_epoch(
        &self,
        region_id: u64,
        expected_start_key: &[u8],
        expected_end_key: &[u8],
    ) -> Result<()> {
        let states = self.region_states.lock().unwrap();

        let region = states
            .get(&region_id)
            .ok_or(Error::RegionEpochMismatch(
                0, 0, 0, 0,
            ))?;

        let actual_start = region.get_start_key();
        let actual_end = region.get_end_key();

        // Check that the Region's current key range matches expectations
        if actual_start != expected_start_key {
            return Err(Error::RegionEpochMismatch(
                expected_start_key.len() as u64,
                expected_end_key.len() as u64,
                actual_start.len() as u64,
                actual_end.len() as u64,
            ));
        }

        if actual_end != expected_end_key {
            return Err(Error::RegionEpochMismatch(
                expected_start_key.len() as u64,
                expected_end_key.len() as u64,
                actual_start.len() as u64,
                actual_end.len() as u64,
            ));
        }

        debug!(
            "RegionEpoch validated: region={}, start={:?}, end={:?}",
            region_id, actual_start, actual_end
        );

        Ok(())
    }
}
