// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine_traits::{KvEngine, SstExt, SstWriter, SstWriterBuilder, TabletRegistry};
use slog_global::{debug, info, warn};

use crate::direct_ingest::DirectIngestContext;
use crate::errors::{Error, Result};
use crate::metrics::STREAM_INGEST_METRICS;

/// An MVCC-encoded key-value pair for PCR buffering.
#[derive(Clone, Debug)]
pub struct MvccKeyValue {
    /// MVCC-encoded key (includes timestamp suffix)
    pub key: Vec<u8>,
    /// MVCC-encoded value
    pub value: Vec<u8>,
    /// Target column family ("default", "write", "lock")
    pub cf: String,
}

/// Reasons why a flush is triggered.
#[derive(Debug, PartialEq)]
pub enum FlushReason {
    /// KV buffer exceeded max size
    BufferFull,
    /// Checkpoint event arrived
    Checkpoint,
    /// Key range crossed a Region boundary (avoid cross-Region SST)
    RangeBoundary,
    /// Manual flush requested
    Manual,
}

/// Result of a flush operation.
#[derive(Debug)]
pub struct FlushResult {
    pub region_id: u64,
    pub ingested_bytes: u64,
    pub ingested_kvs: u64,
}

/// Streaming SST batcher that:
/// 1. Buffers MVCC KV pairs in memory
/// 2. Sorts them by key (byte order)
/// 3. Generates SST files via RocksDB SstFileWriter
/// 4. Calls DirectIngestContext to write SST bypassing Raft
pub struct SstBatcher<E: KvEngine> {
    /// Buffered KV pairs, sorted by key before flush
    kv_buffer: Vec<MvccKeyValue>,

    /// Total byte size of buffered KVs (key + value lengths)
    pub kv_buffer_size: usize,

    /// Maximum KV buffer size before triggering flush
    pub max_kv_buffer_size: usize,

    /// Buffered delete-range operations: (start_key, end_key, ts)
    range_buffer: Vec<(Vec<u8>, Vec<u8>, u64)>,

    /// Total byte size of buffered range deletes
    pub range_buffer_size: usize,

    /// Maximum range-key buffer size
    pub max_range_buffer_size: usize,

    /// The current key range being batched
    current_start_key: Vec<u8>,
    current_end_key: Vec<u8>,

    /// The Region this batch belongs to (set by caller before add_kv)
    pub current_region_id: u64,

    /// Direct ingest context — handles Epoch validation, latch, and ingest
    ingest_ctx: Arc<DirectIngestContext<E>>,

    /// Statistics
    total_flushes: u64,
    total_kvs_written: u64,
    total_bytes_written: u64,

    /// Timestamp of last flush for time-based flush triggering
    last_flush_time: Instant,
}

impl<E: KvEngine> SstBatcher<E> {
    /// Milliseconds since last flush. Returns 0 if never flushed.
    pub fn elapsed_since_last_flush(&self) -> u64 {
        self.last_flush_time.elapsed().as_millis() as u64
    }

    /// Number of pending KVs in the buffer (unflushed).
    pub fn pending_kvs(&self) -> usize {
        self.kv_buffer.len()
    }
}

impl<E: KvEngine> SstBatcher<E> {
    pub fn new(
        ingest_ctx: Arc<DirectIngestContext<E>>,
        max_kv_buffer_size: usize,
        max_range_buffer_size: usize,
    ) -> Self {
        Self {
            kv_buffer: Vec::with_capacity(65536),
            kv_buffer_size: 0,
            max_kv_buffer_size,
            range_buffer: Vec::new(),
            range_buffer_size: 0,
            max_range_buffer_size,
            current_start_key: Vec::new(),
            current_end_key: Vec::new(),
            current_region_id: 0,
            ingest_ctx,
            total_flushes: 0,
            total_kvs_written: 0,
            total_bytes_written: 0,
            last_flush_time: Instant::now(),
        }
    }

    /// Add a single MVCC KV pair to the buffer.
    /// Returns true if the buffer should be flushed.
    pub fn add_kv(&mut self, kv: MvccKeyValue) -> bool {
        let entry_size = kv.key.len() + kv.value.len();
        self.kv_buffer_size += entry_size;
        self.update_current_range(&kv.key);
        self.kv_buffer.push(kv);

        self.kv_buffer_size >= self.max_kv_buffer_size
    }

    /// Add a delete-range operation to the buffer.
    pub fn add_delete_range(&mut self, start_key: &[u8], end_key: &[u8], ts: u64) {
        let entry_size = start_key.len() + end_key.len();
        self.range_buffer_size += entry_size;
        self.range_buffer
            .push((start_key.to_vec(), end_key.to_vec(), ts));
    }

    /// Check if the given flush condition is met.
    pub fn should_flush(&self, reason: FlushReason) -> bool {
        match reason {
            FlushReason::BufferFull => self.kv_buffer_size >= self.max_kv_buffer_size,
            FlushReason::Checkpoint => !self.kv_buffer.is_empty(),
            FlushReason::RangeBoundary => self.current_region_id != 0 && !self.kv_buffer.is_empty(),
            FlushReason::Manual => !self.kv_buffer.is_empty(),
        }
    }

    /// Check if adding a key with the given start/end key crosses the current
    /// buffered range boundary (indicating a different Region).
    pub fn crosses_region_boundary(&self, key: &[u8]) -> bool {
        if self.current_end_key.is_empty() {
            return false;
        }
        // If the new key goes beyond the current end, we may be in a new Region
        key < self.current_start_key.as_slice()
    }

    /// Flush all buffered data: group by CF → sort → per-CF SST → direct ingest.
    pub fn flush(&mut self) -> Result<FlushResult> {
        let start = Instant::now();
        debug!("PCR: SstBatcher flush started"; "kv_count" => self.kv_buffer.len());

        if self.kv_buffer.is_empty() && self.range_buffer.is_empty() {
            return Ok(FlushResult {
                region_id: 0,
                ingested_bytes: 0,
                ingested_kvs: 0,
            });
        }

        let kv_count = self.kv_buffer.len();

        // 1. Group KVs by column family (clone to avoid borrow issues)
        let mut cf_groups: std::collections::HashMap<String, Vec<MvccKeyValue>> =
            std::collections::HashMap::new();
        for kv in self.kv_buffer.drain(..) {
            cf_groups
                .entry(kv.cf.clone())
                .or_default()
                .push(kv);
        }

        let mut total_bytes: u64 = 0;
        let mut region_id = 0u64;
        let cf_count = cf_groups.len();

        // 2. For each CF: sort KVs, generate SST, ingest to that CF
        for (cf, mut kvs) in cf_groups {
            let t_sort = std::time::Instant::now();
            kvs.sort_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));
            STREAM_INGEST_METRICS.sst_sort_latency.observe(t_sort.elapsed().as_secs_f64());

            let first_key = kvs[0].key.clone();
            let last_key = kvs[kvs.len() - 1].key.clone();

            // Generate SST for this CF
            let t_gen = std::time::Instant::now();
            let sst_path = self.temp_sst_path();
            let mut writer = <<E as SstExt>::SstWriterBuilder>::new()
                .set_in_memory(true)
                .set_cf(cf.as_str())
                .build(&sst_path)
                .map_err(|e| {
                    Error::SstWriterError(format!(
                        "failed to create SST writer for CF {}: {:?}", cf, e
                    ))
                })?;

            for kv in &kvs {
                writer
                    .put(&kv.key, &kv.value)
                    .map_err(|e| Error::SstWriterError(format!("SST put failed: {:?}", e)))?;
            }

            let (_, mut reader) = writer
                .finish_read()
                .map_err(|e| Error::SstWriterError(format!("SST finish_read failed: {:?}", e)))?;

            let mut sst_data = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut sst_data)?;
            let sst_size = sst_data.len() as u64;
            STREAM_INGEST_METRICS.sst_generate_latency.observe(t_gen.elapsed().as_secs_f64());

            // Build key range for this CF batch
            let first_key_start = first_key.clone();
            let mut last_key_end = last_key.clone();
            last_key_end.push(0);

            // Ingest SST to the correct CF, using key-based PD lookup
            let t_ingest = std::time::Instant::now();
            region_id = self.ingest_ctx.ingest_sst(
                self.current_region_id,
                &sst_data,
                &first_key_start,
                &last_key_end,
                cf.as_str(),
            )?;
            STREAM_INGEST_METRICS.sst_ingest_latency.observe(t_ingest.elapsed().as_secs_f64());

            // Per-CF counters
            STREAM_INGEST_METRICS
                .cf_ingested_bytes
                .with_label_values(&[cf.as_str()])
                .inc_by(sst_size);
            STREAM_INGEST_METRICS
                .cf_ingested_kvs
                .with_label_values(&[cf.as_str()])
                .inc_by(kvs.len() as u64);

            std::fs::remove_file(&sst_path).ok();
            total_bytes += sst_size;

            info!(
                "SstBatcher flushed CF={}: region={}, kvs={}, bytes={}",
                cf, region_id, kvs.len(), sst_size
            );
        }

        // 3. Update metrics
        let elapsed = start.elapsed();
        STREAM_INGEST_METRICS.flush_count.inc();
        STREAM_INGEST_METRICS.flush_latency.observe(elapsed.as_secs_f64());
        STREAM_INGEST_METRICS.ingested_kvs.inc_by(kv_count as u64);
        STREAM_INGEST_METRICS.ingested_bytes.inc_by(total_bytes);
        STREAM_INGEST_METRICS.ingested_ssts.inc_by(cf_count as u64);
        STREAM_INGEST_METRICS.buffer_size.set(0);

        // 3. Apply delete ranges (DROP TABLE / TRUNCATE) via DirectIngestContext
        for (start_key, end_key, _ts) in self.range_buffer.drain(..) {
            self.ingest_ctx.apply_delete_range(&start_key, &end_key);
        }

        // 4. Reset buffers (kv_buffer already drained above)
        self.kv_buffer_size = 0;
        self.range_buffer.clear();
        self.range_buffer_size = 0;

        self.total_flushes += 1;
        self.total_kvs_written += kv_count as u64;
        self.total_bytes_written += total_bytes;
        self.last_flush_time = Instant::now();

        debug!(
            "SstBatcher flushed: region={}, kvs={}, bytes={}, cfs={}, elapsed={:?}",
            region_id, kv_count, total_bytes, cf_count, elapsed
        );

        Ok(FlushResult {
            region_id,
            ingested_bytes: total_bytes,
            ingested_kvs: kv_count as u64,
        })
    }

    /// Update the tracked key range based on a newly added key.
    fn update_current_range(&mut self, key: &[u8]) {
        if self.current_start_key.is_empty() {
            self.current_start_key = key.to_vec();
        }
        self.current_end_key = key.to_vec();
    }

    fn temp_sst_path(&self) -> String {
        format!(
            "/tmp/pcr_sst_{}_{}.sst",
            uuid::Uuid::new_v4(),
            self.total_flushes
        )
    }

    // ---- Accessors ----

    pub fn kv_buffer_size(&self) -> usize {
        self.kv_buffer_size
    }

    /// Access the underlying ingest context (for region lookups, split handling).
    pub fn ingest_context(&self) -> &Arc<DirectIngestContext<E>> {
        &self.ingest_ctx
    }

    /// Access the ingest context directly.
    pub fn ingest_ctx_ref(&self) -> &DirectIngestContext<E> {
        &self.ingest_ctx
    }

    pub fn total_kvs_written(&self) -> u64 {
        self.total_kvs_written
    }

    pub fn total_bytes_written(&self) -> u64 {
        self.total_bytes_written
    }
}
