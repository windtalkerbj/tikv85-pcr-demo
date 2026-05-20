// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR Event Batcher — batches MVCC KV events for streaming to PCR Consumers.
//!
//! Uses generated protobuf types from `stream_ingest::pcrpb` with proper
//! `Message` serialization for gRPC wire transport.

use std::mem;
use std::time::Instant;


use protobuf::Message;
use protobuf::RepeatedField;
use stream_ingest::pcrpb::*;

/// Default batch byte size: 1MB
const DEFAULT_BATCH_BYTE_SIZE: usize = 1 * 1024 * 1024;

/// Default flush interval: 150ms
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 150;

/// Accumulates PCR events into a batch using proper protobuf types.
pub struct PcrEventBatcher {
    /// Current batch being accumulated
    batch: PcrKvBatch,
    /// Total approximate byte size of the batch
    size: usize,
    /// Size threshold that triggers a flush (bytes)
    batch_byte_size: usize,
    /// Monotonically increasing sequence number
    seq_num: u64,
    /// Time of last flush (for time-based trigger)
    pub(crate) last_flush: Instant,
    /// Maximum time between flushes (ms, 0 = disabled)
    flush_interval_ms: u64,
}

impl PcrEventBatcher {
    pub fn new(batch_byte_size: usize, flush_interval_ms: u64) -> Self {
        Self {
            batch: PcrKvBatch::new(),
            size: 0,
            batch_byte_size,
            seq_num: 0,
            last_flush: Instant::now(),
            flush_interval_ms,
        }
    }

    /// Add a single MVCC KV to the batch.
    pub fn add_kv(&mut self, key: Vec<u8>, value: Vec<u8>, op: OpType, cf: &str) {
        self.size += key.len() + value.len();
        let mut kv = PcrKv::new();
        kv.set_key(key);
        kv.set_value(value);
        kv.set_op(op);
        kv.set_cf(cf.to_string());
        self.batch.mut_kvs().push(kv);
    }

    /// Check if the batch should be flushed: size threshold reached
    /// OR the flush interval has elapsed since last flush.
    pub fn should_flush(&self) -> bool {
        self.size >= self.batch_byte_size
            || (self.flush_interval_ms > 0
                && self.size > 0
                && self.last_flush.elapsed().as_millis() as u64 >= self.flush_interval_ms)
    }

    /// Get current batch size in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Flush the current batch into a protobuf-serialized PcrEvent.
    ///
    /// Returns None if the batch is empty. The returned bytes are a
    /// protobuf-encoded `PcrEvent` with a `kv_batch` payload.
    pub fn flush(&mut self) -> Option<Vec<u8>> {
        if self.batch.get_kvs().is_empty() {
            return None;
        }

        self.seq_num += 1;

        let mut event = PcrEvent::new();
        event.set_stream_seq(self.seq_num);
        event.set_kv_batch(mem::take(&mut self.batch));
        self.batch = PcrKvBatch::new();
        self.size = 0;
        self.last_flush = Instant::now();

        // Serialize to protobuf wire format
        event.write_to_bytes().ok()
    }

    /// Get the next sequence number without incrementing.
    pub fn next_seq(&self) -> u64 {
        self.seq_num + 1
    }
}

impl Default for PcrEventBatcher {
    fn default() -> Self {
        Self::new(DEFAULT_BATCH_BYTE_SIZE, DEFAULT_FLUSH_INTERVAL_MS)
    }
}
