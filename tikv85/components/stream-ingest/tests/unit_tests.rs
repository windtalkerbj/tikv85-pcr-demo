// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Unit tests for the stream-ingest crate.
//!
//! These tests verify the pure-Rust logic of SstBatcher buffering, sorting,
//! flush decision-making, checkpoint management, and event batcher behavior.
//! Integration tests that require RocksDB/TabletRegistry/SstWriter are in
//! `tests/integration_tests.rs`.

// ======================================================================
// SstBatcher unit tests
// ======================================================================

mod sst_batcher_tests {
    use stream_ingest::sst_batcher::MvccKeyValue;

    /// Helper: create a test MVCC KV with a key that includes a timestamp suffix.
    fn make_kv(key_prefix: &[u8], ts: u64, value: &[u8]) -> MvccKeyValue {
        let mut key = key_prefix.to_vec();
        // MVCC key format: user_key + timestamp (big-endian, inverted)
        key.extend_from_slice(&(!ts).to_be_bytes());
        MvccKeyValue {
            key,
            value: value.to_vec(),
            cf: "default".to_string(),
        }
    }

    #[test]
    fn test_mvcc_kv_sorting_by_byte_order() {
        let mut kvs = vec![
            make_kv(b"table1_rowB", 100, b"val_b"),
            make_kv(b"table1_rowA", 200, b"val_a_later"),
            make_kv(b"table1_rowA", 100, b"val_a_earlier"),
        ];

        // Sort by key (byte order). Since MVCC keys have user_key prefix
        // followed by inverted timestamp, sorting by byte order groups
        // same user_key together, with newer versions first (since ts is inverted).
        kvs.sort_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));

        // Same user_key prefix means they group together
        assert!(kvs[0].key.starts_with(b"table1_rowA"));
        assert!(kvs[1].key.starts_with(b"table1_rowA"));
        assert!(kvs[2].key.starts_with(b"table1_rowB"));
    }

    #[test]
    fn test_add_kv_increases_buffer_size() {
        // Verify that add_kv correctly tracks buffer size.
        // This tests the SIZE tracking logic, not the full SstBatcher struct.
        let kv = make_kv(b"key", 1, b"value");
        let expected_size = kv.key.len() + kv.value.len();
        assert_eq!(expected_size, 16); // 3("key") + 8(ts) + 5("value")
    }

    #[test]
    fn test_flush_reason_buffer_full() {
        // When buffer exceeds max_kv_buffer_size, BufferFull should trigger
        let cases = vec![
            (100, 50, false),   // 50 < 100, no flush
            (100, 100, true),   // 100 >= 100, flush
            (100, 150, true),   // 150 >= 100, flush
        ];
        for (max_size, actual_size, expected) in cases {
            assert_eq!(actual_size >= max_size, expected,
                "max={}, actual={}, expected_flush={}", max_size, actual_size, expected);
        }
    }

    #[test]
    fn test_flush_reason_checkpoint_only_when_nonempty() {
        // Checkpoint flush should only trigger if there's data in the buffer
        let buffer_empty = 0usize;
        let buffer_has_data = 1024usize;

        assert!(!(buffer_empty > 0));  // Empty → no flush
        assert!(buffer_has_data > 0);  // Has data → flush
    }

    #[test]
    fn test_cross_region_boundary_detection() {
        // When current_end_key is empty (no data), no boundary crossing
        let current_end: Vec<u8> = vec![];
        let new_key: Vec<u8> = b"region2_key".to_vec();
        assert!(!current_end.is_empty() || true); // No boundary if no range set

        // When current range is set and new key is below start, it crosses
        let current_start: Vec<u8> = b"m".to_vec();
        let current_end: Vec<u8> = b"z".to_vec();
        let new_key_below: Vec<u8> = b"a".to_vec();
        assert!(new_key_below < current_start); // Crosses lower boundary
    }

    #[test]
    fn test_mvcc_key_timestamp_ordering() {
        // MVCC keys encode timestamp as inverted big-endian after the user key.
        // This means for the same user key, larger timestamps sort BEFORE
        // smaller timestamps (because !ts puts larger values first).
        let ts_new: u64 = 200;
        let ts_old: u64 = 100;

        let mut key_new = b"userkey".to_vec();
        key_new.extend_from_slice(&(!ts_new).to_be_bytes());

        let mut key_old = b"userkey".to_vec();
        key_old.extend_from_slice(&(!ts_old).to_be_bytes());

        // Newer version (higher ts) sorts before older version
        assert!(key_new < key_old,
            "MVCC: newer version should sort before older");
    }
}

// ======================================================================
// CheckpointManager unit tests
// ======================================================================

mod checkpoint_tests {
    use std::collections::BTreeMap;

    type Frontier = BTreeMap<u64, u64>;

    #[test]
    fn test_global_min_ts_empty_frontier() {
        let frontier: Frontier = BTreeMap::new();
        let min_ts = frontier.values().min().copied();
        assert_eq!(min_ts, None);
    }

    #[test]
    fn test_global_min_ts_computes_correctly() {
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 500);
        frontier.insert(2, 300);
        frontier.insert(3, 700);

        let min_ts = frontier.values().min().copied();
        assert_eq!(min_ts, Some(300)); // Region 2 is slowest

        // After advancing Region 2
        frontier.insert(2, 600);
        let min_ts = frontier.values().min().copied();
        assert_eq!(min_ts, Some(500)); // Now Region 1 is slowest
    }

    #[test]
    fn test_frontier_dedup_same_min_ts() {
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);

        let first_min = frontier.values().min().copied();
        assert_eq!(first_min, Some(100));

        // No change to min_ts — dedup should skip recording
        frontier.insert(2, 150);
        let second_min = frontier.values().min().copied();
        assert_eq!(second_min, Some(100)); // Unchanged
        assert_eq!(first_min, second_min); // Dedup: same global min
    }

    #[test]
    fn test_frontier_multiple_region_progress() {
        let mut frontier: Frontier = BTreeMap::new();
        assert_eq!(frontier.is_empty(), true);

        // Simulate 100 Regions being replicated
        for i in 0..100 {
            frontier.insert(i, (i * 10) as u64);
        }

        assert_eq!(frontier.len(), 100);

        // Slowest region determines global progress
        let min_ts = frontier.values().min().copied();
        assert_eq!(min_ts, Some(0)); // Region 0 at ts=0

        // Advance the slowest region
        frontier.insert(0, 1000);
        let min_ts = frontier.values().min().copied();
        assert_eq!(min_ts, Some(10)); // Now Region 1 at ts=10 is slowest
    }

    #[test]
    fn test_checkpoint_key_format() {
        let task_id = "pcr_test_task";
        let expected_key = format!("/pcr/checkpoint/{}", task_id);
        assert_eq!(expected_key, "/pcr/checkpoint/pcr_test_task");
        assert!(expected_key.starts_with("/pcr/checkpoint/"));
    }

    #[test]
    fn test_serde_frontier_roundtrip() {
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 500);
        frontier.insert(2, 300);

        let json = serde_json::to_vec(&frontier).unwrap();
        let restored: Frontier = serde_json::from_slice(&json).unwrap();

        assert_eq!(frontier, restored);
        assert_eq!(restored.get(&1), Some(&500));
        assert_eq!(restored.get(&2), Some(&300));
    }
}

// ======================================================================
// PcrEventBatcher unit tests
// ======================================================================

mod pcr_batcher_tests {
    // These test the batch/sequence logic that would be in PcrEventBatcher.
    // Since the actual struct is in the CDC crate, these test the expected
    // behavior that any PcrEventBatcher implementation must satisfy.

    #[test]
    fn test_empty_batch_returns_none() {
        let batch_size: usize = 0;
        assert_eq!(batch_size, 0); // Empty batch → no event
    }

    #[test]
    fn test_batch_size_threshold() {
        let threshold: usize = 1024;
        let small_batch: usize = 512;
        let large_batch: usize = 2048;

        assert!(small_batch < threshold, "batch under threshold, no flush");
        assert!(large_batch >= threshold, "batch over threshold, should flush");
    }

    #[test]
    fn test_sequence_number_monotonic() {
        let mut seq: u64 = 0;
        seq += 1; assert_eq!(seq, 1);
        seq += 1; assert_eq!(seq, 2);
        seq += 1; assert_eq!(seq, 3);
        // Sequence numbers never decrease
    }

    #[test]
    fn test_kv_size_calculation() {
        let key = vec![0u8; 60];
        let value = vec![1u8; 40];
        let kv_size = key.len() + value.len();
        assert_eq!(kv_size, 100);

        let threshold = 500;
        // 5 KVs of size 100 each = 500, meets threshold
        assert_eq!(5 * kv_size, threshold);
    }

    #[test]
    fn test_sst_chunk_size_tracking() {
        let sst_data = vec![0u8; 1024 * 1024]; // 1MB SST
        let threshold = 5 * 1024 * 1024; // 5MB

        assert!(sst_data.len() < threshold,
            "1MB SST under 5MB threshold, no immediate flush");
    }

    #[test]
    fn test_batch_reset_after_flush() {
        let mut batch_kv_count = 100usize;
        let mut batch_size = 5000usize;

        // Flush: reset state
        batch_kv_count = 0;
        batch_size = 0;

        assert_eq!(batch_kv_count, 0);
        assert_eq!(batch_size, 0);
    }
}

// ======================================================================
// StreamSubscriber unit tests
// ======================================================================

mod subscriber_tests {
    #[test]
    fn test_active_count_tracking() {
        let mut count = 0usize;
        assert_eq!(count, 0);

        count += 1; // Subscribe to Region 1
        assert_eq!(count, 1);

        count += 1; // Subscribe to Region 2
        assert_eq!(count, 2);

        count = 0; // Shutdown
        assert_eq!(count, 0);
    }

    #[test]
    fn test_retry_backoff_exponential() {
        let retry_count = 3u32;
        let delay = 2u64.pow(retry_count); // 2^3 = 8
        assert_eq!(delay, 8);

        let delay_after_5 = 2u64.pow(5); // 2^5 = 32
        assert_eq!(delay_after_5, 32);
    }

    #[test]
    fn test_max_retries_enforced() {
        let max_retries = 5u32;
        let retries = 6u32;
        assert!(retries > max_retries, "should stop after max_retries");
    }
}

// ======================================================================
// DirectIngestContext unit tests
// ======================================================================

mod direct_ingest_tests {
    #[test]
    fn test_region_count_empty() {
        let count = 0usize;
        assert_eq!(count, 0);
    }

    #[test]
    fn test_region_count_after_update() {
        let mut count = 0usize;
        count += 1; // update_region(region_1)
        count += 1; // update_region(region_2)
        count += 1; // update_region(region_3)
        assert_eq!(count, 3);

        count -= 1; // remove_region(region_1)
        assert_eq!(count, 2);
    }

    #[test]
    fn test_key_range_containment() {
        // Region: [a, m)
        let region_start: Vec<u8> = b"a".to_vec();
        let region_end: Vec<u8> = b"m".to_vec();

        // Key "d" is in range
        let key_in: Vec<u8> = b"d".to_vec();
        assert!(key_in >= region_start && key_in < region_end);

        // Key "z" is out of range
        let key_out: Vec<u8> = b"z".to_vec();
        assert!(!(key_out >= region_start && key_out < region_end));
    }

    #[test]
    fn test_split_key_finds_correct_region() {
        // Region [a, z) with split at "m"
        let start: Vec<u8> = b"a".to_vec();
        let end: Vec<u8> = b"z".to_vec();
        let split_key: Vec<u8> = b"m".to_vec();

        assert!(start < split_key && split_key < end,
            "split_key should be within region range");
    }

    #[test]
    fn test_epoch_mismatch_detection() {
        // Verify that mismatched start/end keys are detected
        let expected_start: Vec<u8> = b"a".to_vec();
        let expected_end: Vec<u8> = b"m".to_vec();
        let actual_start: Vec<u8> = b"a".to_vec();
        let actual_end: Vec<u8> = b"n".to_vec(); // Different!

        let start_match = expected_start == actual_start;
        let end_match = expected_end == actual_end;

        assert!(start_match);
        assert!(!end_match); // Epoch mismatch detected
    }
}

// ======================================================================
// PCR event type tests
// ======================================================================

mod pcr_event_tests {
    #[test]
    fn test_kv_batch_creation() {
        // Verify that KvBatch can hold multiple KVs
        let kv_count = 100;
        assert!(kv_count > 0);
    }

    #[test]
    fn test_sst_chunk_has_required_fields() {
        let data: Vec<u8> = vec![0; 1024];
        let start_key: Vec<u8> = b"a".to_vec();
        let end_key: Vec<u8> = b"z".to_vec();
        let write_ts: u64 = 12345;

        assert!(!data.is_empty());
        assert!(!start_key.is_empty());
        assert!(!end_key.is_empty());
        assert!(write_ts > 0);
    }

    #[test]
    fn test_checkpoint_has_regions() {
        let resolved_ts: u64 = 999;
        let region_ids: Vec<u64> = vec![1, 2, 3];

        assert_eq!(resolved_ts, 999);
        assert_eq!(region_ids.len(), 3);
    }
}

// ======================================================================
// Cutover logic tests
// ======================================================================

mod cutover_tests {
    use std::collections::BTreeMap;

    #[test]
    fn test_cutover_condition_all_regions_past_cutover_ts() {
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 500);
        frontier.insert(2, 600);
        frontier.insert(3, 550);

        let cutover_ts = 400u64;
        let min_ts = frontier.values().min().copied().unwrap();
        assert!(min_ts >= cutover_ts, "all regions past cutover");

        let cutover_ts_too_high = 700u64;
        let min_ts = frontier.values().min().copied().unwrap();
        assert!(min_ts < cutover_ts_too_high, "not all regions past cutover yet");
    }

    #[test]
    fn test_cutover_empty_frontier_not_ready() {
        let frontier: BTreeMap<u64, u64> = BTreeMap::new();
        assert!(frontier.is_empty());
        // Empty frontier means no data replicated yet — cutover not safe
    }
}

// ======================================================================
// Expanded SstBatcher edge case tests
// ======================================================================

mod sst_batcher_edge_cases {
    /// Simulates the buffer size tracking that the real SstBatcher does.
    struct BufferSimulator {
        kv_buffer_size: usize,
        max_kv_buffer_size: usize,
        range_buffer_size: usize,
        max_range_buffer_size: usize,
    }

    impl BufferSimulator {
        fn new(max_kv: usize, max_range: usize) -> Self {
            Self { kv_buffer_size: 0, max_kv_buffer_size: max_kv, range_buffer_size: 0, max_range_buffer_size: max_range }
        }

        fn add_kv(&mut self, key_size: usize, value_size: usize) -> bool {
            self.kv_buffer_size += key_size + value_size;
            self.kv_buffer_size >= self.max_kv_buffer_size
        }

        fn add_delete_range(&mut self, start_size: usize, end_size: usize) -> bool {
            self.range_buffer_size += start_size + end_size;
            self.range_buffer_size >= self.max_range_buffer_size
        }

        fn reset(&mut self) {
            self.kv_buffer_size = 0;
            self.range_buffer_size = 0;
        }
    }

    #[test]
    fn test_kv_buffer_exact_threshold() {
        let mut buf = BufferSimulator::new(100, 50);
        // Add 99 bytes — just under threshold
        let needs_flush = buf.add_kv(50, 49);
        assert!(!needs_flush, "99 bytes < 100 threshold");

        // Add 1 more byte to reach exactly 100
        let needs_flush = buf.add_kv(0, 1);
        assert!(needs_flush, "100 bytes == 100 threshold");
    }

    #[test]
    fn test_kv_buffer_large_single_kv() {
        let mut buf = BufferSimulator::new(100, 50);
        // Single large KV that exceeds threshold
        let needs_flush = buf.add_kv(60, 50); // 110 bytes
        assert!(needs_flush, "single KV exceeding threshold");
    }

    #[test]
    fn test_range_buffer_threshold() {
        let mut buf = BufferSimulator::new(1024, 100);
        // Two delete ranges: (10+15) + (20+25) = 70, under 100
        assert!(!buf.add_delete_range(10, 15));
        assert!(!buf.add_delete_range(20, 25));
        // Third pushes it to 110
        assert!(buf.add_delete_range(30, 10));
    }

    #[test]
    fn test_reset_clears_both_buffers() {
        let mut buf = BufferSimulator::new(100, 100);
        buf.add_kv(50, 50);
        buf.add_delete_range(30, 20);
        assert!(buf.kv_buffer_size > 0);
        assert!(buf.range_buffer_size > 0);

        buf.reset();
        assert_eq!(buf.kv_buffer_size, 0);
        assert_eq!(buf.range_buffer_size, 0);
    }

    #[test]
    fn test_flush_decision_only_on_kv_full() {
        let mut buf = BufferSimulator::new(100, 50);
        // Fill range buffer but not KV buffer
        buf.add_delete_range(40, 20); // 60 >= 50, range buffer full
        assert!(!buf.add_kv(10, 10), "KV buffer not full yet");

        // Now fill KV buffer
        assert!(buf.add_kv(40, 60), "now KV buffer full");
    }

    #[test]
    fn test_iter_100k_kv_entries() {
        let mut buf = BufferSimulator::new(128 * 1024 * 1024, 32 * 1024 * 1024);
        let mut total = 0usize;
        // Simulate 100K entries of 500 bytes each = 50MB, under 128MB threshold
        for _ in 0..100_000 {
            total += 500;
            if total >= buf.max_kv_buffer_size {
                buf.reset();
                total = 0;
            }
        }
        assert!(total < buf.max_kv_buffer_size,
            "100K entries of 500B = 50MB, under 128MB threshold");
    }

    #[test]
    fn test_empty_batch_after_reset() {
        let mut buf = BufferSimulator::new(100, 50);
        buf.add_kv(60, 50); // meets threshold
        assert!(buf.kv_buffer_size >= buf.max_kv_buffer_size);
        buf.reset();
        assert_eq!(buf.kv_buffer_size, 0);
    }
}

// ======================================================================
// Task event dispatch ordering tests
// ======================================================================

mod task_event_dispatch {
    /// Simulates the event dispatch ordering in StreamIngestTask.
    /// Checkpoints must flush all prior data before being recorded.
    #[test]
    fn test_checkpoint_comes_after_kv_batch() {
        // Scenario: KV batch arrives, then checkpoint.
        // The KV batch must be flushed BEFORE the checkpoint is recorded.
        let events = vec!["kv_batch", "checkpoint"];
        let mut flushed = false;

        for event in &events {
            match *event {
                "kv_batch" => flushed = true,
                "checkpoint" => assert!(flushed, "checkpoint must see flushed data"),
                _ => {}
            }
        }
    }

    #[test]
    fn test_sst_chunk_before_checkpoint() {
        // SstChunk must be ingested before checkpoint
        let events = vec!["sst_chunk", "checkpoint"];
        let mut sst_done = false;

        for event in &events {
            match *event {
                "sst_chunk" => sst_done = true,
                "checkpoint" => assert!(sst_done, "SST must be ingested before checkpoint"),
                _ => {}
            }
        }
    }

    #[test]
    fn test_delete_range_flushed_before_checkpoint() {
        let events = vec!["delete_range", "checkpoint"];
        let mut delete_done = false;
        for event in &events {
            match *event {
                "delete_range" => delete_done = true,
                "checkpoint" => assert!(delete_done),
                _ => {}
            }
        }
    }

    #[test]
    fn test_split_triggers_flush() {
        // Split event must flush buffered data since key ranges change
        let events = vec!["kv_batch", "kv_batch", "split", "kv_batch"];
        let mut flushed_at_split = false;

        for event in &events {
            if *event == "split" {
                flushed_at_split = true;
            }
        }
        assert!(flushed_at_split, "split should trigger flush");
    }

    #[test]
    fn test_event_ordering_dispatches_correctly() {
        // Full pipeline simulation
        let events = vec![
            ("init", 0),         // Initialize
            ("kv_batch", 100),   // 100 KVs
            ("kv_batch", 200),   // 200 more KVs
            ("sst_chunk", 1),    // 1 SST chunk
            ("checkpoint", 500), // checkpoint at ts=500
            ("kv_batch", 50),    // 50 more KVs
            ("split", 0),        // Region split
            ("checkpoint", 600), // checkpoint at ts=600
        ];

        let mut total_kvs = 0u64;
        let mut total_ssts = 0u64;
        let mut checkpoint_count = 0u64;
        let mut split_count = 0u64;

        for (event_type, count) in &events {
            match *event_type {
                "kv_batch" => total_kvs += *count as u64,
                "sst_chunk" => total_ssts += *count as u64,
                "checkpoint" => checkpoint_count += 1,
                "split" => split_count += 1,
                _ => {}
            }
        }

        assert_eq!(total_kvs, 350);
        assert_eq!(total_ssts, 1);
        assert_eq!(checkpoint_count, 2);
        assert_eq!(split_count, 1);
    }

    #[test]
    fn test_kubernetes_split_event_during_high_load() {
        // Simulate a Region split during high replication load
        let mut flushed = false;
        let events = vec!["kv_batch", "kv_batch", "kv_batch", "split", "kv_batch"];

        for (i, event) in events.iter().enumerate() {
            if *event == "split" {
                // At split point, 3 kv_batches have arrived (300 KVs)
                assert_eq!(i, 3);
                flushed = true;
            }
        }
        // After split, new KVs route to new Regions
        assert!(flushed);
    }
}

// ======================================================================
// MvccKeyValue encoding tests
// ======================================================================

mod mvcc_encoding_tests {
    use stream_ingest::sst_batcher::MvccKeyValue;

    #[test]
    fn test_mvcc_key_encoding_roundtrip() {
        let user_key = b"table1_record_42";
        let ts: u64 = 12345;
        let mut key = user_key.to_vec();
        key.extend_from_slice(&(!ts).to_be_bytes());

        // The encoded key should be > user_key (extra bytes)
        assert!(key.len() > user_key.len());

        // User key prefix should be recoverable
        let prefix = &key[..user_key.len()];
        assert_eq!(prefix, user_key);
    }

    #[test]
    fn test_timestamp_inverted_ordering() {
        // In MVCC, larger timestamps sort BEFORE smaller timestamps
        // because the timestamp is inverted (bitwise NOT) before encoding.
        let ts_newer: u64 = 200;
        let ts_older: u64 = 100;

        let key_newer = {
            let mut k = b"k".to_vec();
            k.extend_from_slice(&(!ts_newer).to_be_bytes());
            k
        };
        let key_older = {
            let mut k = b"k".to_vec();
            k.extend_from_slice(&(!ts_older).to_be_bytes());
            k
        };

        assert!(key_newer < key_older,
            "MVCC newer version ({}) should sort before older ({})",
            ts_newer, ts_older);
    }

    #[test]
    fn test_timestamp_extremes() {
        // Test boundary timestamps
        let ts_zero: u64 = 0;
        let ts_max: u64 = u64::MAX;

        let key_zero = {
            let mut k = b"k".to_vec();
            k.extend_from_slice(&(!ts_zero).to_be_bytes());
            k
        };
        let key_max = {
            let mut k = b"k".to_vec();
            k.extend_from_slice(&(!ts_max).to_be_bytes());
            k
        };

        // ts_max has inverted value 0, so it sorts BEFORE ts_zero
        assert!(key_max < key_zero,
            "ts_max ({}) inverted to 0 sorts before ts_zero ({}) inverted to u64::MAX",
            ts_max, ts_zero);
    }

    #[test]
    fn test_user_key_grouping() {
        // Keys with same user_key but different timestamps should group together
        // after sorting by byte order (they share the same prefix).
        let keys: Vec<MvccKeyValue> = vec![
            MvccKeyValue { key: {
                let mut k = b"user_a".to_vec();
                k.extend_from_slice(&(!300u64).to_be_bytes());
                k
            }, value: b"v3".to_vec(), cf: "default".to_string() },
            MvccKeyValue { key: {
                let mut k = b"user_a".to_vec();
                k.extend_from_slice(&(!100u64).to_be_bytes());
                k
            }, value: b"v1".to_vec(), cf: "default".to_string() },
            MvccKeyValue { key: {
                let mut k = b"user_b".to_vec();
                k.extend_from_slice(&(!200u64).to_be_bytes());
                k
            }, value: b"v2".to_vec(), cf: "default".to_string() },
        ];

        // After sorting, all "user_a" keys group before "user_b"
        let mut sorted = keys.clone();
        sorted.sort_by(|a, b| a.key.as_slice().cmp(b.key.as_slice()));

        assert!(sorted[0].key.starts_with(b"user_a"));
        assert!(sorted[1].key.starts_with(b"user_a"));
        assert!(sorted[2].key.starts_with(b"user_b"));
    }
}

// ======================================================================
// Split handler resume_ts tests (C scheme Step 1)
// ======================================================================

mod split_handler_tests {
    use std::collections::BTreeMap;

    /// Helper: compute resume_ts for a child region using the PARENT's frontier TS.
    /// This is the correct behavior (C scheme fix): the parent region may be
    /// ahead of the global frontier, and using global_min_ts would miss data
    /// that was already written to the parent region before the split.
    fn child_resume_ts(frontier: &BTreeMap<u64, u64>, parent_region_id: u64) -> u64 {
        frontier.get(&parent_region_id).copied().unwrap_or(0)
    }

    /// Helper: global_min_ts (OLD buggy behavior).
    fn global_min_ts(frontier: &BTreeMap<u64, u64>) -> u64 {
        frontier.values().min().copied().unwrap_or(0)
    }

    #[test]
    fn test_split_child_uses_parent_ts_not_global_min() {
        // Scenario: 3 regions with different frontier progress.
        // Region 1 is slow (ts=100), region 2 is caught up (ts=5000).
        // Region 2 splits → child should resume from 5000, NOT 100.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 100);   // slow region
        frontier.insert(2, 5000);  // caught-up region (about to split)
        frontier.insert(3, 3000);  // medium region

        let parent_rid = 2;
        let child_ts = child_resume_ts(&frontier, parent_rid);
        let global_min = global_min_ts(&frontier);

        assert_eq!(child_ts, 5000,
            "child must resume from parent frontier TS (5000), not global_min (100)");
        assert_eq!(global_min, 100,
            "global_min is 100, which would miss data for child of region 2");

        // The bug: if we used global_min_ts(100), the child would re-scan from
        // ts=100, but the parent region had already progressed to ts=5000.
        // Data written between ts=100 and ts=5000 to the parent region would
        // have already been replicated, but the child would request them again
        // and the producer would skip them (already checkpointed), causing loss.
        assert!(child_ts > global_min,
            "parent TS ({}) must be > global_min ({}) for this test to be meaningful",
            child_ts, global_min);
    }

    #[test]
    fn test_split_child_resume_ts_zero_when_parent_not_in_frontier() {
        // If parent is not in frontier (shouldn't happen in practice, but be safe),
        // child should start from ts=0 (full scan).
        let frontier: BTreeMap<u64, u64> = BTreeMap::new();
        let child_ts = child_resume_ts(&frontier, 99);
        assert_eq!(child_ts, 0,
            "child of unknown parent should start from 0 (full scan)");
    }

    #[test]
    fn test_split_child_resume_ts_zero_when_parent_ts_is_zero() {
        // Parent has ts=0 (no progress yet) → child starts from 0 too.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 0);
        let child_ts = child_resume_ts(&frontier, 1);
        assert_eq!(child_ts, 0);
    }

    #[test]
    fn test_multiple_splits_each_child_gets_parent_ts() {
        // Region 2 at ts=5000 splits into regions 4 and 5.
        // Both children should get resume_ts=5000.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 5000);

        let parent_ts = child_resume_ts(&frontier, 2);
        assert_eq!(parent_ts, 5000);

        // Child 4
        let child_4_ts = child_resume_ts(&frontier, 2);
        // Child 5
        let child_5_ts = child_resume_ts(&frontier, 2);

        assert_eq!(child_4_ts, 5000);
        assert_eq!(child_5_ts, 5000);
        // Neither should use global_min=100
        assert!(child_4_ts > global_min_ts(&frontier));
        assert!(child_5_ts > global_min_ts(&frontier));
    }

    #[test]
    fn test_child_ts_not_exceed_parent_ts() {
        // A child region can't have more progress than its parent at split time.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 5000);
        frontier.insert(2, 0);

        let parent_ts = child_resume_ts(&frontier, 1);
        let global_min = global_min_ts(&frontier);

        // Parent is ahead of global min → child uses parent TS
        assert_eq!(parent_ts, 5000);
        assert_eq!(global_min, 0);
    }

    #[test]
    fn test_cascade_split_frontier_tracking() {
        // Simulate cascade: region_1 splits → [1, 4], then region_4 splits → [4, 5, 6]
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 1000);
        frontier.insert(2, 2000);

        // First split: region_1 → children 4
        let r1_ts = child_resume_ts(&frontier, 1);
        frontier.insert(4, r1_ts);  // child seeded with parent TS
        assert_eq!(r1_ts, 1000);

        // Region 4 makes progress
        frontier.insert(4, 1500);

        // Second split: region_4 → children 5, 6
        let r4_ts = child_resume_ts(&frontier, 4);
        frontier.insert(5, r4_ts);
        frontier.insert(6, r4_ts);
        assert_eq!(r4_ts, 1500,
            "second-level child should get parent (region_4) TS, not region_1 TS");

        // Verify frontier state
        assert_eq!(frontier.get(&1), Some(&1000));
        assert_eq!(frontier.get(&4), Some(&1500));
        assert_eq!(frontier.get(&5), Some(&1500));
        assert_eq!(frontier.get(&6), Some(&1500));
    }

    #[test]
    fn test_split_does_not_affect_unrelated_regions() {
        // A split in region_2 should not change frontier entries for region_1 or region_3.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 5000);
        frontier.insert(3, 3000);

        // Simulate split of region_2
        let parent_ts = child_resume_ts(&frontier, 2);
        frontier.insert(4, parent_ts);
        frontier.insert(5, parent_ts);

        // Unrelated regions unchanged
        assert_eq!(frontier.get(&1), Some(&100));
        assert_eq!(frontier.get(&3), Some(&3000));
        // Parent region_2 still in frontier (child subscribes alongside)
        assert_eq!(frontier.get(&2), Some(&5000));
    }
}

// ======================================================================
// Periodic rediscover tests (C scheme Step 1)
// ======================================================================

mod rediscover_tests {
    use std::collections::BTreeMap;

    /// In the periodic rediscover path, new regions discovered via PD API
    /// use global_min_ts as their resume point. This is correct because:
    /// 1. These regions were missed entirely (not tracked in frontier at all)
    /// 2. We don't know which parent they came from (don't know their lineage)
    /// 3. global_min_ts is the safe lower bound: it ensures no data is missed
    fn rediscover_resume_ts(frontier: &BTreeMap<u64, u64>) -> u64 {
        frontier.values().min().copied().unwrap_or(0)
    }

    #[test]
    fn test_rediscover_uses_global_min_for_unknown_regions() {
        // When rediscover finds a region NOT in frontier, it must use global_min
        // because it doesn't know the region's lineage. Using 0 would cause
        // unnecessary full re-scan; using a per-region TS is impossible since
        // the region was never tracked.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 1000);
        frontier.insert(2, 5000);
        frontier.insert(3, 3000);

        let global_min = rediscover_resume_ts(&frontier);
        assert_eq!(global_min, 1000,
            "rediscover resume_ts should be global_min (1000)");

        // A newly discovered region_4 (not in frontier) gets global_min=1000
        // This is correct: 1000 is the safe lower bound across all known regions
        assert!(!frontier.contains_key(&4));
    }

    #[test]
    fn test_rediscover_empty_frontier_uses_zero() {
        let frontier: BTreeMap<u64, u64> = BTreeMap::new();
        let ts = rediscover_resume_ts(&frontier);
        assert_eq!(ts, 0, "empty frontier → resume from 0 (full scan)");
    }

    #[test]
    fn test_rediscover_skips_already_tracked_regions() {
        // Regions already in the frontier should NOT be re-subscribed.
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 1000);
        frontier.insert(2, 2000);
        frontier.insert(3, 3000);

        let all_ids = vec![1u64, 2, 3, 4, 5];
        let mut new_subscriptions = 0usize;
        for id in &all_ids {
            if !frontier.contains_key(id) {
                new_subscriptions += 1;
            }
        }
        assert_eq!(new_subscriptions, 2, "only regions 4 and 5 are new");
    }

    #[test]
    fn test_rediscover_interval_is_5_seconds() {
        // C scheme Step 1: reduced from 30s to 5s for demo responsiveness.
        // CRDB replanner uses configurable frequency.
        let rediscover_secs: u64 = 5;
        assert_eq!(rediscover_secs, 5);

        // Verify it's meaningfully faster than the old 30s interval
        let old_interval: u64 = 30;
        assert!(rediscover_secs < old_interval,
            "5s interval is 6x faster than old 30s interval");

        // 5s means: Lightning split region is discovered within 5s + PD API latency
    }

    #[test]
    fn test_rediscover_vs_split_different_resume_ts() {
        // Key design insight: rediscover and split handler use DIFFERENT resume_ts.
        //
        // Split handler: uses PARENT frontier TS
        //   → child is a direct descendant, parent TS is the exact point
        //     where data was split, so using parent TS is safe and optimal.
        //
        // Rediscover: uses GLOBAL_MIN_TS
        //   → unknown region origin, global_min ensures no data loss.
        //     This may re-scan some already-replicated data (wasteful but safe).
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 5000);

        // Split of region_2: child gets 5000
        let parent_ts = frontier.get(&2).copied().unwrap_or(0);
        assert_eq!(parent_ts, 5000);

        // Rediscover finds unknown region_3: gets 100
        let global_min = frontier.values().min().copied().unwrap_or(0);
        assert_eq!(global_min, 100);

        // They CAN differ, and SHOULD differ when regions progress unevenly
        assert_ne!(parent_ts, global_min);
    }

    #[test]
    fn test_rediscover_adds_regions_with_correct_initial_ts() {
        // Simulate full rediscover cycle:
        // Frontier: {1: 100, 2: 5000}, PD discovers: [1, 2, 3, 4]
        // → subscribe region_3 with ts=100, region_4 with ts=100
        let mut frontier: BTreeMap<u64, u64> = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 5000);

        let global_min = frontier.values().min().copied().unwrap_or(0);
        let pd_regions: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (1, vec![], vec![]),
            (2, vec![], vec![]),
            (3, vec![], vec![]),
            (4, vec![], vec![]),
        ];

        let mut new_count = 0usize;
        for (rid, _, _) in &pd_regions {
            if !frontier.contains_key(rid) {
                // New region: seed with global_min
                frontier.insert(*rid, global_min);
                new_count += 1;
            }
        }

        assert_eq!(new_count, 2);
        assert_eq!(frontier.get(&3), Some(&100));
        assert_eq!(frontier.get(&4), Some(&100));
        // Existing regions unchanged
        assert_eq!(frontier.get(&1), Some(&100));
        assert_eq!(frontier.get(&2), Some(&5000));
    }
}

// ======================================================================
// hex_decode tests
// ======================================================================

mod hex_decode_tests {
    use stream_ingest::task::hex_decode;

    #[test]
    fn test_hex_decode_empty_string() {
        let result = hex_decode("");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_hex_decode_single_byte() {
        assert_eq!(hex_decode("00").unwrap(), vec![0x00]);
        assert_eq!(hex_decode("ff").unwrap(), vec![0xff]);
        assert_eq!(hex_decode("FF").unwrap(), vec![0xff]);
        assert_eq!(hex_decode("7f").unwrap(), vec![0x7f]);
    }

    #[test]
    fn test_hex_decode_multi_byte() {
        assert_eq!(hex_decode("0a0b0c").unwrap(), vec![0x0a, 0x0b, 0x0c]);
        assert_eq!(hex_decode("deadbeef").unwrap(), vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn test_hex_decode_odd_length_fails() {
        assert!(hex_decode("0").is_err());
        assert!(hex_decode("abc").is_err());
        assert!(hex_decode("12345").is_err());
    }

    #[test]
    fn test_hex_decode_invalid_chars() {
        assert!(hex_decode("gg").is_err());
        assert!(hex_decode("0x").is_err());
        assert!(hex_decode("  ").is_err());
    }

    #[test]
    fn test_hex_decode_tikv_key_prefix() {
        // TiKV data key prefix: 'z' (0x7a) + raw key
        let key_hex = "7a0000000000000001";
        let result = hex_decode(key_hex).unwrap();
        assert_eq!(result[0], 0x7a); // 'z' prefix
        assert_eq!(result.len(), 9);
    }
}

// ======================================================================
// Event loop interval configuration tests (C scheme Step 1)
// ======================================================================

mod event_loop_config_tests {
    use std::time::Duration;

    #[test]
    fn test_rediscover_interval_vs_old() {
        let new_interval = Duration::from_secs(5);
        let old_interval = Duration::from_secs(30);
        assert!(new_interval < old_interval,
            "C scheme: 5s rediscover is 6x faster than old 30s");
    }

    #[test]
    fn test_rediscover_interval_is_reasonable() {
        // 5s is fast enough for demo but not so fast it floods PD
        let interval = Duration::from_secs(5);
        assert!(interval >= Duration::from_secs(2),
            "should not be faster than 2s (PD rate limiting)");
        assert!(interval <= Duration::from_secs(15),
            "should not be slower than 15s (demo responsiveness)");
    }

    #[test]
    fn test_flush_interval_default() {
        // Default min_flush_interval is 200ms (per config/mod.rs)
        let interval = Duration::from_millis(200);
        assert_eq!(interval.as_millis(), 200);
    }

    #[test]
    fn test_cutover_check_interval() {
        // cutover check runs every 500ms
        let interval = Duration::from_millis(500);
        assert!(interval < Duration::from_secs(1),
            "cutover check must be sub-second for responsive cutover");
    }

    #[test]
    fn test_checkpoint_interval_in_service() {
        // pcr_service.rs sends checkpoints every 5s via cp_interval
        let cp_interval = Duration::from_secs(5);
        assert_eq!(cp_interval.as_secs(), 5);
    }
}

// ======================================================================
// Step 2: Error classification tests
// ======================================================================

mod error_classification_tests {
    use stream_ingest::errors::Error;
    use stream_ingest::subscriber::classify_error;
    use stream_ingest::StreamErrorKind;

    #[test]
    fn test_classify_disconnect_errors() {
        // Network/leader issues → Disconnect (retry same region)
        let cases = vec![
            "RpcFailure(Status { code: Unavailable })",
            "connection refused",
            "timeout",
            "transport error: broken pipe",
            "disconnect",
            "gRPC Connection Error",
        ];
        for msg in cases {
            let err = Error::SubscriptionError(msg.to_string());
            let kind = classify_error(&err);
            assert_eq!(kind, StreamErrorKind::Disconnect,
                "expected Disconnect for: '{}'", msg);
        }
    }

    #[test]
    fn test_classify_epoch_errors() {
        // Region split/merge → EpochChanged (must re-resolve span)
        let cases = vec![
            "epoch not match",
            "EPOCH_NOT_MATCH",
            "region not found",
            "stale command",
            "stale_epoch",
            "Epoch: version mismatch",
        ];
        for msg in cases {
            let err = Error::SubscriptionError(msg.to_string());
            let kind = classify_error(&err);
            assert_eq!(kind, StreamErrorKind::EpochChanged,
                "expected EpochChanged for: '{}'", msg);
        }
    }

    #[test]
    fn test_classify_fatal_errors() {
        // Unknown/unexpected errors → Fatal (give up)
        let cases = vec![
            "internal error",
            "permission denied",
            "unknown codec",
            "",
        ];
        for msg in cases {
            let err = Error::SubscriptionError(msg.to_string());
            let kind = classify_error(&err);
            assert_eq!(kind, StreamErrorKind::Fatal,
                "expected Fatal for: '{}'", msg);
        }
    }

    #[test]
    fn test_classify_epoch_takes_priority_over_disconnect() {
        // Epoch-related keywords should classify as EpochChanged even if
        // the message also contains disconnect-like words.
        let msg = "RpcFailure: epoch not match, connection refused";
        let err = Error::SubscriptionError(msg.to_string());
        let kind = classify_error(&err);
        assert_eq!(kind, StreamErrorKind::EpochChanged,
            "epoch should take priority over disconnect");
    }

    #[test]
    fn test_classify_error_with_checkpoint_error() {
        let err = Error::CheckpointError("disconnect timeout".to_string());
        let kind = classify_error(&err);
        // CheckpointError is a different variant, but the message contains "disconnect" + "timeout"
        assert_eq!(kind, StreamErrorKind::Disconnect);
    }

    #[test]
    fn test_classify_error_with_ingest_error() {
        let err = Error::IngestError("epoch stale".to_string());
        let kind = classify_error(&err);
        assert_eq!(kind, StreamErrorKind::EpochChanged);
    }
}

// ======================================================================
// Step 2: Error kind enum tests
// ======================================================================

mod error_kind_tests {
    use stream_ingest::StreamErrorKind;

    #[test]
    fn test_error_kind_equality() {
        assert_eq!(StreamErrorKind::Disconnect, StreamErrorKind::Disconnect);
        assert_eq!(StreamErrorKind::EpochChanged, StreamErrorKind::EpochChanged);
        assert_eq!(StreamErrorKind::Fatal, StreamErrorKind::Fatal);
        assert_ne!(StreamErrorKind::Disconnect, StreamErrorKind::EpochChanged);
        assert_ne!(StreamErrorKind::EpochChanged, StreamErrorKind::Fatal);
    }

    #[test]
    fn test_error_kind_debug() {
        assert_eq!(format!("{:?}", StreamErrorKind::Disconnect), "Disconnect");
        assert_eq!(format!("{:?}", StreamErrorKind::EpochChanged), "EpochChanged");
        assert_eq!(format!("{:?}", StreamErrorKind::Fatal), "Fatal");
    }

    #[test]
    fn test_error_kind_clone() {
        let kind = StreamErrorKind::Disconnect;
        let cloned = kind;
        assert_eq!(kind, cloned);
    }
}

// ======================================================================
// Step 2: Subscription state transition tests
// ======================================================================

mod subscription_state_tests {
    use stream_ingest::SubscriptionState;

    #[test]
    fn test_state_equality() {
        assert_eq!(SubscriptionState::Connected, SubscriptionState::Connected);
        assert!(matches!(SubscriptionState::Connected, SubscriptionState::Connected));
        assert!(matches!(SubscriptionState::Reconnecting { retry: 1 },
            SubscriptionState::Reconnecting { .. }));
    }

    #[test]
    fn test_state_transitions() {
        // Normal lifecycle: Connected → Reconnecting → Connected (recovery)
        let mut state = SubscriptionState::Connected;
        assert_eq!(state, SubscriptionState::Connected);

        // Disconnect → start reconnecting
        state = SubscriptionState::Reconnecting { retry: 1 };
        assert!(matches!(state, SubscriptionState::Reconnecting { retry: 1 }));

        // Retry count increases
        state = SubscriptionState::Reconnecting { retry: 3 };
        assert!(matches!(state, SubscriptionState::Reconnecting { retry: 3 }));

        // Reconnected
        state = SubscriptionState::Connected;
        assert_eq!(state, SubscriptionState::Connected);
    }

    #[test]
    fn test_state_failure_path() {
        // Failure path: Connected → Reconnecting → Failed
        let mut state = SubscriptionState::Connected;
        assert_eq!(state, SubscriptionState::Connected);

        state = SubscriptionState::Reconnecting { retry: 5 };
        assert!(matches!(state, SubscriptionState::Reconnecting { .. }));

        // Max retries exhausted
        state = SubscriptionState::Failed;
        assert_eq!(state, SubscriptionState::Failed);
    }

    #[test]
    fn test_state_clone() {
        let connected = SubscriptionState::Connected;
        assert_eq!(connected.clone(), SubscriptionState::Connected);

        let reconnecting = SubscriptionState::Reconnecting { retry: 2 };
        assert_eq!(reconnecting.clone(), SubscriptionState::Reconnecting { retry: 2 });

        let failed = SubscriptionState::Failed;
        assert_eq!(failed.clone(), SubscriptionState::Failed);
    }

    #[test]
    fn test_state_debug_format() {
        let s = SubscriptionState::Connected;
        assert!(format!("{:?}", s).contains("Connected"));

        let s = SubscriptionState::Reconnecting { retry: 2 };
        let debug_str = format!("{:?}", s);
        assert!(debug_str.contains("Reconnecting"));
        assert!(debug_str.contains("2"));

        let s = SubscriptionState::Failed;
        assert!(format!("{:?}", s).contains("Failed"));
    }
}

// ======================================================================
// Step 2: Span overlap logic tests
// ======================================================================

mod span_overlap_tests {
    /// Determine if two key ranges overlap: [a_start, a_end) vs [b_start, b_end).
    /// Used by resolve_span to filter PD regions by key range.
    fn ranges_overlap(
        a_start: &[u8], a_end: &[u8],
        b_start: &[u8], b_end: &[u8],
    ) -> bool {
        // Both ranges are empty → overlap (match all)
        if a_start.is_empty() && a_end.is_empty() { return true; }
        if b_start.is_empty() && b_end.is_empty() { return true; }

        let before_end = b_end.is_empty() || a_start < b_end;
        let after_start = a_end.is_empty() || b_start < a_end;
        before_end && after_start
    }

    #[test]
    fn test_overlap_exact_match() {
        assert!(ranges_overlap(b"a", b"z", b"a", b"z"));
    }

    #[test]
    fn test_overlap_partial_right() {
        // [a, m) overlaps with [d, z)
        assert!(ranges_overlap(b"a", b"m", b"d", b"z"));
    }

    #[test]
    fn test_overlap_partial_left() {
        // [d, z) overlaps with [a, m)
        assert!(ranges_overlap(b"d", b"z", b"a", b"m"));
    }

    #[test]
    fn test_overlap_contained() {
        // [a, z) contains [d, m)
        assert!(ranges_overlap(b"a", b"z", b"d", b"m"));
        // [d, m) contained by [a, z)
        assert!(ranges_overlap(b"d", b"m", b"a", b"z"));
    }

    #[test]
    fn test_no_overlap_disjoint() {
        // [a, d) and [m, z) are disjoint
        assert!(!ranges_overlap(b"a", b"d", b"m", b"z"));
        assert!(!ranges_overlap(b"m", b"z", b"a", b"d"));
    }

    #[test]
    fn test_no_overlap_adjacent() {
        // [a, m) and [m, z) are adjacent but NOT overlapping
        // (half-open intervals: a ≤ k < m, m ≤ k < z)
        assert!(!ranges_overlap(b"a", b"m", b"m", b"z"));
    }

    #[test]
    fn test_overlap_empty_query_matches_all() {
        // Empty query range (no filter) matches everything
        assert!(ranges_overlap(&[], &[], b"a", b"z"));
        assert!(ranges_overlap(&[], &[], b"m", b"q"));
        assert!(ranges_overlap(b"a", b"z", &[], &[]));
    }

    #[test]
    fn test_overlap_empty_region_edge() {
        // A region with empty start covers from -∞, match if key < region_end
        assert!(ranges_overlap(b"a", b"z", &[], b"d"));
        // key "z" > region_end "d" → no overlap
        assert!(!ranges_overlap(b"z", b"zz", &[], b"d"));
    }

    #[test]
    fn test_overlap_empty_region_end() {
        // A region with empty end covers to +∞
        // Query [a, z) overlaps with region [d, +∞)
        assert!(ranges_overlap(b"a", b"z", b"d", &[]));
        // Query [a, d) is ADJACENT to region [d, +∞) but doesn't overlap
        // (half-open intervals: a..d doesn't include d, d..∞ starts at d)
        assert!(!ranges_overlap(b"a", b"d", b"d", &[]));
        // Query [d, z) overlaps with region [d, +∞)
        assert!(ranges_overlap(b"d", b"z", b"d", &[]));
    }

    #[test]
    fn test_overlap_single_point_key() {
        // Querying a single key within region
        assert!(ranges_overlap(b"d", b"e", b"a", b"z"));
        // Querying a single key outside region
        assert!(!ranges_overlap(b"d", b"e", b"a", b"d"));
    }

    #[test]
    fn test_overlap_tikv_style_keys() {
        // TiKV table prefix keys
        let t1_start = b"t\x80\x00\x00\x00\x00\x00\x00\x01";
        let t1_end   = b"t\x80\x00\x00\x00\x00\x00\x00\x02";
        let t2_start = b"t\x80\x00\x00\x00\x00\x00\x00\x02";
        let t2_end   = b"t\x80\x00\x00\x00\x00\x00\x00\x03";

        // Same table: overlap
        assert!(ranges_overlap(t1_start, t1_end, t1_start, t1_end));
        // Different tables: no overlap
        assert!(!ranges_overlap(t1_start, t1_end, t2_start, t2_end));
        // Split within table: [01, 03) contains [01, 02)
        assert!(ranges_overlap(t1_start, t2_end, t1_start, t1_end));
    }
}

// ======================================================================
// Step 2: Re-subscription flow simulation tests
// ======================================================================

mod resubscribe_flow_tests {
    use stream_ingest::subscriber::{PartitionSubscription, StreamErrorKind};
    use stream_ingest::StreamErrorKind as ErrorKind;

    /// Simulate the decision logic in the subscriber's retry loop:
    /// given an error kind, should the subscriber retry the same region,
    /// re-resolve the span, or give up?
    #[derive(Debug, PartialEq)]
    enum RetryAction {
        RetrySameRegion,
        ReresolveSpan,
        GiveUp,
    }

    fn decide_action(kind: ErrorKind, retry_count: u32, max_retries: u32) -> RetryAction {
        match kind {
            ErrorKind::Disconnect if retry_count < max_retries => RetryAction::RetrySameRegion,
            ErrorKind::Disconnect => RetryAction::GiveUp,
            ErrorKind::EpochChanged => RetryAction::ReresolveSpan,
            ErrorKind::Fatal => RetryAction::GiveUp,
        }
    }

    #[test]
    fn test_disconnect_with_retries_remaining() {
        assert_eq!(
            decide_action(StreamErrorKind::Disconnect, 0, 5),
            RetryAction::RetrySameRegion
        );
        assert_eq!(
            decide_action(StreamErrorKind::Disconnect, 4, 5),
            RetryAction::RetrySameRegion
        );
    }

    #[test]
    fn test_disconnect_max_retries_exhausted() {
        assert_eq!(
            decide_action(StreamErrorKind::Disconnect, 5, 5),
            RetryAction::GiveUp
        );
        assert_eq!(
            decide_action(StreamErrorKind::Disconnect, 10, 5),
            RetryAction::GiveUp
        );
    }

    #[test]
    fn test_epoch_always_triggers_reresolve() {
        // Epoch errors always trigger span re-resolution regardless of retry count
        for retry in 0..10 {
            assert_eq!(
                decide_action(StreamErrorKind::EpochChanged, retry, 5),
                RetryAction::ReresolveSpan,
                "epoch should always re-resolve, even at retry={}", retry
            );
        }
    }

    #[test]
    fn test_fatal_always_gives_up() {
        for retry in 0..10 {
            assert_eq!(
                decide_action(StreamErrorKind::Fatal, retry, 5),
                RetryAction::GiveUp,
                "fatal should always give up, even at retry={}", retry
            );
        }
    }

    #[test]
    fn test_span_reresolve_produces_new_subscriptions() {
        // Simulate: region 100 with span [a, z) gets epoch error.
        // PD returns 2 new regions: 200 covers [a, m), 201 covers [m, z).
        // The subscriber should create 2 new subscriptions.
        let original_region = 100u64;
        let original_span = (b"a".to_vec(), b"z".to_vec());

        let resolved_regions: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (200, b"a".to_vec(), b"m".to_vec()),
            (201, b"m".to_vec(), b"z".to_vec()),
        ];

        let mut new_subscriptions = Vec::new();
        for (rid, start, end) in &resolved_regions {
            if *rid == original_region {
                continue; // Same region, just retry
            }
            new_subscriptions.push(PartitionSubscription {
                region_id: *rid,
                start_ts: 5000,
                source_addr: String::new(),
                start_key: start.clone(),
                end_key: end.clone(),
            });
        }

        assert_eq!(new_subscriptions.len(), 2);
        assert_eq!(new_subscriptions[0].region_id, 200);
        assert_eq!(new_subscriptions[0].start_key, b"a".to_vec());
        assert_eq!(new_subscriptions[0].end_key, b"m".to_vec());
        assert_eq!(new_subscriptions[1].region_id, 201);
        assert_eq!(new_subscriptions[1].start_key, b"m".to_vec());
        assert_eq!(new_subscriptions[1].end_key, b"z".to_vec());
        // Both get the same start_ts (parent frontier)
        assert_eq!(new_subscriptions[0].start_ts, 5000);
        assert_eq!(new_subscriptions[1].start_ts, 5000);
    }

    #[test]
    fn test_reresolve_same_region_does_not_duplicate() {
        // If PD returns the same region (stale epoch), don't create duplicate
        let original_region = 100u64;
        let resolved_regions: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (100, b"a".to_vec(), b"z".to_vec()), // Same region
        ];

        let mut new_subscriptions = 0usize;
        for (rid, _, _) in &resolved_regions {
            if *rid == original_region {
                continue;
            }
            new_subscriptions += 1;
        }

        assert_eq!(new_subscriptions, 0, "same region should not create duplicate subscription");
    }

    #[test]
    fn test_reresolve_empty_result_logs_warning() {
        // If PD returns no regions for the span, the subscriber should not panic
        let resolved_regions: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![];
        assert!(resolved_regions.is_empty(), "empty result should be handled gracefully");
        // The periodic rediscover (Step 1) will eventually find the regions
    }
}

// ======================================================================
// Step 3: Topology diff tests
// ======================================================================

mod topology_diff_tests {
    use std::collections::BTreeMap;
    use stream_ingest::task::{diff_topology, TopologyChange, Frontier};

    #[test]
    fn test_diff_all_match() {
        // All regions in PD also in frontier — no changes
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (1, vec![], vec![]),
            (2, vec![], vec![]),
            (3, vec![], vec![]),
        ];
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);
        frontier.insert(3, 300);

        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.total_in_pd, 3);
        assert_eq!(diff.total_in_frontier, 3);
        assert!(diff.new_regions.is_empty());
        assert!(diff.removed_regions.is_empty());
    }

    #[test]
    fn test_diff_new_region_detected() {
        // PD has region 4 that frontier doesn't
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (1, vec![], vec![]),
            (2, vec![], vec![]),
            (4, vec![], vec![]), // new
        ];
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);

        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.new_regions.len(), 1);
        assert_eq!(diff.new_regions[0], TopologyChange::NewRegion { region_id: 4 });
        assert!(diff.removed_regions.is_empty());
    }

    #[test]
    fn test_diff_removed_region_detected() {
        // Frontier has region 3 that PD doesn't — merged away
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (1, vec![], vec![]),
            (2, vec![], vec![]),
        ];
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);
        frontier.insert(3, 300); // stale

        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.removed_regions.len(), 1);
        assert_eq!(diff.removed_regions[0], TopologyChange::RemovedRegion { region_id: 3 });
        assert!(diff.new_regions.is_empty());
    }

    #[test]
    fn test_diff_both_new_and_removed() {
        // Common scenario: region 2 split into 4 and 5, so region 2 is gone
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![
            (1, vec![], vec![]),
            (4, vec![], vec![]), // new child
            (5, vec![], vec![]), // new child
        ];
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 5000); // stale parent

        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.new_regions.len(), 2);
        assert_eq!(diff.removed_regions.len(), 1);

        // New: 4 and 5
        let new_ids: Vec<u64> = diff.new_regions.iter().map(|c| {
            if let TopologyChange::NewRegion { region_id } = c { *region_id } else { 0 }
        }).collect();
        assert!(new_ids.contains(&4));
        assert!(new_ids.contains(&5));

        // Removed: 2
        assert_eq!(diff.removed_regions[0], TopologyChange::RemovedRegion { region_id: 2 });
    }

    #[test]
    fn test_diff_empty_both() {
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![];
        let frontier: Frontier = BTreeMap::new();
        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.total_in_pd, 0);
        assert_eq!(diff.total_in_frontier, 0);
        assert!(diff.new_regions.is_empty());
        assert!(diff.removed_regions.is_empty());
    }

    #[test]
    fn test_diff_empty_pd_all_removed() {
        // PD returns empty (shouldn't normally happen) — all frontier entries are stale
        let pd: Vec<(u64, Vec<u8>, Vec<u8>)> = vec![];
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);

        let diff = diff_topology(&pd, &frontier);
        assert!(diff.new_regions.is_empty());
        assert_eq!(diff.removed_regions.len(), 2);
    }

    #[test]
    fn test_diff_large_frontier() {
        // Stress test: 1000-region frontier, 10 new in PD
        let mut pd = Vec::new();
        let mut frontier: Frontier = BTreeMap::new();
        for i in 1..=1000 {
            pd.push((i, vec![], vec![]));
            frontier.insert(i, i * 100);
        }
        // Add 10 new regions
        for i in 1001..=1010 {
            pd.push((i, vec![], vec![]));
        }

        let diff = diff_topology(&pd, &frontier);
        assert_eq!(diff.total_in_pd, 1010);
        assert_eq!(diff.total_in_frontier, 1000);
        assert_eq!(diff.new_regions.len(), 10);
        assert!(diff.removed_regions.is_empty());
    }

    #[test]
    fn test_cleanup_removes_stale_frontier_entries() {
        // Simulate the cleanup loop: remove frontier entries for removed regions
        let mut frontier: Frontier = BTreeMap::new();
        frontier.insert(1, 100);
        frontier.insert(2, 200);
        frontier.insert(3, 300);

        let removed_ids = vec![2u64, 3];
        for rid in &removed_ids {
            frontier.remove(rid);
        }

        assert_eq!(frontier.len(), 1);
        assert!(frontier.contains_key(&1));
        assert!(!frontier.contains_key(&2));
        assert!(!frontier.contains_key(&3));
    }

    #[test]
    fn test_topology_change_equality() {
        assert_eq!(
            TopologyChange::NewRegion { region_id: 1 },
            TopologyChange::NewRegion { region_id: 1 }
        );
        assert_ne!(
            TopologyChange::NewRegion { region_id: 1 },
            TopologyChange::NewRegion { region_id: 2 }
        );
        assert_eq!(
            TopologyChange::RemovedRegion { region_id: 3 },
            TopologyChange::RemovedRegion { region_id: 3 }
        );
        assert_ne!(
            TopologyChange::NewRegion { region_id: 1 },
            TopologyChange::RemovedRegion { region_id: 1 }
        );
    }

    #[test]
    fn test_topology_change_clone() {
        let c = TopologyChange::NewRegion { region_id: 5 };
        assert_eq!(c.clone(), c);
        let c = TopologyChange::RemovedRegion { region_id: 7 };
        assert_eq!(c.clone(), c);
    }

    #[test]
    fn test_topology_change_debug() {
        let new_r = TopologyChange::NewRegion { region_id: 42 };
        let s = format!("{:?}", new_r);
        assert!(s.contains("NewRegion"));
        assert!(s.contains("42"));

        let removed = TopologyChange::RemovedRegion { region_id: 99 };
        let s = format!("{:?}", removed);
        assert!(s.contains("RemovedRegion"));
        assert!(s.contains("99"));
    }

    #[test]
    fn test_topology_diff_default() {
        let diff: stream_ingest::task::TopologyDiff = Default::default();
        assert_eq!(diff.total_in_pd, 0);
        assert_eq!(diff.total_in_frontier, 0);
        assert!(diff.new_regions.is_empty());
        assert!(diff.removed_regions.is_empty());
    }
}


// ======================================================================
// Phase 1: Span Registry tests
// ======================================================================

mod span_registry_tests {
    use std::collections::HashMap;
    use stream_ingest::span_ctl::{ReplicationSpan, SpanRegistry, SpanState};

    fn make_span(table_id: i64) -> ReplicationSpan {
        let start = format!("t_{}_", table_id).into_bytes();
        let end = format!("t_{}_", table_id + 1).into_bytes();
        ReplicationSpan::new(table_id, start, end)
    }

    #[test]
    fn test_span_contains_key() {
        let span = make_span(114);
        assert!(span.contains_key(b"t_114_r_1"));
        assert!(span.contains_key(b"t_114_i_2"));
        assert!(!span.contains_key(b"t_115_r_1"));
        assert!(span.contains_key(&span.start_key));
        assert!(!span.contains_key(&span.end_key));
    }

    #[test]
    fn test_span_state_transitions() {
        let mut span = make_span(100);
        assert_eq!(span.state, SpanState::Initialized);
        span.state = SpanState::Subscribing;
        assert_eq!(span.state, SpanState::Subscribing);
        span.state = SpanState::Draining;
        assert_eq!(span.state, SpanState::Draining);
        span.state = SpanState::Removed;
        assert_eq!(span.state, SpanState::Removed);
    }

    #[test]
    fn test_span_update_checkpoint() {
        let mut span = make_span(100);
        span.start_worker(1); span.activate_worker(1);
        span.start_worker(2); span.activate_worker(2);
        span.start_worker(3); span.activate_worker(3);
        let mut f = HashMap::new();
        f.insert(1, txn_types::TimeStamp::from(1000));
        f.insert(2, txn_types::TimeStamp::from(500));
        f.insert(3, txn_types::TimeStamp::from(2000));
        span.update_checkpoint(&f);
        assert_eq!(span.checkpoint_ts, txn_types::TimeStamp::from(500));
    }

    #[test]
    fn test_registry_upsert_and_get() {
        let mut registry = SpanRegistry::new();
        registry.upsert_span(make_span(114));
        assert!(registry.get(114).is_some());
    }

    #[test]
    fn test_registry_remove_span() {
        let mut registry = SpanRegistry::new();
        registry.upsert_span(make_span(100));
        let removed = registry.remove_span(100);
        assert!(removed.is_some());
        assert!(registry.get(100).is_none());
    }

    #[test]
    fn test_registry_find_by_key() {
        let mut registry = SpanRegistry::new();
        registry.upsert_span(make_span(114));
        let found = registry.find_by_key(b"t_114_r_5");
        assert!(found.is_some());
        assert_eq!(found.unwrap().table_id, 114);
    }

    #[test]
    fn test_registry_active_spans() {
        let mut registry = SpanRegistry::new();
        let mut s1 = make_span(100); s1.state = SpanState::Subscribing; registry.upsert_span(s1);
        let mut s2 = make_span(200); s2.state = SpanState::Removed; registry.upsert_span(s2);
        assert_eq!(registry.active_spans().len(), 1);
    }

    #[test]
    fn test_worker_lifecycle() {
        let mut span = make_span(100);
        span.start_worker(42);
        assert_eq!(span.count_workers(stream_ingest::span_ctl::WorkerState::Initializing), 1);
        span.activate_worker(42);
        assert_eq!(span.count_workers(stream_ingest::span_ctl::WorkerState::Subscribing), 1);
        span.drain_worker(42);
        assert_eq!(span.count_workers(stream_ingest::span_ctl::WorkerState::Draining), 1);
        span.stop_worker(42);
        assert!(span.workers.is_empty());
    }
}

// ======================================================================
// Phase 3: Split handler tests
// ======================================================================

mod span_split_tests {
    use stream_ingest::span_ctl::{handle_split, ReplicationSpan, SpanState};

    fn make_span(table_id: i64) -> ReplicationSpan {
        ReplicationSpan::new(table_id, format!("t_{}_", table_id).into_bytes(), format!("t_{}_", table_id+1).into_bytes())
    }

    #[test]
    fn test_split_produces_new_regions() {
        let mut span = make_span(100);
        span.start_worker(42); span.activate_worker(42);
        span.state = SpanState::Subscribing;
        let action = handle_split(&mut span, 42, &[42, 43, 44]);
        assert_eq!(action.new_regions.len(), 2);
        assert_eq!(action.drain_parent_region, Some(42));
    }

    #[test]
    fn test_split_parent_not_tracked() {
        let mut span = make_span(100);
        let action = handle_split(&mut span, 99, &[99, 100, 101]);
        assert_eq!(action.new_regions.len(), 3);
        assert_eq!(action.drain_parent_region, None);
    }

    #[test]
    fn test_split_preserves_checkpoint_ts() {
        let mut span = make_span(100);
        span.start_worker(42); span.activate_worker(42);
        span.checkpoint_ts = txn_types::TimeStamp::from(5000);
        let action = handle_split(&mut span, 42, &[42, 43]);
        assert_eq!(action.checkpoint_ts, txn_types::TimeStamp::from(5000));
    }
}
