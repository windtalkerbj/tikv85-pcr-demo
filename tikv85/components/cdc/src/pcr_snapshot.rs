// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Full snapshot scan for PCR initial replication.
//! Uses RocksDB Snapshot (via engine_traits::Iterable) for consistent reads.
//!
//! Bounded scans encode PD region boundaries as complete MVCC keys
//! (encode_bytes(user_key) + encode_u64_desc(MAX_TS)) before passing
//! them to RocksDB as iterator bounds. The write CF comparator accepts
//! these as valid keys, and bounds work correctly with per-CF ordering.

use engine_rocks::RocksEngine;
use engine_rocks::RocksSnapshot;
use engine_traits::{IterOptions, Iterable, Iterator as EngineIterator, Peekable};
use slog_global::info;
use stream_ingest::pcrpb::{OpType, PcrKv, PcrKvBatch, PcrEvent};
use txn_types::{Key, TimeStamp};

/// Streaming full scan: iterate one CF with the same RocksSnapshot/Iterator
/// logic as scan_default_cf, but call `on_kv` for each KV instead of
/// collecting into a Vec. This keeps memory at O(1) per KV.
pub fn stream_cf_full(
    engine: &RocksEngine,
    cf: &str,
    _op: OpType,
    mut on_kv: impl FnMut(Vec<u8>, Vec<u8>),
) -> u64 {
    let mut count: u64 = 0;
    let snap = RocksSnapshot::new(engine.get_sync_db());
    let iter_opt = IterOptions::new(None, None, false);
    let mut iter = match snap.iterator_opt(cf, iter_opt) {
        Ok(i) => i,
        Err(_) => return count,
    };
    let _ = iter.seek_to_first();
    while iter.valid().unwrap_or(false) {
        on_kv(iter.key().to_vec(), iter.value().to_vec());
        count += 1;
        let _ = iter.next();
    }
    count
}

/// Encode a PD region boundary (already in memcomparable format) into a
/// complete MVCC key for RocksDB iterator bounds.
/// Uses Key::from_encoded (not from_raw!) to avoid double-encoding,
/// then appends MAX_TS suffix. Both lower and upper bounds use this.
fn encode_bound(pd_key: &[u8]) -> Vec<u8> {
    Key::from_encoded_slice(pd_key)
        .append_ts(TimeStamp::max())
        .into_encoded()
}

/// Scan the default CF in [from, to) range, up to `limit` items.
pub fn scan_default_cf(
    engine: &RocksEngine,
    from: &[u8],
    to: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut results = Vec::new();
    let snap = RocksSnapshot::new(engine.get_sync_db());

    let mut iter_opt = IterOptions::new(None, None, false);
    let seek_target: Vec<u8>;
    if !from.is_empty() {
        seek_target = encode_bound(from);
        iter_opt.set_lower_bound(&seek_target, 0);
    } else {
        seek_target = vec![];
    }
    let mut iter = match snap.iterator_opt("default", iter_opt) {
        Ok(i) => i,
        Err(_) => return results,
    };

    if seek_target.is_empty() {
        let _ = iter.seek_to_first();
    } else {
        let _ = iter.seek(&seek_target);
    }

    while iter.valid().unwrap_or(false) && results.len() < limit {
        let k = iter.key().to_vec();
        let v = iter.value().to_vec();
        results.push((k, v));
        let _ = iter.next();
    }
    results
}

/// Scan write CF raw entries (no decode) for full sync replication.
/// Returns raw (key, value) pairs that can be ingested directly into the target write CF.
pub fn scan_write_cf_raw(
    engine: &RocksEngine,
    from: &[u8],
    to: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut results = Vec::new();
    let snap = RocksSnapshot::new(engine.get_sync_db());

    let mut iter_opt = IterOptions::new(None, None, false);
    let seek_target: Vec<u8>;
    if !from.is_empty() {
        seek_target = encode_bound(from);
        iter_opt.set_lower_bound(&seek_target, 0);
    } else {
        seek_target = vec![];
    }
    let mut iter = match snap.iterator_opt("write", iter_opt) {
        Ok(i) => i,
        Err(_) => return results,
    };

    if seek_target.is_empty() {
        let _ = iter.seek_to_first();
    } else {
        let _ = iter.seek(&seek_target);
    }

    while iter.valid().unwrap_or(false) && results.len() < limit {
        let k = iter.key().to_vec();
        let v = iter.value().to_vec();
        results.push((k, v));
        let _ = iter.next();
    }
    results
}

/// Scan write CF raw entries committed after `since_ts`.
pub fn scan_write_cf_raw_since(
    engine: &RocksEngine,
    since_ts: u64,
    limit: usize,
    from: &[u8],
    to: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut results = Vec::new();
    let snap = RocksSnapshot::new(engine.get_sync_db());
    let mut iter_opt = IterOptions::new(None, None, false);
    let seek_target: Vec<u8>;
    if !from.is_empty() {
        seek_target = encode_bound(from);
        iter_opt.set_lower_bound(&seek_target, 0);
    } else { seek_target = vec![]; }
    let mut iter = match snap.iterator_opt("write", iter_opt) {
        Ok(i) => i, Err(_) => return results,
    };
    if seek_target.is_empty() { let _ = iter.seek_to_first(); }
    else { let _ = iter.seek(&seek_target); }
    while iter.valid().unwrap_or(false) && results.len() < limit {
        let ek = iter.key();
        if let Ok(commit_ts) = txn_types::Key::decode_ts_from(ek) {
            if commit_ts.into_inner() > since_ts {
                if let Ok(user_key) = txn_types::Key::truncate_ts_for(ek) {
                    if !to.is_empty() && user_key >= to { break; }
                    results.push((ek.to_vec(), iter.value().to_vec()));
                }
            }
        }
        let _ = iter.next();
    }
    results
}

/// Scan write CF for delta replication: returns raw WRITE CF entries (cf="write")
/// AND raw DEFAULT CF entries (cf="default") for non-short_value puts.
/// Both retain correct MVCC key encoding so TiDB can read them.
pub fn scan_delta_entries(
    engine: &RocksEngine,
    since_ts: u64,
    from: &[u8],
    to: &[u8],
) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>, OpType)>) {
    let mut write_entries = Vec::new();
    let mut default_entries: Vec<(Vec<u8>, Vec<u8>, OpType)> = Vec::new();
    let snap = RocksSnapshot::new(engine.get_sync_db());

    let mut iter_opt = IterOptions::new(None, None, false);
    let seek_target: Vec<u8>;
    if !from.is_empty() {
        seek_target = encode_bound(from);
        iter_opt.set_lower_bound(&seek_target, 0);
    } else { seek_target = vec![]; }
    let mut iter = match snap.iterator_opt("write", iter_opt) {
        Ok(i) => i, Err(_) => return (write_entries, default_entries),
    };
    if seek_target.is_empty() { let _ = iter.seek_to_first(); }
    else { let _ = iter.seek(&seek_target); }

    while iter.valid().unwrap_or(false) {
        let encoded_key = iter.key();
        if let Ok(commit_ts) = txn_types::Key::decode_ts_from(encoded_key) {
            if commit_ts.into_inner() > since_ts {
                if let Ok(user_key) = txn_types::Key::truncate_ts_for(encoded_key) {
                    if !to.is_empty() && user_key >= to { break; }
                    let write_bytes = iter.value();
                    if let Ok(write) = txn_types::WriteRef::parse(write_bytes) {
                        match write.write_type {
                            txn_types::WriteType::Put => {
                                write_entries.push((encoded_key.to_vec(), write_bytes.to_vec()));
                                if write.short_value.is_none() {
                                    let default_key = Key::from_encoded_slice(user_key)
                                        .append_ts(write.start_ts)
                                        .into_encoded();
                                    if let Ok(Some(v)) = snap.get_value_cf_opt(
                                        &engine_traits::ReadOptions::new(), "default", &default_key) {
                                        default_entries.push((default_key, v.to_vec(), OpType::Put));
                                    }
                                }
                            }
                            txn_types::WriteType::Delete => {
                                write_entries.push((encoded_key.to_vec(), write_bytes.to_vec()));
                                let default_key = Key::from_encoded_slice(user_key)
                                    .append_ts(write.start_ts)
                                    .into_encoded();
                                default_entries.push((default_key, vec![], OpType::Delete));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        let _ = iter.next();
    }
    (write_entries, default_entries)
}
