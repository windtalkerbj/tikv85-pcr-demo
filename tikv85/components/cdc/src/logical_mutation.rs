// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! MVCC semantic mutation — decodes WriteRef from the WRITE CF Raft
//! commands into logical Put/Delete/Rollback operations. This is the
//! source-side half of MVCC semantic replication: instead of forwarding
//! raw WRITE CF bytes and requiring the consumer to implement an MVCC
//! reader, we emit materialised DEFAULT CF tombstones that the consumer
//! can ingest directly.

use txn_types::{Key, TimeStamp, WriteRef, WriteType};

/// A logical MVCC mutation derived from parsing a WRITE CF WriteRef.
#[derive(Debug, Clone)]
pub enum LogicalMutation {
    /// Put a value for this key at the given commit_ts.
    Put {
        /// DEFAULT CF encoded key: z + memcomparable(user_key) + ts_suffix(start_ts)
        default_key: Vec<u8>,
        /// Original WRITE CF key for the raw WriteRef, needed for MVCC completeness.
        write_key: Vec<u8>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        /// If Some, the value is embedded in the WriteRef (short_value).
        /// If None, the value lives in DEFAULT CF (replicated by full scan).
        short_value: Option<Vec<u8>>,
    },
    /// Delete this key at the given commit_ts.
    Delete {
        /// DEFAULT CF tombstone key: z + memcomparable(user_key) + ts_suffix(start_ts)
        default_key: Vec<u8>,
        /// Original WRITE CF key for the raw WriteRef.
        write_key: Vec<u8>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    },
    /// Rollback a previously-prewritten mutation.
    Rollback {
        /// DEFAULT CF tombstone key to undo any Lock short_value Put.
        default_key: Vec<u8>,
        /// Original WRITE CF key for the raw WriteRef.
        write_key: Vec<u8>,
        start_ts: TimeStamp,
    },
}

impl LogicalMutation {
    /// Parse a WRITE CF entry into a logical mutation.
    ///
    /// `cf_key` is the raw WRITE CF key from the Raft command / CDC observer.
    /// In API v1 this is WITHOUT the 'z' DATA_PREFIX — cf_key = memcomparable(key) + TS.
    /// The 'z' prefix must be added when constructing target RocksDB keys.
    ///
    /// `write_bytes` is the raw WriteRef value from the Raft command.
    pub fn from_write_cf(cf_key: &[u8], write_bytes: &[u8]) -> Option<Self> {
        let write = WriteRef::parse(write_bytes).ok()?;
        let commit_ts = Key::decode_ts_from(cf_key).ok()?;
        // user_key is memcomparable-encoded but WITHOUT 'z' DATA_PREFIX.
        // Do NOT call Key::from_raw which would double-encode.
        let user_key = Key::truncate_ts_for(cf_key).ok()?;

        // Build encoded DEFAULT CF key: 'z' + memcomparable(key) + encoded(start_ts).
        // Matches RocksDB key format (same as span_bridge.rs full scan path).
        let build_default_key = |start_ts: TimeStamp| {
            let mut dk = Vec::with_capacity(1 + user_key.len() + 8);
            dk.push(b'z');
            dk.extend_from_slice(user_key);
            dk.extend_from_slice(&(!start_ts.into_inner()).to_be_bytes());
            dk
        };

        match write.write_type {
            WriteType::Put => {
                let default_key = build_default_key(write.start_ts);
                Some(LogicalMutation::Put {
                    default_key,
                    write_key: cf_key.to_vec(),
                    start_ts: write.start_ts,
                    commit_ts,
                    short_value: write.short_value.map(|v| v.to_vec()),
                })
            }
            WriteType::Delete => {
                let default_key = build_default_key(write.start_ts);
                Some(LogicalMutation::Delete {
                    default_key,
                    write_key: cf_key.to_vec(),
                    start_ts: write.start_ts,
                    commit_ts,
                })
            }
            WriteType::Rollback => {
                let default_key = build_default_key(write.start_ts);
                Some(LogicalMutation::Rollback {
                    default_key,
                    write_key: cf_key.to_vec(),
                    start_ts: write.start_ts,
                })
            }
            // Lock / PessimisticLock: processed separately via Lock::parse in delegate.
            _ => None,
        }
    }
}
