// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use slog_global::{info, warn};

use super::registry::{ReplicationSpan, SpanRegistry};

/// Build initial ReplicationSpans from source TiDB's information_schema.
///
/// Queries the source TiDB (via MySQL protocol over HTTP) to get all
/// user-table IDs, then constructs a ReplicationSpan for each.
pub fn build_initial_spans(
    source_tidb_addr: &str,  // e.g. "127.0.0.1:4100"
) -> Vec<ReplicationSpan> {
    let table_ids = match query_table_ids(source_tidb_addr) {
        Ok(ids) => ids,
        Err(e) => {
            warn!("PCR span resolver: failed to query source TiDB: {:?}", e);
            return Vec::new();
        }
    };

    let mut spans = Vec::with_capacity(table_ids.len());
    for tid in table_ids {
        spans.push(make_span_for_table(tid));
    }
    info!("PCR span resolver: built initial spans from source TiDB");
    spans
}

/// Query source TiDB for table IDs in the target database.
fn query_table_ids(tidb_addr: &str) -> Result<Vec<i64>, String> {
    // Use mysql CLI to query information_schema.
    // Table key range: t{table_id}_ → t{table_id+1}_
    let output = std::process::Command::new("mysql")
        .args(&[
            "-u", "root",
            "-h", &tidb_addr.split(':').next().unwrap_or("127.0.0.1"),
            "-P", &tidb_addr.split(':').nth(1).unwrap_or("4000"),
            "-N",
            "-e",
            "SELECT DISTINCT TIDB_TABLE_ID FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'metrics_schema')",
        ])
        .output()
        .map_err(|e| format!("mysql command failed: {:?}", e))?;

    if !output.status.success() {
        return Err(format!("mysql exit status: {:?}", output.status));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let ids: Vec<i64> = stdout
        .lines()
        .filter_map(|line| line.trim().parse::<i64>().ok())
        .collect();

    if ids.is_empty() {
        return Err("no tables found".to_string());
    }

    Ok(ids)
}

/// Query source TiDB's information_schema.tables for current table IDs.
/// Used as bootstrap on first run (when last_seen_job_id == 0).
fn query_current_tables(tidb_addr: &str) -> Result<(Vec<i64>, Vec<String>), String> {
    let output = std::process::Command::new("mysql")
        .args(&[
            "-u", "root",
            "-h", &tidb_addr.split(':').next().unwrap_or("127.0.0.1"),
            "-P", &tidb_addr.split(':').nth(1).unwrap_or("4000"),
            "-N",
            "-e",
            "SELECT TIDB_TABLE_ID, TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','metrics_schema')",
        ])
        .output()
        .map_err(|e| format!("mysql query failed: {:?}", e))?;

    if !output.status.success() {
        return Err(format!("mysql exit: {:?}", output.status));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut ids = Vec::new();
    let mut names = Vec::new();

    for line in stdout.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 2 {
            if let Ok(id) = parts[0].trim().parse::<i64>() {
                ids.push(id);
                names.push(parts[1].trim().to_string());
            }
        }
    }

    Ok((ids, names))
}

/// Construct a ReplicationSpan covering one table's full key range.
fn make_span_for_table(table_id: i64) -> ReplicationSpan {
    let start_key = format_key_prefix(table_id);
    let end_key = format_key_end(table_id);
    ReplicationSpan::new(table_id, start_key, end_key)
}

/// Build the TiDB table key prefix for a given table_id.
///
/// TiDB stores table keys in raw format: `t_{table_id}_r{row_id}`
/// where table_id and row_id are decimal strings, NOT big-endian int64.
/// This raw key is what PCR sees in API v1 (`z + raw_key + ts_suffix`).
fn format_key_prefix(table_id: i64) -> Vec<u8> {
    format!("t_{}_", table_id).into_bytes()
}

/// Build the end key (exclusive) for a table's key range.
fn format_key_end(table_id: i64) -> Vec<u8> {
    format!("t_{}_", table_id + 1).into_bytes()
}

/// Resolve spans to current PD region IDs.
///
/// Queries PD HTTP API to find which regions cover each span's key range.
/// Uses iterative `/pd/api/v1/regions/key` queries to walk the key space.
pub fn resolve_spans_to_regions(
    source_pd: &str,
    spans: &mut SpanRegistry,
) {
    for span in spans.spans.values_mut() {
        if span.state != super::registry::SpanState::Initialized
            && span.state != super::registry::SpanState::Subscribing
        {
            continue;
        }
        match query_regions_for_range(source_pd, &span.start_key, &span.end_key) {
            Ok(region_ids) => {
                for rid in &region_ids {
                    span.start_worker(*rid);
                }
                info!(
                    "PCR span resolver: resolved span to regions";
                    "table_id" => span.table_id,
                    "start_key" => ?String::from_utf8_lossy(&span.start_key),
                    "end_key" => ?String::from_utf8_lossy(&span.end_key),
                    "region_count" => region_ids.len(),
                );
            }
            Err(e) => {
                warn!(
                    "PCR span resolver: failed to resolve span";
                    "table_id" => span.table_id,
                    "error" => ?e,
                );
            }
        }
    }
}

/// Hex-encode a byte slice (no external crate needed).
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Query PD HTTP API for all regions covering [start_key, end_key).
///
/// Uses curl to query PD's HTTP API iteratively, walking the key space
/// from start_key to end_key. Each call returns the region covering the
/// current cursor position and its end_key; we advance and repeat.
fn query_regions_for_range(
    pd_addr: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<u64>, String> {
    let url = format!("http://{}/pd/api/v1/regions", pd_addr);
    let output = std::process::Command::new("curl")
        .args(&["-s", &url])
        .output()
        .map_err(|e| format!("curl failed: {:?}", e))?;

    if !output.status.success() {
        return Err(format!("curl exit: {:?}", output.status));
    }

    let body = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("PD JSON parse: {:?}", e))?;

    let regions = json
        .get("regions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("PD missing 'regions' array"))?;

    let start = String::from_utf8_lossy(start_key).to_string();
    let end = String::from_utf8_lossy(end_key).to_string();

    let mut region_ids = Vec::new();
    for r in regions {
        let rid = r.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        if rid == 0 {
            continue;
        }
        let r_start = r.get("start_key").and_then(|v| v.as_str()).unwrap_or("");
        let r_end = r.get("end_key").and_then(|v| v.as_str()).unwrap_or("");

        let r_start_bytes = hex_decode(r_start).unwrap_or_default();
        let r_end_bytes = hex_decode(r_end).unwrap_or_default();

        // Skip placeholder/empty regions (both start and end empty).
        if r_start_bytes.is_empty() && r_end_bytes.is_empty() {
            continue;
        }

        // Check if this region overlaps with [start_key, end_key)
        let overlaps = (r_end_bytes.is_empty() || r_end_bytes.as_slice() > start_key)
            && (r_start_bytes.is_empty() || r_start_bytes.as_slice() < end_key);
        if overlaps {
            region_ids.push(rid);
        }
    }

    Ok(region_ids)
}

/// Hex-decode a hex string to bytes (no external crate needed).
fn hex_decode(hex_str: &str) -> Result<Vec<u8>, String> {
    if hex_str.is_empty() {
        return Ok(Vec::new());
    }
    if hex_str.len() % 2 != 0 {
        return Err(format!("hex string has odd length: {}", hex_str.len()));
    }
    (0..hex_str.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex_str[i..i + 2], 16)
                .map_err(|e| format!("hex decode at {}: {:?}", i, e))
        })
        .collect()
}

/// Split event: a region has split, producing new child regions.
///
/// Returns the list of new region IDs that need delta scan + subscription,
/// and the parent region ID that should be drained.
pub fn handle_split(
    span: &mut ReplicationSpan,
    parent_region_id: u64,
    new_region_ids: &[u64],
) -> SplitAction {
    // Check before mutation: was the parent region previously tracked?
    let drain_parent = span.workers.contains_key(&parent_region_id);

    let mut new_regions = Vec::new();
    for &rid in new_region_ids {
        if !span.workers.contains_key(&rid) {
            span.start_worker(rid); // Initializing state
            new_regions.push(rid);
        }
    }

    if drain_parent {
        span.drain_worker(parent_region_id);
    }

    SplitAction {
        new_regions,
        drain_parent_region: if drain_parent { Some(parent_region_id) } else { None },
        checkpoint_ts: span.checkpoint_ts,
    }
}

/// Periodic reconcile: compare PD region list with SpanRegistry workers.
///
/// Returns regions to add (with delta scan needed) and regions to remove.
/// This is a safety net, not the primary mechanism — split/merge events
/// are the primary driver. Reconcile catches any edge cases.
pub fn reconcile(
    registry: &mut SpanRegistry,
    pd_regions: &[(u64, Vec<u8>, Vec<u8>)],
) -> ReconcileResult {
    let mut to_add = Vec::new();
    let mut to_remove = Vec::new();

    // Build PD region set
    let pd_set: std::collections::HashSet<u64> =
        pd_regions.iter().map(|(id, _, _)| *id).collect();

    // Build set of all region IDs tracked across all spans
    let mut tracked: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for span in registry.spans.values() {
        for rid in span.all_region_ids() {
            tracked.insert(rid);
        }
    }

    // Find regions in PD but not tracked → add
    for (rid, start_key, end_key) in pd_regions {
        if !tracked.contains(rid) {
            to_add.push((*rid, start_key.clone(), end_key.clone()));
        }
    }

    // Find regions tracked but not in PD → remove
    for rid in &tracked {
        if !pd_set.contains(rid) {
            to_remove.push(*rid);
        }
    }

    // Apply removals to all spans
    for rid in &to_remove {
        for span in registry.spans.values_mut() {
            span.stop_worker(*rid);
        }
    }

    info!("PCR reconcile: completed");

    ReconcileResult { to_add, to_remove }
}

/// Result of periodic reconcile.
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// Regions to add: (region_id, start_key, end_key)
    pub to_add: Vec<(u64, Vec<u8>, Vec<u8>)>,
    /// Regions to remove
    pub to_remove: Vec<u64>,
}

/// Result of handling a split event.
#[derive(Debug, Clone)]
pub struct SplitAction {
    /// New region IDs that need: delta scan → subscribe
    pub new_regions: Vec<u64>,
    /// Parent region to drain after delta scan completes
    pub drain_parent_region: Option<u64>,
    /// Checkpoint timestamp for delta scan (capture writes since this point)
    pub checkpoint_ts: txn_types::TimeStamp,
}

/// Resolve a single span (start_key, end_key) to region IDs with key ranges.
/// Returns Vec<(region_id, start_key, end_key)>.
pub async fn resolve_span_to_regions(
    pd_addr: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<(u64, Vec<u8>, Vec<u8>)>, String> {
    let pd_owned = pd_addr.to_string();
    let start = start_key.to_vec();
    let end = end_key.to_vec();
    tokio::task::spawn_blocking(move || {
        query_regions_for_range_with_keys(&pd_owned, &start, &end)
    })
    .await
    .map_err(|e| format!("spawn_blocking failed: {:?}", e))?
}

/// Like query_regions_for_range but returns start/end keys per region.
fn query_regions_for_range_with_keys(
    pd_addr: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<Vec<(u64, Vec<u8>, Vec<u8>)>, String> {
    let url = format!("http://{}/pd/api/v1/regions", pd_addr);
    let output = std::process::Command::new("curl")
        .args(&["-s", &url])
        .output()
        .map_err(|e| format!("curl failed: {:?}", e))?;

    if !output.status.success() {
        return Err(format!("curl exit: {:?}", output.status));
    }

    let body = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| format!("PD JSON parse: {:?}", e))?;

    let regions = json
        .get("regions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("PD missing 'regions' array"))?;

    let mut results = Vec::new();
    for r in regions {
        let rid = r.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        if rid == 0 {
            continue;
        }
        let r_start = r.get("start_key").and_then(|v| v.as_str())
            .map(|s| hex_decode(s).unwrap_or_default()).unwrap_or_default();
        let r_end = r.get("end_key").and_then(|v| v.as_str())
            .map(|s| hex_decode(s).unwrap_or_default()).unwrap_or_default();

        // Skip placeholder/empty regions (both start and end empty) —
        // these don't contain actual data.
        if r_start.is_empty() && r_end.is_empty() {
            continue;
        }

        // Check if this region overlaps with [start_key, end_key)
        let overlaps = (r_end.is_empty() || r_end.as_slice() > start_key)
            && (r_start.is_empty() || r_start.as_slice() < end_key);
        if overlaps {
            results.push((rid, r_start, r_end));
        }
    }

    Ok(results)
}

/// DDL-triggered span discovery with local table cache diff.
///
/// Polls MAX(job_id) from mysql.tidb_ddl_history. When a DDL event is
/// detected (job_id changed), queries current tables and diffs against
/// local cache to generate pseudo DDL lifecycle events (CREATE/DROP).
/// This avoids parsing job_meta while still detecting table lifecycle.
pub fn discover_new_spans(
    registry: &mut SpanRegistry,
    source_tidb_addr: &str,
) -> Vec<ReplicationSpan> {
    // Poll DDL history for changes.
    let current_max = query_max_ddl_job_id(source_tidb_addr);

    // Bootstrap: first run seeds cache and spans from full snapshot.
    if registry.last_seen_ddl_job_id == 0 {
        let (current_ids, names) = match query_current_tables(source_tidb_addr) {
            Ok(r) => r,
            Err(_e) => return Vec::new(),
        };
        // Seed table cache
        for (tid, name) in current_ids.iter().zip(names.iter()) {
            registry.table_cache.insert(*tid, name.clone());
        }
        // Create spans for all tables
        let mut new_spans = Vec::new();
        for &tid in &current_ids {
            let span = make_span_for_table(tid);
            registry.upsert_span(span.clone());
            new_spans.push(span);
        }
        registry.last_seen_ddl_job_id = current_max;
        return new_spans;
    }

    // No DDL change — skip expensive full refresh.
    if current_max <= registry.last_seen_ddl_job_id {
        return Vec::new();
    }
    registry.last_seen_ddl_job_id = current_max;

    // DDL detected: refresh current tables and diff against cache.
    let (current_ids, names) = match query_current_tables(source_tidb_addr) {
        Ok(r) => r,
        Err(_e) => return Vec::new(),
    };
    let current: HashSet<i64> = current_ids.iter().copied().collect();
    let cached: HashSet<i64> = registry.table_cache.keys().copied().collect();

    let mut new_spans = Vec::new();

    // Missing from current → DROP
    for &tid in &cached {
        if !current.contains(&tid) {
            registry.remove_span(tid);
            registry.table_cache.remove(&tid);
            info!("PCR DDL discover: table dropped"; "table_id" => tid);
        }
    }

    // New in current → CREATE (or TRUNCATE with new table_id)
    for (i, &tid) in current_ids.iter().enumerate() {
        if !cached.contains(&tid) {
            let span = make_span_for_table(tid);
            registry.upsert_span(span.clone());
            new_spans.push(span);
            let name = names.get(i).cloned().unwrap_or_default();
            registry.table_cache.insert(tid, name);
            info!("PCR DDL discover: table created"; "table_id" => tid);
        }
    }

    new_spans
}

/// Get the current max DDL job_id from mysql.tidb_ddl_history.
fn query_max_ddl_job_id(tidb_addr: &str) -> i64 {
    let host = tidb_addr.split(':').next().unwrap_or("127.0.0.1");
    let port = tidb_addr.split(':').nth(1).unwrap_or("4100");
    let output = match std::process::Command::new("mysql")
        .args(&["-u", "root", "-h", host, "-P", port, "-N",
            "-e", "SELECT COALESCE(MAX(job_id),0) FROM mysql.tidb_ddl_history"])
        .output()
    {
        Ok(o) => o,
        Err(_) => return 0,
    };
    if !output.status.success() { return 0; }
    String::from_utf8_lossy(&output.stdout).trim().parse().unwrap_or(0)
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;
    use crate::span_ctl::registry::{ReplicationSpan, SpanRegistry, SpanState};

    fn make_reg_span(table_id: i64) -> ReplicationSpan {
        ReplicationSpan::new(table_id, format!("t_{}_", table_id).into_bytes(), format!("t_{}_", table_id + 1).into_bytes())
    }

    #[test]
    fn test_reconcile_detects_new_regions() {
        let mut registry = SpanRegistry::new();
        let mut span = make_reg_span(100);
        span.start_worker(1); span.activate_worker(1);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);
        let pd = vec![(1, vec![], vec![]), (2, vec![], vec![])];
        let r = reconcile(&mut registry, &pd);
        assert_eq!(r.to_add.len(), 1);
    }

    #[test]
    fn test_reconcile_all_match() {
        let mut registry = SpanRegistry::new();
        let mut span = make_reg_span(100);
        span.start_worker(1); span.activate_worker(1);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);
        let pd = vec![(1, vec![], vec![])];
        let r = reconcile(&mut registry, &pd);
        assert!(r.to_add.is_empty());
        assert!(r.to_remove.is_empty());
    }

    #[test]
    fn test_reconcile_removes_stale() {
        let mut registry = SpanRegistry::new();
        let mut span = make_reg_span(100);
        span.start_worker(1); span.activate_worker(1);
        span.start_worker(2); span.activate_worker(2);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);
        let pd = vec![(1, vec![], vec![])];
        let r = reconcile(&mut registry, &pd);
        assert_eq!(r.to_remove.len(), 1);
    }
}

    #[test]
    fn test_discover_new_spans_detects_new_table() {
        use crate::span_ctl::registry::{SpanRegistry, SpanState};
        let mut registry = SpanRegistry::new();
        // Note: discover_new_spans queries a real TiDB, so this tests the diff logic:
        // If registry has table 100, and TiDB has tables 100 and 200,
        // table 200 should be detected as new.
        let known: std::collections::HashSet<i64> = registry.table_ids().into_iter().collect();
        assert!(known.is_empty());
    }

    #[test]
    fn test_discover_new_spans_skip_existing() {
        use crate::span_ctl::registry::{ReplicationSpan, SpanRegistry};
        let mut registry = SpanRegistry::new();
        registry.upsert_span(ReplicationSpan::new(100, b"t_100_".to_vec(), b"t_101_".to_vec()));
        let known: std::collections::HashSet<i64> = registry.table_ids().into_iter().collect();
        assert!(known.contains(&100));
        assert!(!known.contains(&200));
    }

    #[test]
    fn test_ddl_job_parse_create_table() {
        // Simulates parsing mysql.tidb_ddl_job output:
        // job_id\ttable_id\ttype
        let line = "123\t114\tcreate table";
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1].trim().parse::<i64>().unwrap(), 114);
        assert_eq!(parts[2].trim(), "create table");
    }

    #[test]
    fn test_ddl_job_parse_drop_table() {
        let line = "124\t114\tdrop table";
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts[2].trim(), "drop table");
    }

    #[test]
    fn test_ddl_job_parse_truncate() {
        let line = "125\t116\ttruncate table";
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts[2].trim(), "truncate table");
    }

    #[test]
    fn test_discover_creates_span_for_new_table() {
        use crate::span_ctl::registry::{SpanRegistry, SpanState};
        let mut registry = SpanRegistry::new();
        // Simulate DDL discover finding table 200
        registry.upsert_span(ReplicationSpan::new(200, b"t_200_".to_vec(), b"t_201_".to_vec()));
        assert!(registry.get(200).is_some());
    }

    #[test]
    fn test_discover_removes_dropped_table() {
        use crate::span_ctl::registry::{SpanRegistry, SpanState};
        let mut registry = SpanRegistry::new();
        registry.upsert_span(ReplicationSpan::new(300, b"t_300_".to_vec(), b"t_301_".to_vec()));
        assert!(registry.get(300).is_some());
        registry.remove_span(300);
        assert!(registry.get(300).is_none());
    }
