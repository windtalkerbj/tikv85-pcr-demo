// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use txn_types::TimeStamp;

/// Lifecycle state of a region worker within a ReplicationSpan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Delta scan running, not yet receiving live events.
    Initializing,
    /// Live CDC streaming active.
    Subscribing,
    /// Being torn down (split source, merge source), waiting for drain.
    Draining,
    /// Removed, no longer active.
    Stopped,
}

/// A region worker tracked by the Control Plane.
#[derive(Debug, Clone)]
pub struct RegionWorker {
    pub region_id: u64,
    pub state: WorkerState,
}

impl RegionWorker {
    pub fn new(region_id: u64, state: WorkerState) -> Self {
        Self { region_id, state }
    }
}

/// A logical replication span covering one TiDB table's key range.
#[derive(Debug, Clone)]
pub struct ReplicationSpan {
    pub table_id: i64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub checkpoint_ts: TimeStamp,
    /// Currently subscribed region workers, keyed by region_id.
    pub workers: HashMap<u64, RegionWorker>,
    pub state: SpanState,
}

/// Lifecycle state of a ReplicationSpan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanState {
    Initialized,
    Subscribing,
    Draining,
    Removed,
}

impl ReplicationSpan {
    pub fn new(table_id: i64, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self {
            table_id,
            start_key,
            end_key,
            checkpoint_ts: TimeStamp::zero(),
            workers: HashMap::new(),
            state: SpanState::Initialized,
        }
    }

    /// Check if a key falls within this span.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.start_key.as_slice() && key < self.end_key.as_slice()
    }

    /// Get all region IDs currently in Subscribing state.
    pub fn active_region_ids(&self) -> Vec<u64> {
        self.workers
            .iter()
            .filter(|(_, w)| w.state == WorkerState::Subscribing)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all region IDs (any state except Stopped).
    pub fn all_region_ids(&self) -> Vec<u64> {
        self.workers
            .iter()
            .filter(|(_, w)| w.state != WorkerState::Stopped)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Start a new region worker in Initializing state.
    pub fn start_worker(&mut self, region_id: u64) {
        self.workers.insert(
            region_id,
            RegionWorker::new(region_id, WorkerState::Initializing),
        );
    }

    /// Transition a worker from Initializing to Subscribing.
    pub fn activate_worker(&mut self, region_id: u64) {
        if let Some(w) = self.workers.get_mut(&region_id) {
            w.state = WorkerState::Subscribing;
        }
    }

    /// Begin draining a worker (split parent, merge source).
    pub fn drain_worker(&mut self, region_id: u64) {
        if let Some(w) = self.workers.get_mut(&region_id) {
            w.state = WorkerState::Draining;
        }
    }

    /// Remove a worker completely.
    pub fn stop_worker(&mut self, region_id: u64) {
        if let Some(w) = self.workers.get_mut(&region_id) {
            w.state = WorkerState::Stopped;
        }
        self.workers.remove(&region_id);
    }

    /// Update checkpoint_ts to the minimum across subscribing workers.
    pub fn update_checkpoint(&mut self, region_frontiers: &HashMap<u64, TimeStamp>) {
        let min_ts = self
            .active_region_ids()
            .iter()
            .filter_map(|rid| region_frontiers.get(rid))
            .min()
            .copied()
            .unwrap_or(self.checkpoint_ts);
        self.checkpoint_ts = min_ts;
    }

    /// Count workers by state.
    pub fn count_workers(&self, state: WorkerState) -> usize {
        self.workers.values().filter(|w| w.state == state).count()
    }
}

/// Reverse index: region_id → set of table_ids whose spans cover this region.
type RegionIndex = HashMap<u64, Vec<i64>>;

/// Central registry mapping logical spans to physical regions.
#[derive(Debug, Default)]
pub struct SpanRegistry {
    pub spans: HashMap<i64, ReplicationSpan>,
    /// Reverse index: region → spans that cover it.
    region_index: RegionIndex,
    /// Last seen DDL job_id from mysql.tidb_ddl_history. When this
    /// changes, a DDL event occurred — trigger table cache diff.
    pub last_seen_ddl_job_id: i64,
    /// Local table cache: table_id → table_name. Used to detect
    /// CREATE/DROP/TRUNCATE by diffing against current source state.
    pub table_cache: HashMap<i64, String>,
}

impl SpanRegistry {
    pub fn new() -> Self {
        Self {
            spans: HashMap::new(),
            region_index: HashMap::new(),
            last_seen_ddl_job_id: 0,
            table_cache: HashMap::new(),
        }
    }

    // ---- Span lifecycle ----

    pub fn upsert_span(&mut self, span: ReplicationSpan) {
        // Update reverse index for existing workers
        for rid in span.all_region_ids() {
            self.region_index.entry(rid).or_default().push(span.table_id);
        }
        self.spans.insert(span.table_id, span);
    }

    pub fn remove_span(&mut self, table_id: i64) -> Option<ReplicationSpan> {
        let span = self.spans.remove(&table_id)?;
        // Clean up reverse index
        for rid in span.all_region_ids() {
            if let Some(tids) = self.region_index.get_mut(&rid) {
                tids.retain(|t| *t != table_id);
                if tids.is_empty() {
                    self.region_index.remove(&rid);
                }
            }
        }
        Some(span)
    }

    // ---- Query ----

    pub fn find_by_key(&self, key: &[u8]) -> Option<&ReplicationSpan> {
        self.spans.values().find(|span| span.contains_key(key))
    }

    pub fn get(&self, table_id: i64) -> Option<&ReplicationSpan> {
        self.spans.get(&table_id)
    }

    pub fn get_mut(&mut self, table_id: i64) -> Option<&mut ReplicationSpan> {
        self.spans.get_mut(&table_id)
    }

    pub fn active_spans(&self) -> Vec<&ReplicationSpan> {
        self.spans
            .values()
            .filter(|s| matches!(s.state, SpanState::Subscribing | SpanState::Draining))
            .collect()
    }

    pub fn table_ids(&self) -> Vec<i64> {
        self.spans.keys().copied().collect()
    }

    pub fn count_by_state(&self, state: SpanState) -> usize {
        self.spans.values().filter(|s| s.state == state).count()
    }

    // ---- RegionIndex operations ----

    /// All spans covering a given region.
    pub fn spans_for_region(&self, region_id: u64) -> Vec<&ReplicationSpan> {
        let Some(table_ids) = self.region_index.get(&region_id) else {
            return Vec::new();
        };
        table_ids
            .iter()
            .filter_map(|tid| self.spans.get(tid))
            .collect()
    }

    /// Check if a region is already tracked by any span.
    pub fn has_region(&self, region_id: u64) -> bool {
        self.region_index.contains_key(&region_id)
    }

    /// Set of all tracked region IDs (any span, any state).
    pub fn all_region_ids(&self) -> Vec<u64> {
        self.region_index.keys().copied().collect()
    }

    /// Add a region to a span's worker list and update the reverse index.
    pub fn add_region_to_span(&mut self, table_id: i64, region_id: u64) {
        if let Some(span) = self.spans.get_mut(&table_id) {
            span.start_worker(region_id);
        }
        self.region_index
            .entry(region_id)
            .or_default()
            .push(table_id);
    }

    /// Handle a region split: for each span covering the parent region,
    /// drain the parent and add the child regions.
    /// Returns the new child region IDs that need subscription.
    pub fn on_region_split(
        &mut self,
        parent_region_id: u64,
        child_region_ids: &[u64],
    ) -> Vec<u64> {
        // Collect covering table IDs first to avoid double-borrow.
        let covering_table_ids: Vec<i64> = self
            .region_index
            .get(&parent_region_id)
            .cloned()
            .unwrap_or_default();

        let mut new_to_subscribe = Vec::new();

        for table_id in covering_table_ids {
            // Drain parent in the span's worker list.
            if let Some(s) = self.spans.get_mut(&table_id) {
                s.drain_worker(parent_region_id);
            }
            // Remove this table from the parent's index entry.
            if let Some(tids) = self.region_index.get_mut(&parent_region_id) {
                tids.retain(|t| *t != table_id);
            }

            // Add children to span and index.
            // If a child has the same ID as the parent (split where old region
            // keeps its ID but gets a smaller key range), the old worker will be
            // in Draining state — force re-subscribe.
            for &child in child_region_ids {
                if let Some(s) = self.spans.get_mut(&table_id) {
                    let needs_subscribe = !s.workers.contains_key(&child)
                        || s.workers.get(&child).map_or(false, |w| w.state == WorkerState::Draining);
                    if needs_subscribe {
                        if s.workers.contains_key(&child) {
                            s.stop_worker(child); // remove drained worker
                        }
                        s.start_worker(child);
                        new_to_subscribe.push(child);
                    }
                }
                self.region_index
                    .entry(child)
                    .or_default()
                    .push(table_id);
            }
        }

        // Clean up parent if no more spans cover it.
        if self.region_index
            .get(&parent_region_id)
            .map_or(true, |v| v.is_empty())
        {
            self.region_index.remove(&parent_region_id);
        }

        new_to_subscribe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_span(tid: i64) -> ReplicationSpan {
        let start = format!("t_{}_", tid).into_bytes();
        let end = format!("t_{}_", tid + 1).into_bytes();
        ReplicationSpan::new(tid, start, end)
    }

    #[test]
    fn test_span_contains_key() {
        let span = make_span(100);
        assert!(span.contains_key(b"t_100_r1"));
        assert!(span.contains_key(b"t_100_"));
        assert!(!span.contains_key(b"t_99_r1"));
        assert!(!span.contains_key(b"t_101_r1"));
    }

    #[test]
    fn test_region_index_add_and_query() {
        let mut registry = SpanRegistry::new();
        let mut span = make_span(100);
        span.start_worker(10);
        span.activate_worker(10);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);

        assert!(registry.has_region(10));
        let covering = registry.spans_for_region(10);
        assert_eq!(covering.len(), 1);
        assert_eq!(covering[0].table_id, 100);
    }

    #[test]
    fn test_region_index_multi_spans() {
        let mut registry = SpanRegistry::new();

        let mut s1 = make_span(100);
        s1.start_worker(10); s1.activate_worker(10);
        s1.state = SpanState::Subscribing;
        registry.upsert_span(s1);

        let mut s2 = make_span(200);
        s2.start_worker(10); s2.activate_worker(10);
        s2.state = SpanState::Subscribing;
        registry.upsert_span(s2);

        let covering = registry.spans_for_region(10);
        assert_eq!(covering.len(), 2);
    }

    #[test]
    fn test_on_region_split() {
        let mut registry = SpanRegistry::new();

        let mut span = make_span(100);
        span.start_worker(10);
        span.activate_worker(10);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);

        let new = registry.on_region_split(10, &[11, 12]);
        assert_eq!(new.len(), 2);
        assert!(new.contains(&11));
        assert!(new.contains(&12));

        // Old region should be drained
        let s = registry.get(100).unwrap();
        assert_eq!(s.workers.get(&10).unwrap().state, WorkerState::Draining);

        // New regions in index
        assert!(registry.has_region(11));
        assert!(registry.has_region(12));
    }

    #[test]
    fn test_remove_span_cleans_index() {
        let mut registry = SpanRegistry::new();
        let mut span = make_span(100);
        span.start_worker(10); span.activate_worker(10);
        span.state = SpanState::Subscribing;
        registry.upsert_span(span);

        assert!(registry.has_region(10));
        registry.remove_span(100);
        assert!(!registry.has_region(10));
    }

    #[test]
    fn test_active_region_ids() {
        let mut span = make_span(100);
        span.start_worker(1);
        span.activate_worker(1);
        span.start_worker(2); // Initializing, not Subscribing
        assert_eq!(span.active_region_ids(), vec![1]);
    }

    #[test]
    fn test_worker_lifecycle() {
        let mut span = make_span(100);
        span.start_worker(1);
        assert_eq!(span.workers.get(&1).unwrap().state, WorkerState::Initializing);
        span.activate_worker(1);
        assert_eq!(span.workers.get(&1).unwrap().state, WorkerState::Subscribing);
        span.drain_worker(1);
        assert_eq!(span.workers.get(&1).unwrap().state, WorkerState::Draining);
        span.stop_worker(1);
        assert!(!span.workers.contains_key(&1));
    }
}
