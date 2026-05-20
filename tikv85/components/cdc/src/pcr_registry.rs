// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::channel::mpsc;

/// A span-level PCR subscription. The shared sink is cloned to every
/// matching delegate — split/merge is transparent because the subscription
/// outlives any single region.
pub struct SpanSubscription {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub sink: Arc<mpsc::UnboundedSender<Vec<u8>>>,
    pub event_buffer_size: usize,
    pub event_flush_interval_ms: u64,
}

pub struct PcrRegistry {
    subscriptions: Vec<SpanSubscription>,
}

impl PcrRegistry {
    pub fn new() -> Self {
        Self { subscriptions: Vec::new() }
    }

    pub fn register(&mut self, sub: SpanSubscription) {
        self.subscriptions.push(sub);
    }

    /// Check if a region's key range overlaps any active span subscription.
    /// Returns the shared sink + PCR config if matched.
    pub fn match_region(
        &self,
        _region_id: u64,
        r_start: &[u8],
        r_end: &[u8],
    ) -> Option<(Arc<mpsc::UnboundedSender<Vec<u8>>>, usize, u64)> {
        for sub in &self.subscriptions {
            let left = sub.start_key.is_empty()
                || r_end.is_empty()
                || r_end > sub.start_key.as_slice();
            let right = sub.end_key.is_empty()
                || r_start.is_empty()
                || r_start < sub.end_key.as_slice();
            if left && right {
                return Some((
                    sub.sink.clone(),
                    sub.event_buffer_size,
                    sub.event_flush_interval_ms,
                ));
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn clear(&mut self) {
        self.subscriptions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sink() -> Arc<mpsc::UnboundedSender<Vec<u8>>> {
        let (tx, _rx) = mpsc::unbounded();
        Arc::new(tx)
    }

    #[test]
    fn test_full_span_matches_any_region() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: vec![],
            end_key: vec![],
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        assert!(reg.match_region(1, b"abc", b"def").is_some());
        assert!(reg.match_region(2, &[], &[]).is_some());
        assert!(reg.match_region(3, b"t_100_", b"t_200_").is_some());
    }

    #[test]
    fn test_partial_span_match() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: b"t_100_".to_vec(),
            end_key: b"t_200_".to_vec(),
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        assert!(reg.match_region(1, b"t_120_", b"t_150_").is_some());
        assert!(reg.match_region(2, b"t_050_", b"t_120_").is_some());
        assert!(reg.match_region(3, b"t_200_", b"t_300_").is_none());
    }

    #[test]
    fn test_empty_registry_returns_none() {
        let reg = PcrRegistry::new();
        assert!(reg.match_region(1, b"abc", b"def").is_none());
    }

    #[test]
    fn test_clear() {
        let mut reg = PcrRegistry::new();
        reg.register(SpanSubscription {
            start_key: vec![],
            end_key: vec![],
            sink: make_sink(),
            event_buffer_size: 1024,
            event_flush_interval_ms: 150,
        });
        assert!(reg.match_region(1, b"abc", b"def").is_some());
        reg.clear();
        assert!(reg.match_region(1, b"abc", b"def").is_none());
    }
}
