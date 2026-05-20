// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! # Span Control-Plane
//!
//! Logical topology layer separating "what to replicate" (Spans) from
//! "how to replicate" (per-region workers).
//!
//! ## Design
//!
//! ```text
//! info_schema → ReplicationSpans → PD resolve regions → Execution workers
//! DDL job KV  → new ReplicationSpan  → PD resolve → new worker
//! Split event → invalidate(span) → scan_delta(new_regions) → live stream
//! ```

pub mod registry;
pub mod resolver;
pub use registry::{RegionWorker, ReplicationSpan, SpanRegistry, SpanState, WorkerState};
pub use resolver::{build_initial_spans, discover_new_spans, handle_split, reconcile, resolve_span_to_regions, resolve_spans_to_regions, ReconcileResult, SplitAction};
