# Review: Relay Observability Fix

**Date:** 2026-05-23 | **Decision:** RETURN TO BUILDER → CLOSED

## Evidence
- Relay heartbeat shows events_processed=0 while INSERT converges in 3s
- Original relay exits after t+30s (Arc ref drop), counters die with instance
- Reconnect relay spawned every 30s, each with fresh zero counters

## Root Cause
Per-instance u64 counters scoped to spawned relay task. Relay lifetime = 30s. Counters reset on each SpanBridge rediscovery cycle.

## Fix
Arc<AtomicU64> counters scoped to SpanBridge::run(), shared across all relay instances.

## Validation
- events_processed tracks correctly (7→24→128→177)
- parse_ok matches, parse_fail=0
- Counters survive relay restart
