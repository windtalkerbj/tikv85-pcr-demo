# SpanBridge P0 Fixes: gRPC Reconnect + Reconcile Overwrite

## Fix #1: gRPC Writer Reconnect

**Problem:** Current writer task does `while let Some(event) = merge_rx.recv()` then `sink.send()`. On send error, it logs "gRPC sink error" and breaks. The task exits, merge_rx data backs up, PCR stalls.

**Fix:** Wrap writer in reconnect loop. On disconnect, sleep 2s, create new gRPC sink.

**File:** `components/cdc/src/span_bridge.rs`

### Steps:

- [ ] Step 1: Read current writer task code in span_bridge.rs
- [ ] Step 2: Replace simple writer loop with reconnect loop
- [ ] Step 3: Build + verify compilation

## Fix #2: 30min Reconcile Overwrite Risk

**Problem:** The 30min periodic reconciliation does a full scan (`is_full = start_ts == 0`), re-sending ALL RocksDB data. This overwrites live CDC deltas in the consumer SstBatcher (e.g., DELETE tombstone overwritten by old DEFAULT CF Put).

**Fix:** Remove the 30min reconcile loop entirely. PcrRegistry auto-match already handles live CDC for new regions. Full scan is only needed at startup.

**File:** `components/cdc/src/span_bridge.rs`

### Steps:

- [ ] Step 1: Remove the reconcile loop from run()
- [ ] Step 2: Build + verify compilation

## Regression Test

- [ ] Step 1: Restart cluster with fixed binary
- [ ] Step 2: Run INSERT + DELETE convergence test
- [ ] Step 3: Verify COUNT + SUM consistency
