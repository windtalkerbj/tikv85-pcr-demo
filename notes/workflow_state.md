# CURRENT OWNER

Builder

---
# CURRENT PHASE

Build (extend HashSet guard to old_value_cb + WriteRef fallback paths)

---
# ROOT CAUSE (2026-05-27)
cf="" handler writes full JSON to DEFAULT CF.
cf="write" handler short_value/old_value_cb/fallback overwrites with version counter.

# Fix
Existing HashSet tracks cf="" keys. short_value path already protected.
Add same check to:
1. old_value_cb path (`if !seen_keys.contains(&default_key)`)
2. WriteRef fallback path (same check)

Both in sink_raw_put + sink_txn_put (sink_txn_put has identical structure).

# RULED OUT (8)
old_value_cb | lock_tracker | LockRelated | IngestSst | CF routing | cf="" skip | z-prefix | key encoding
