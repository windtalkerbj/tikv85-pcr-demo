# CURRENT OWNER

Builder

---
# CURRENT PHASE

Build (cf="" fix: add z prefix to DEFAULT CF key)

---
# ROOT CAUSE (2026-05-27)

cf="" fix writes raw JSON to target DEFAULT CF but key missing `z` DATA_PREFIX.
Raft command key lacks `z`. RocksDB API v1 requires `z` on ALL keys.
TiDB reads with `z` → key not found → 61 errors still.

Same root as the earlier WRITE CF fallback bug — DEFAULT CF key missing `z`.

## Fix

sink_raw_put + sink_txn_put cf="" branch: add `z` prefix before writing DEFAULT CF key.
```rust
let mut dk = Vec::with_capacity(1 + raw_key.len());
dk.push(b'z');
dk.extend_from_slice(raw_key);
batcher.add_kv(dk, value, OpType::Put, "default");
```

## Ruled out (all 6)
old_value_cb | lock_tracker | LockRelated | IngestSst | CF routing | cf="" skip
