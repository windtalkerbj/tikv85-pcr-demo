# MVCC Forensic Analysis: DefaultNotFound Root Cause

Date: 2026-05-26 | Owner: Researcher | Status: COMPLETE

## Scope

| Step | Question | Result |
|------|----------|--------|
| 1 | Source vs target byte-identical? | ✅ Yes (verified earlier) |
| 2 | Protobuf corrupts WRITE CF? | ✅ No (bytes identical proves pipeline preserves data) |
| 3 | Is it source GC or pipeline bug? | Pipeline bug (confirmed) |
| 4 | Theory-A or Theory-B? | **Neither** — both theories wrong |

## Actual Root Cause

Not source corruption (Theory-A), not protobuf corruption (Theory-B).
**Cross-CF timestamp mismatch caused by incomplete live CDC replication.**

```
Timeline:
  t0: Full scan → WRITE CF[ts=A] + DEFAULT CF[ts=A]  ← matches ✅
  t1: Live CDC writes mDB:110 → WRITE CF[ts=B]        ← NEW version
      Fallback: is_write_ref=false → DEFAULT CF skipped   ← BUG
  
Target state after live CDC:
  WRITE CF:  NEW version at ts=B
  DEFAULT CF: OLD version at ts=A (from full scan)

TiKV Prewrite:
  1. Read WRITE CF → latest version at ts=B
  2. WriteRef.start_ts = B
  3. Read DEFAULT CF at ts=B → NOT FOUND (only ts=A exists)
  4. → DefaultNotFound { key: mDB:110 }
```

## TiKV Trigger Code

point_getter.rs:374:
```rust
let value = snapshot.get_cf(CF_DEFAULT, &user_key.append_ts(write_start_ts))?;
// write_start_ts = B, but DEFAULT CF only has entries at timestamp A ≠ B
// → get_cf returns None → DefaultNotFound
```

## Fix

delegate.rs fallback: for non-WriteRef values (JSON meta keys), generate DEFAULT CF
using commit_ts from raw_key. This ensures the new WRITE CF version has matching DEFAULT CF.

## Theories Eliminated

- ❌ Theory-A (source GC): DEFAULT CF data exists on source, bytes match
- ❌ Theory-B (protobuf corruption): bytes preserved through pipeline
- ❌ GC timing: timestamps are from different writes, not GC cleanup
- ❌ Cross-TSO-domain issue: same PD clock, single machine setup
