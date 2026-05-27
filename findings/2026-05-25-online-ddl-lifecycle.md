# Online DDL Visibility — Lifecycle Analysis

Date: 2026-05-25 | Owner: Researcher

## Hypothesis Validation

| # | Hypothesis | Verdict | Evidence |
|---|-----------|---------|----------|
| H1 | DDL metadata replay incomplete | ❌ REJECTED | Data IS in target KV — visible after restart |
| H2 | System-table keys not propagated | ❌ REJECTED | mysql.columns KV present in target RocksDB |
| H3 | Schema reload path bypassed | ❌ REJECTED | FullLoad confirmed triggered (domain.go logs) |
| H4 | Replay violates DDL propagation lifecycle | ✅ CONFIRMED | Live CDC fallback writes DEFAULT CF with garbage start_ts |

## Replay Lifecycle

### Phase 1: Initial Full Scan (ti=0)
```
Source RocksDB → stream_cf_full(default) → relay → target ingest
Source RocksDB → stream_cf_full(write)  → relay → target ingest
```
Both CFs byte-identical. All timestamps correct (source PD TSO).
TiDB FullLoad reads → information_schema correct ✅

### Phase 2: Live CDC (ALTER TABLE)
```
Source TiDB writes mysql.columns:
  WRITE CF:  z + data_key + !commit_ts, value=JSON
  DEFAULT CF: z + data_key + !start_ts, value=JSON (start_ts==commit_ts for meta)

PCR captures CmdType::Put (WRITE CF):
  from_write_cf(JSON) → WriteRef::parse fails (value is JSON, not WriteRef)
  → fallback path (delegate.rs:1426-1458)

Fallback writes:
  WRITE CF:  z + data_key + !commit_ts ← correct ✅
  DEFAULT CF: z + data_key + !garbage_start_ts ← BUG ❌
    garbage_start_ts = parse_var_u64_from_JSON_bytes()
    e.g., JSON '{"id":...' → byte[1]='"'=0x22 → val=34 → !34=0xFFFFFFFFFFDD
```

### Phase 3: Online Schema Reload
```
Schema version bump → TiDB FullLoad triggered
FullLoad reads mysql.columns:
  Latest WRITE CF found → value = correct JSON ✅
  But value is JSON, not WriteRef → TiDB needs DEFAULT CF
  DEFAULT CF key = z + data_key + !correct_start_ts
  Actual key in RocksDB = z + data_key + !garbage_start_ts
  → KEY NOT FOUND → TiDB reads OLDER version (from full scan)
  → Old schema returned → new columns invisible ❌
```

### Phase 4: TiDB Restart
```
TiDB bootstrap WRITES to mysql.columns (DDL upgrade scripts)
  TiKV Prewrite validates existing MVCC state:
    reads WRITE CF → finds WriteRef with start_ts=X
    reads DEFAULT CF at X → garbage timestamp → NOT FOUND
    → DefaultNotFound → TiDB crashes ❌
```

## Why Restart Sometimes Works

When TiDB restarts on FRESH PCR data (before live CDC): full scan data has correct timestamps in both CFs. TiDB bootstrap writes succeed. FullLoad sees correct data.

When TiDB restarts AFTER live CDC: meta keys have damaged DEFAULT CF timestamps. TiDB bootstrap Prewrite fails with DefaultNotFound. Crash loop.

When TiDB starts with PCR read-only mode (main.go fix): skips bootstrap DDL → goes straight to FullLoad → reads latest meta keys → finds damaged DEFAULT CF → falls back to old version → old schema.

## Fix

delegate.rs fallback path: use `WriteRef::parse` to distinguish WriteRef values from raw JSON values.
- WriteRef → use write.start_ts for DEFAULT CF suffix
- Raw JSON/non-WriteRef → use commit_ts from WRITE CF key suffix (start_ts==commit_ts for meta keys)

```rust
let dk_ts = WriteRef::parse(&value).ok()
    .map(|w| (!w.start_ts.into_inner()).to_be_bytes().to_vec())
    .unwrap_or_else(|| raw_key[raw_key.len()-8..].to_vec());
```

## Impact

- Fixes DefaultNotFound after live CDC
- Fixes online DDL visibility (FullLoad can read correct DEFAULT CF)
- Fixes TiDB restart reliability
