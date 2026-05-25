# DefaultNotFound on mDB:110 — Root Cause Resolved

Date: 2026-05-25 | Owner: Researcher | Status: COMPLETE

## Reproduction

TiDB crash loop, 100% reproducible:
```
[ERROR] [tidb.go:101] ["init domain failed"]
[error="DefaultNotFound { key: mDB:110/hTable:11/2 }"]
```

## Root Cause

TiDB bootstrap does NOT just read meta keys — it WRITES them (DDL upgrade scripts). TiKV's Prewrite path checks existing MVCC state: reads WRITE CF → finds WriteRef → follows `start_ts` to DEFAULT CF → NOT FOUND → DefaultNotFound → TiDB crashes.

This is a **WRITE-path failure**, not a READ-path failure. TiDB's bootstrap DDL triggers a Prewrite, which validates MVCC consistency. The replicated data has broken WriteRef→DEFAULT CF cross-references.

## Why Source TiDB Works

Source TiDB wrote the meta keys itself. If a WriteRef→DEFAULT CF reference was broken, source TiDB would also hit DefaultNotFound on its own Prewrite. The fact that source TiDB starts successfully means the cross-CF references are intact on source — they get broken during PCR replication.

## The Breakage Point

PCR full scan copies raw KV pairs. For meta keys, WRITE CF uses WriteRef format (Put + start_ts + short_value). The `short_value` is embedded in WRITE CF and doesn't require DEFAULT CF. But TiKV's Prewrite validation reads the WriteRef.start_ts and looks up DEFAULT CF at that timestamp. If the DEFAULT CF key with that exact timestamp suffix doesn't exist → DefaultNotFound.

Source RocksDB has both WRITE CF and matching DEFAULT CF. Target has WRITE CF but may not have the matching DEFAULT CF at the exact `start_ts` required by the WriteRef.

## Fix Direction

PCR must ensure cross-CF consistency during replication:
1. Full scan: for each WRITE CF WriteRef, verify DEFAULT CF with matching start_ts exists
2. Live CDC: `from_write_cf` already generates DEFAULT CF from short_value — verify this covers meta keys
3. If DEFAULT CF missing: synthesize from short_value
