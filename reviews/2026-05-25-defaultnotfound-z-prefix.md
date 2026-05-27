# Review: DefaultNotFound Root Cause — z-prefix fix

**Date:** 2026-05-25 | **Decision:** RETURN TO BUILDER → CLOSED

## Problem
TiDB restart after PCR live CDC activity fails with DefaultNotFound. Cannot start target TiDB reliably.

## Investigation Path
1. Initially suspected: WriteRef parse failure, CF data loss, RocksDB compaction GC — all ruled out
2. Traced TiDB source code: found actual error is WRITE-path Prewrite validation failure, not READ-path
3. Compared full scan vs live CDC paths: full scan works, live CDC DEFAULT CF fails
4. Identified: fallback path WRITE CF key has `z` DATA_PREFIX, DEFAULT CF key missing it

## Root Cause (delegate.rs:1441)
TiDB API v1 requires `z` prefix on ALL keys. Fallback path writes:
```
WRITE CF:  z{raw_key}  ✅ correct
DEFAULT CF: {raw_key}  ❌ missing z prefix
```
TiDB reads DEFAULT CF with `z` prefix → key not found → DefaultNotFound.

Full scan works because it copies RocksDB keys directly (already have `z` prefix).
WRITE CF works because fallback adds `z` for WRITE path.

## Fix
delegate.rs: add `z` prefix to DEFAULT CF key in fallback path. 3 lines.

## Impact
- Fixes DefaultNotFound after live CDC activity
- Meta keys correctly written with `z` prefix in BOTH CFs
- TiDB restart after PCR live CDC → no more crash loop
