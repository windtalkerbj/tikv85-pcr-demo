# Review: DDL Online Visibility

**Date:** 2026-05-24 | **Decision:** RETURN TO BUILDER → CLOSED

## Problem
ALTER TABLE ADD COLUMN / CREATE INDEX not visible on target without TiDB restart.

## Investigation Chain (7 false leads eliminated)
- ❌ DefaultNotFound on mDB:110 — actually Write-path Prewrite failure
- ❌ 0xBC WriteRef parse failure — meta key parses correctly
- ❌ CF data loss — byte-identical source vs target
- ❌ mSchemaDiff missing — 0 keys on BOTH sides
- ❌ RocksDB compaction GC — timestamps from NOW
- ❌ old_value_cb silent failure — 0 failures logged
- ❌ metrics counter not registering — instrumentation issue

## Confirmed Root Cause
TiDB ApplyDiff requires continuous version chain in mysql.tidb. PCR bumps PD key with local counter from separate TSO domain → version spaces disconnected. TiDB detects version change but finds no matching DDL job diffs → silently skips reload.

FullLoad (restart) bypasses version chain → reads all KV directly → works.

## Fix
1. TiDB domain.go: zero-diffs → force FullLoad (2 lines)
2. PCR schema_sync.rs: replace local counter with source TiDB @@tidb_schema_version (read source PD etcd v3 → write target PD)
3. TiDB main.go: DefaultNotFound → createReadOnlyDomain (prevents crash loop)

## Demo Limitation
ALTER TABLE / CREATE INDEX still requires TiDB restart on fresh PCR. Online FullLoad triggers but KV data from live CDC incomplete for system tables. TiDB architecture boundary — cannot fully resolve without TiDB source changes.
