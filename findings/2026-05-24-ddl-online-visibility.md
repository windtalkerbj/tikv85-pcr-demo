# DDL Online Visibility — Why TiDB Restart Is Required

Date: 2026-05-24 | Owner: Researcher | Status: COMPLETE

## Observation

ALTER TABLE ADD COLUMN and CREATE INDEX metadata reaches target RocksDB, but `information_schema` does not reflect the changes until target TiDB is restarted.

## Root Cause

**TiDB has two schema reload paths, and PCR violates the assumptions of the online path.**

### Online reload (ApplyDiff)

TiDB polls PD key `/tidb/ddl/global_schema_version` every schema lease interval (1s). On change, it reads `mysql.tidb` to find the DDL job chain from old_version to new_version. It applies only the delta — specific DDL jobs between the two versions.

PCR's schema_sync bumps PD key with a local counter (1, 2, 3, ...). But `mysql.tidb` on target contains source TiDB's schema versions (from source PD TSO: 50, 51, 52, ...). The two version spaces are disconnected.

When target TiDB sees PD key=100 and reads mysql.tidb latest=52, it tries to find DDL jobs for versions 52→100. These jobs don't exist — they were never created on target TiDB. ApplyDiff finds zero relevant jobs and returns silently. No schema change is applied.

### Full reload (FullLoad / restart)

On restart, TiDB does not use the incremental diff mechanism. It reads PD key once, then scans ALL metadata from KV (mysql.columns, mysql.indexes, etc.) and rebuilds information_schema from scratch. All replicated DDL metadata becomes visible immediately.

## Why CREATE TABLE Works Without Restart

CREATE TABLE creates a new table in a new region. SpanBridge rediscovery detects the new region → triggers full scan → copies ALL KV data including system table metadata. The full scan path does not depend on the schema version chain.

## Why DROP TABLE Works Without Restart

DROP TABLE sends a DeleteRange CDC event. Target ingests the range tombstone. TiDB table lookup fails → marks table as dropped. No schema version dependency.

## Architecture Violation

PCR's model assumes "replicate committed MVCC state → target TiDB can read it." This holds for user data (MVCC key-value reads). It does NOT hold for DDL metadata visibility — TiDB's online schema loader requires a continuous version chain in `mysql.tidb`, which PCR cannot provide from a different TSO domain.

## Fix Directions

### A. Align schema versions (preferred)
Read source TiDB's `@@tidb_schema_version` and write to target PD key instead of using local counter. This ensures `mysql.tidb` versions match PD key version → ApplyDiff can resolve the delta.

### B. Trigger FullLoad online
Set PD key to 0, wait for TiDB to detect → triggers full reload (same as restart) → set back to target version. Forces online full reload without restart.

### C. Accept restart requirement
Document as Demo limitation. Cutover procedure includes target TiDB restart step.

## Impact

| Severity | Demo-blocking for DDL operations |
|----------|------|
| Scope | ALTER TABLE ADD COLUMN, CREATE INDEX, any DDL that modifies existing table metadata |
| Workaround | Target TiDB restart after each DDL batch |
| Production impact | Would require online reload fix for production |
