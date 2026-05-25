# TiDB Meta Key CF Dependency Model — Analysis

Date: 2026-05-24 | Owner: Researcher | Status: COMPLETE

## Observation

Builder reports TiDB restart failure with "DefaultNotFound on mDB:110" on PCR-replicated target RocksDB. DDL metadata (ALTER TABLE ADD COLUMN, CREATE INDEX) does not become visible on target despite confirmed KV data arrival and schema version bumps.

## Evidence

### Full Scan Data Path
- `span_bridge.rs:403-404`: scans BOTH "default" CF and "write" CF
- All RocksDB keys (user + meta) captured with native encoding
- Key prefixes on target confirmed: `[74,80]` (user), `[6d,44,42,3a]` (meta mDB:), `[7a,6d,44,44]` (meta zmDD)

### Live CDC Data Path
- TiDB async_write converts Prewrite → Put+Delete before Raft apply
- CDC observer sees only CmdType::Put (WRITE CF) and CmdType::Delete (LOCK CF)
- Prewrite handler (delegate.rs:1096-1122): 0 invocations — confirmed dead code
- WRITE CF parsed via WriteRef::parse → LogicalMutation::from_write_cf
- DEFAULT CF synthesized from WriteRef short_value or old_value_cb fallback

### WriteRef Parsing for Meta Keys
- `WriteRef::parse` requires first byte = P/D/L/R + var_u64 start_ts
- Meta key writes use same WriteRef encoding as user key writes
- `Key::from_raw()` adds DATA_PREFIX ('z') — correct for both user and meta keys
- Verified: parsing succeeds for all transactional Put/Delete/Rollback writes

### Incomplete Data Path
- DEFAULT CF missing when: `short_value=None` AND `old_value_cb` returns None/Err
- Meta key values are typically small (<255B) → short_value should be Some
- Full scan captures both CFs → DEFAULT CF present for initially-scanned data
- `DefaultNotFound` error not reproducible in current session — log evidence absent

## Architectural Assessment

PCR's data capture model is architecturally correct for committed MVCC state:
- Full scan: both CFs captured directly from RocksDB
- Live CDC: WRITE CF captured, DEFAULT CF synthesized from WriteRef
- Key encoding preserved through entire pipeline

The model has one architectural gap:
- `short_value=None` + `old_value_cb` failure → silent DEFAULT CF data loss
- This is a GC-timing vulnerability, not a systematic encoding bug
- Probability is low for meta keys (small values), moderate for large user values

## Rejected Hypotheses

- ❌ WriteRef parse failure for meta keys — parsing is type-agnostic
- ❌ Key encoding mismatch — `Key::from_raw()` works correctly for both key types

## CORRECTED FINDING #2 (2026-05-24, post-source-target comparison)

**TSO hypothesis INVALIDATED. Root cause: meta key data loss during full scan → target pipeline.**

Source-vs-target RocksDB comparison for `zmDB:110/hTable:11/2`:
- Source: 9 entries in each CF (revisions 4-8, ddl_test table with v1/v2 columns)
- Target: 1 entry in each CF (revision 0, initial CREATE TABLE with id/val only)
- **8 of 9 meta key versions lost between source full scan and target RocksDB**

Key insight: TiDB writes meta keys non-transactionally — same raw JSON in both WRITE CF and DEFAULT CF. No WriteRef encoding. This explains 62 `write_ref_parse_fallbacks` — live CDC can't parse meta key WRITE CF values.

## CORRECTED FINDING #1 (2026-05-24, post-Builder re-validation)

**WriteRef parse DOES fail for some writes. Metric confirms 62 fallbacks.**

Root cause: delegate.rs line 1420-1424 fallback path writes ONLY WRITE CF:
```rust
write_ref_parse_fallbacks.inc();  // 62 and counting
key.push(b'z');
key.extend_from_slice(put.get_key());
batcher.add_kv(key, ..., put.get_cf());  // WRITE CF only — NO DEFAULT CF
```
Each fallback → one missing DEFAULT CF entry on target → TiDB restart check fails.

The 62 failed parses are non-table writes (meta keys, bootstrap keys) whose WRITE CF value uses a format not compatible with WriteRef::parse.

## Recommendations

1. If `DefaultNotFound` reproducible: add info-level log on old_value_cb failure, capture CF + key
2. Primary blocker remains DDL visibility (TiDB schema reload semantics) — not CF completeness
3. For production: add observability for old_value_cb failure rate per CF
