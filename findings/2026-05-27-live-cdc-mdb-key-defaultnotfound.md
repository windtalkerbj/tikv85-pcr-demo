# Finding: Live CDC DDL meta key DEFAULT CF 缺失 (#11)

**Date**: 2026-05-27
**Phase**: Build
**Status**: Root cause identified, fix pending

---

## Root Cause

**Prewrite commands are rejected at the raftstore apply layer — this is ORIGINAL TiKV behavior, not PCR-introduced.**

```rust
// raftstore/src/store/fsm/apply.rs:1839 (both original TiKV 8.5.6 and our PCR version)
CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
    Err(box_err!("invalid cmd type, message maybe corrupted"))
}
```

The `CmdType::Prewrite` branch in `delegate.rs:1096` is dead code — it will never execute.

## Impact Chain

1. TiDB DDL writes mDB meta keys via Prewrite (DEFAULT CF) + Commit (WRITE CF)
2. Raft apply rejects Prewrite → CDC observer never sees it
3. CDC observer only delivers Commit events (CmdType::Put)
4. Delegate's `sink_data` processes Commit via `LogicalMutation::from_write_cf`
5. DEFAULT CF must be synthesized from WriteRef metadata:
   - **short_value path**: embedded value → DEFAULT CF correct ✅
   - **old_value_cb path**: read source RocksDB DEFAULT CF at start_ts → may fail ❌

## Why Some DDL Works But Not Others

| DDL | short_value available? | Result |
|-----|----------------------|--------|
| CREATE TABLE | ✅ (DDL history entry, small JSON) | ✅ |
| ALTER TABLE ADD COLUMN | ✅ | ✅ |
| CREATE INDEX | ✅ (small metadata) | ✅ |
| ALTER INDEX INVISIBLE | ❓ (possibly larger metadata) | ❌ |
| DROP INDEX | ❓ | ❌ |

ALTER INDEX INVISIBLE + DROP INDEX may produce mDB keys without short_value or with a WriteRef format that triggers old_value_cb failure.

## Evidence

- Source TiKV log: `PCR: sink_data batch` cmd_types only show "Put" and "Delete" — never "Prewrite"
- Source TiKV log: "Prewrite captured" and "Prewrite SKIPPED" never appear
- Original TiKV 8.5.6 has identical Prewrite rejection code (confirmed via diff)

## Fix Options

### Option A: Fix old_value_cb fallback (low risk, Demo recommended)
Ensure mDB keys that fail old_value_cb get a valid DEFAULT CF entry (e.g., write empty value at correct start_ts key). TiDB uses short_value embedded in WRITE CF for most reads — DEFAULT CF only matters for non-short_value WriteRefs.

### Option B: Let Prewrite through (high risk, not for Demo)
Modify raftstore apply.rs to pass Prewrite to CDC observer. Breaks "no Prewrite in apply" invariant — unknown side effects on Raft correctness. Not worth Demo risk.

**Recommendation**: Option A, add fallback DEFAULT CF synthesis in delegate.rs when old_value_cb fails.
