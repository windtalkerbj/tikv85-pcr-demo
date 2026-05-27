# #11 Root Cause: short_value Overwrites Full JSON in DEFAULT CF

Date: 2026-05-27 | Owner: Researcher | Status: CONFIRMED

## Confirmed

Source vs target RocksDB byte comparison after ALTER INDEX + DROP INDEX:

| CF | Source value | Target value | Match? |
|-----|-------------|-------------|--------|
| WRITE CF | `P + var_u64(start_ts) + 0xBC06 + v + 05 + "30000"` (14B) | same | ✅ |
| DEFAULT CF | `{"id":112,"name":{...}}` full JSON (~450B) | `"30000"` (5B) | ❌ |

## Root Cause

PCR's cf="write" handler processes the WRITE CF WriteRef:
1. `from_write_cf` parses WriteRef → short_value="30000"
2. PCR writes "30000" to target DEFAULT CF

But "30000" is a TiDB version counter, NOT the actual TableInfo data.
Source TiDB reconstructs the full JSON from short_value + meta store context.
PCR only replicates the counter, losing the actual data.

The cf="" (Prewrite DEFAULT CF) event carries the full JSON. But the
cf="write" handler writes short_value to the SAME DEFAULT CF key,
overwriting or replacing the full JSON.

## Why Other DDLs Work

CREATE TABLE / CREATE INDEX: new entries with small JSON → short_value
contains the ACTUAL data (not just a counter) → PCR replicates correctly.

ALTER INDEX / DROP INDEX: modifies existing entries → short_value is a
version counter → actual data is in DEFAULT CF → PCR gets counter only.

## Fix Direction

Do NOT use short_value as DEFAULT CF value for meta keys.
Options:
a) cf="" handler takes priority over cf="write" for same key
b) Use old_value_cb to fetch full JSON from source RocksDB at start_ts
