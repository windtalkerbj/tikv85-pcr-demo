# CURRENT OWNER

Builder

---
# CURRENT PHASE

Build (#11 final fix: strip short_value from WRITE CF when cf="" has real data)

---
# ROOT CAUSE (2026-05-27)
WRITE CF short_value="30000" overwrites DEFAULT CF full JSON.
TiDB reads short_value directly, never looks at DEFAULT CF.

## Fix
When HashSet has the key (cf="" wrote full JSON), strip short_value
from WriteRef before writing WRITE CF:
```rust
write.short_value = None;
let new_val = write.as_ref().to_bytes();
batcher.add_kv(wk, new_val, "write");
```
Forces TiDB to read DEFAULT CF → finds full JSON.

## DDL status
7 basic types: ✅ (DEFAULT CF overwrite fixed)
RENAME/DROP COLUMN/ALTER INDEX RENAME: ❌ → this fix resolves
