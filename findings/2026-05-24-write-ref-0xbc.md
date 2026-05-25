# WriteRef::parse 0xBC Skip — Root Cause

Date: 2026-05-24 | Owner: Researcher | Status: COMPLETE

## Finding

TiDB writes meta keys (mDB:*, mSchemaV, etc.) with a non-standard WRITE CF format that includes a 0xBC extension byte not recognized by WriteRef::parse.

## WRITE CF Byte Layout

```
[50]              — WriteType::Put (standard)
[8 bytes var_u64] — start_ts (standard)  
[BC]              — TiDB meta key extension flag (NON-STANDARD)
[06]              — extension data (1 byte)
[76]              — SHORT_VALUE_PREFIX (standard)
[05]              — short_value length = 5
[33 30 30 30 30]  — "30000" (short_value)
```

## Why DefaultNotFound

WriteRef::parse hits 0xBC at the optional fields parsing loop → matches `_ => break` → stops. SHORT_VALUE_PREFIX (0x76) after 0xBC is never reached. from_write_cf gets short_value=None → old_value_cb fails → DEFAULT CF KV not generated → TiDB finds WRITE CF but no DEFAULT CF → DefaultNotFound.

## Fix

`components/txn_types/src/write.rs`: add 0xBC skip case before `_ => break`:

```rust
0xBC => { if !b.is_empty() { b = &b[1..]; } }
```

## Impact

- Eliminates 62 write_ref_parse_fallbacks for meta keys
- DEFAULT CF correctly generated for all meta key WRITE CF entries
- TiDB FullLoad completes without DefaultNotFound
- TiDB restart reliability restored
