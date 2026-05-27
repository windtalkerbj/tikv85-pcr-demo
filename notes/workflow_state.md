# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11: 7 hypotheses ruled out, cf="" path confirmed but fix incomplete

---

# ROOT CAUSE (2026-05-27)

meta key raw JSON (`{...}`) Commit comes through cf="" (DEFAULT CF), NOT cf="write".
Raft command key lacks `z` DATA_PREFIX. Delegate explicitly skipped cf="" events.

## Fixes attempted
1. cf="" handling (sink_raw_put + sink_txn_put) — compiles ✅
2. + z-prefix on DEFAULT CF key — compiles ✅
3. Both deployed, tested → still 54 errors on restart

## Why still fails
Likely: cf="" event key timestamp is commit_ts, TiDB expects DEFAULT CF at start_ts.
Causes key mismatch → TiDB can't find DEFAULT CF → BootstrapSession crash.

## RULED OUT (all 7)
old_value_cb | lock_tracker | LockRelated | IngestSst | CF routing | cf="" skip | z-prefix

---

# DDL REGRESSION
5/6 pass. ALTER INDEX INVISIBLE + DROP INDEX (#11) remains.

---

# COMPLETED FIXES
- #10 FullLoad mDB DefaultNotFound ✅
- Live CDC key encoding ✅
- TiDB createReadOnlyDomain ✅
