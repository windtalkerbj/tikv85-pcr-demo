# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — cf="" fix implemented but #11 not resolved

---

# WHAT WE FOUND (2026-05-27)

## on_batch diagnostic confirmed
meta key raw JSON (`{...}`) Commit comes through cf="" (DEFAULT CF), NOT cf="write".
delegate PCR code explicitly skipped cf="" events (comment: "DEFAULT CF puts are NOT replicated").

## Fix implemented
Both sink_raw_put and sink_txn_put: added `else` branch for cf="" / "default".
Writes raw value directly to DEFAULT CF on target. Compiles clean.

## Fix did NOT resolve #11
61 errors on restart after ALTER INDEX + DROP INDEX.
Raw JSON IS being written to target DEFAULT CF, but TiDB still can't read it.
Root cause may be: DEFAULT CF key format mismatch, or value at wrong timestamp.

---

# RULED OUT
1. ❌ old_value_cb fallback — never triggered
2. ❌ lock_tracker stuck — region 2 at All
3. ❌ LockRelated filtering — capture_change acknowledged
4. ❌ cf="" skip — fix implemented, doesn't help

---

# DDL REGRESSION
5/6 pass. Only ALTER INDEX INVISIBLE + DROP INDEX fails (#11).

---

# COMPLETED FIXES
- #10 FullLoad mDB DefaultNotFound ✅
- Live CDC key encoding ✅
- TiDB createReadOnlyDomain ✅
- old_value_cb fallback ✅
- lock_tracker Prepared ✅
- cf="" handling in delegate ✅ (correct but insufficient)
