# CURRENT OWNER

Builder

---
# CURRENT PHASE

Build (delegate.rs: enable_pcr force-push lock_tracker to Prepared)

---
# ROOT CAUSE (2026-05-27)
Region 2 (meta table region) stuck at LockRelated CDC level, never All.
Other regions reach All → min_level=All → region 2 LockRelated batches filtered → meta DDL never enters PCR.

Full scan works (reads RocksDB directly, no CDC observe).
DML works (user table regions reach All quickly).
ALTER/INDEX fails (region 2 never upgrades from LockRelated).

---
# REVIEWER RULING (2026-05-27)
- **Decision:** RETURN TO BUILDER
- **Fix:** enable_pcr() already has lock_tracker force-push logic. Verify it covers the LockRelated → Prepared transition for stuck regions.

### Builder Task
1. In delegate.rs enable_pcr(): ensure `lock_tracker = LockTracker::Prepared` for ALL PCR delegates, bypassing lock resolution wait
2. PCR copies committed MVCC — doesn't need lock tracking
3. Compile → restart → verify ALTER TABLE/INDEX visible on target without TiDB restart
