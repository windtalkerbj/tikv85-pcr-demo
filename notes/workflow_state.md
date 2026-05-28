# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — online DDL visibility: Reload goroutine works but schema load blocked by version check

---

# BUILDER HANDOFF (2026-05-28)

## Completed
- #11 CLOSED: cf="" handler + HashSet + short_value strip (delegate.rs)
- InfoSyncer nil check (main.go createServer)
- Reload goroutine with PD key write (main.go createStoreDDLOwnerMgrAndDomain)
- pcr-restart skill + script
- DDL regression: 10/10 types pass after restart

## Online DDL — current state
- Blind Reload every 3s: goroutine confirmed running ("reload ok" ×15 in log)
- Reload() returns nil (no error) but does NOT actually load new schema
- Root cause: loadInfoSchema → GetSchemaVersionWithNonEmptyDiff → DDL diff empty → version-1 → same as current → skip
- PD key write (/tidb/ddl/global_schema_version = "0") also tried — no effect (no watcher on target)

## Fix direction
Need to bypass schema version comparison in loadInfoSchema() for PCR mode.
Options:
A. Skip version check when PCR_READ_ONLY=1 (modify domain.go loadInfoSchema)
B. Force FullLoad path directly in the goroutine
C. Set PD version to 0 AND ensure schema syncer watches it
