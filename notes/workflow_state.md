# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — online DDL visibility: Reload() succeeds but internally skips schema load

---

# BUILDER HANDOFF (2026-05-28)

## Completed
- cf="" handler + HashSet + short_value strip (delegate.rs) — closes #11
- InfoSyncer nil check + 3s Reload goroutine (main.go)
- DDL regression: all types pass after restart
- pcr-restart skill/script
- Online DDL: Reload goroutine runs, Reload() returns OK, but schema not visible

## Root cause (confirmed)
`dom.Reload()` internally calls `GetSchemaVersionWithNonEmptyDiff()` which
checks if DDL diff is non-empty. PCR doesn't replicate DDL history → diff empty →
version decremented → neededSchemaVersion == currentSchemaVersion →
Reload() skips the actual schema load as a no-op.

Reload() is NOT a "force reload" — it's a "conditional reload" that checks
schema version chain integrity. The chain is broken by PCR.

## Fix direction
- Option A: Call FullLoad directly (bypass version check)
- Option B: Set PD schema version key to 0 to force FullLoad on next Reload
- Option C: Accept restart as schema reload mechanism for Demo
