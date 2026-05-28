# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — online DDL visibility: schema version sync across PCR

---

# BUILDER HANDOFF (2026-05-28)

## Completed
- cf="" handler + HashSet + short_value strip (delegate.rs) — closes #11
- InfoSyncer nil check + 3s Reload goroutine (main.go)
- DDL regression: all types pass after restart (FullLoad)
- pcr-restart skill/script created

## Online DDL visibility — partial
- Reload goroutine works (verified: "PCR: schema version changed, reloading [version=72]")
- But only triggers once at startup — source DDL doesn't change target schema version
- Target TiDB's InfoSchema.SchemaMetaVersion() comes from PD etcd, not from TiKV
- PCR replicates TiKV data, not PD etcd

## Fix direction
- Option A: Read schema version from TiKV meta key (mSchemaVersionKey) instead of InfoSchema version
- Option B: Blind reload every 3s (wasteful but simplest)
- Option C: Accept restart-as-schema-reload limitation for Demo

## Key insight
The schema reload mechanism is correct. The bottleneck is schema version
detection — need to poll TiKV mDB schema version key rather than relying
on in-memory InfoSchema comparison.
