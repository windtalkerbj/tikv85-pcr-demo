# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — online DDL: domain.go version bump logic designed, needs stable implementation

---

# BUILDER HANDOFF (2026-05-28)

## Completed (committed)
- #11 CLOSED: cf="" + HashSet + short_value strip (delegate.rs)
- DDL all types pass after restart

## Online DDL — designed but impl unstable
- Reload goroutine: confirmed works with Reload() call
- Reload() returns OK but internally skips (version chain broken by missing DDL diff)
- Fix designed: `domain.go loadInfoSchema()` — when PCR_READ_ONLY=1, bump `neededSchemaVersion` to `currentSchemaVersion+1` before version comparison
- domain.go change compiled but not verified due to Go cache + file corruption issues

## Fix location
`pkg/domain/domain.go:339`: Insert PCR version bump BEFORE `startTime := time.Now()`:
```go
if os.Getenv("PCR_READ_ONLY") == "1" && neededSchemaVersion <= currentSchemaVersion {
    neededSchemaVersion = currentSchemaVersion + 1
}
```

## Known issues
- main.go currently reverted to pristine v8.5.0 (PCR patches need re-application)
- Go build cache repeatedly prevented binary updates
- goroutine scope issue with `dom` variable (nil check may be blocking)
