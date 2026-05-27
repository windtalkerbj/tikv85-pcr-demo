# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11: cf=""→cf="write" overwrite confirmed, fix needs expansion

---

# ROOT CAUSE (2026-05-27)
cf="" handler writes full TableInfo JSON (~450B) to DEFAULT CF.
cf="write" handler also writes DEFAULT CF (short_value, old_value_cb, fallback) to SAME key.
Short_value="30000" (version counter) overwrites full JSON.
Target TiDB reads version counter → crash.

# Fix implemented (partial)
HashSet tracks cf="" keys. short_value path checks & skips. Compiles ✅.
Still 56 errors — old_value_cb + WriteRef fallback paths NOT protected.
Need to protect ALL cf="write" DEFAULT CF writes.

# RULED OUT (8)
old_value_cb | lock_tracker | LockRelated | IngestSst | CF routing | cf="" skip | z-prefix | short_value-only

# DDL: 5/6 pass (#11 remains)
