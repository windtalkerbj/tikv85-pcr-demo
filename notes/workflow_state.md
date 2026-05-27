# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11 partial fix: DEFAULT CF protected, WRITE CF short_value still corrupt

---

# BUILDER HANDOFF (2026-05-27)

## What was fixed
cf="" handler writes full TableInfo JSON to DEFAULT CF. cf="write" handler
overwrote same key with version counter. HashSet now tracks cf="" keys,
prevents DEFAULT CF overwrite. Works for basic DDL (CREATE TABLE, ALTER
TABLE ADD COLUMN, CREATE/DROP INDEX, DROP TABLE, TRUNCATE).

## What remains broken
WRITE CF still has wrong short_value. TiDB reads WRITE CF first, uses
embedded short_value directly, never reads the correct DEFAULT CF.
RENAME TABLE, DROP COLUMN, ALTER INDEX RENAME write to mDB keys with
different short_value semantics — crash on restart.

## Fix needed
When cf="" has provided real data for a key, cf="write" must also strip
short_value from WRITE CF (set to None), forcing TiDB to read DEFAULT CF.
Need Researcher to analyze mDB key WriteRef semantics per operation type.

## DDL regression status
7 basic DDL types: ✅
RENAME TABLE / DROP COLUMN / ALTER INDEX RENAME: ❌ (same root, uncovered paths)
