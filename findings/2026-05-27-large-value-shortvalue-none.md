# Large-Value TableInfo Live CDC Replay Gap

Date: 2026-05-27 | Owner: Researcher | Status: ACTIVE

## Corrected CF Model (2026-05-27)

TiKV MVCC uses THREE real column families:
- DEFAULT CF — stores values (large data)
- WRITE CF — stores WriteRef (commit metadata, may inline short_value)
- LOCK CF — stores transaction locks

`cf=""` in API/protobuf = DEFAULT CF alias. NOT a separate CF.

Large values: Prewrite→DEFAULT CF, Commit→WRITE CF (short_value=None)
Small values: Commit→WRITE CF (short_value=Some, inline)

## CF Routing in PCR

sink_raw_put correctly skips cf="" (DEFAULT CF) — prevents uncommitted data leak.
Only cf="write" is processed via from_write_cf → generates DEFAULT CF from short_value or old_value_cb.

## Confirmed Behavior

- cf="" batches present during ALTER INDEX (DEFAULT CF writes for large JSON)
- cf="write" batches present (WRITE CF writes with WriteRef)
- short_value_missing_count=0 → all cf="write" Commits have short_value=Some

## Open Question

How does TiDB encode TableInfo JSON (>255B) in WriteRef.short_value (<255B)?
Source WRITE CF dump needed: extract actual short_value bytes to determine if
it's compressed delta, partial struct, or something else.
