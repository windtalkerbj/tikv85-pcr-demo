# End-to-End Byte Trace: ALTER INDEX Meta Key

Date: 2026-05-27 | Owner: Researcher | Status: PLANNED

## Trace Plan

After ALTER INDEX on source, capture the EXACT bytes at each layer:

| Layer | What to capture | Method |
|-------|----------------|--------|
| 1. Raft command | key + value for cf="" Put | observer.rs diagnostic log |
| 2. Delegate cf="" | key + value after z-prefix add | sink_raw_put diagnostic log |
| 3. Batcher output | PcrKvBatch serialized bytes | batcher flush diagnostic |
| 4. Relay | parsed PcrEvent bytes | relay parse log |
| 5. Target ingest | key + value written to RocksDB | task.rs diagnostic |
| 6. Target RocksDB | raw-scan verification | tikv-ctl raw-scan |

## Expected Key Format

Source DEFAULT CF: `z + data_key + !start_ts` (inverted, big-endian)
PCR cf="" handler: `z` + `put.get_key()` = `z + data_key + !start_ts`

These SHOULD match. Need byte-level evidence to confirm/deny.

## Items to Verify

1. Does `put.get_key()` for cf="" include the timestamp suffix?
2. Is the timestamp encoding (!ts, inverted big-endian) preserved?
3. Does the target RocksDB key match source byte-for-byte?
4. If it matches, the issue is elsewhere (MVCC visibility, WriteRef parsing, etc.)
5. If it doesn't match, fix the key encoding
