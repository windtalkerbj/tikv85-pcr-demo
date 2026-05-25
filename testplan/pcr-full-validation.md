# PCR FULL VALIDATION PLAN

## GOAL

验证 PCR DEMO 的：

- correctness
- replay consistency
- relay robustness
- long-run stability


---

# TEST MATRIX

## Basic Replay

- small transaction
- large transaction
- batch replay
- concurrent replay


---

## Visibility

- replay visibility
- stale read
- snapshot isolation
- target SQL visibility


---

## Failover

- target tidb restart
- target tikv restart
- relay restart
- PD restart


---

## Region Behavior

- region split
- region merge
- leader transfer


---

## GC

- gc safepoint interaction
- old version visibility


---

## Relay Robustness

- backpressure
- channel congestion
- protobuf parse failure
- replay retry


---

## Long Run

- TPCC 1h replay
- replay lag trend
- memory growth
- RocksDB growth


---

# SUCCESS CRITERIA

- zero data loss
- no silent drop
- replay monotonicity
- replay eventually visible
- no unresolved consistency issue
