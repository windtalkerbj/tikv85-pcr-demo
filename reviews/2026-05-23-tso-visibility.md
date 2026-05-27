# Review: TSO Bumper Tuning

**Date:** 2026-05-23 | **Decision:** RETURN TO BUILDER → CLOSED

## Evidence
- Data reaches target RocksDB (MAX(id) matches source)
- SQL COUNT(*) lags 30-60s due to TSO visibility gap
- Source-target TSO gap ~3.1M ticks, bumper 100K/5s insufficient

## Options Reviewed
- (a) Strengthen TSO bumper: increase batch or frequency. 1 line, zero risk.
- (b) commit_ts rewrite: eliminate gap, but introduces cross-TSO-domain MVCC risk. Production only.

## Decision
(a), tighten interval 5s→1s. 500K/1s = 500K ticks/s. Gap 30-60s → <2s.

## Rejected
(b) commit_ts rewrite: mixed TSO domains corrupt MVCC version chains. Reopen Research threshold not met.

## Validation
- INSERT convergence: <2s (was 30-60s)
- TSO gap: 1M ticks (~1ms physical)
