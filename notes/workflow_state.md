# CURRENT OWNER

Reviewer

---
# CURRENT PHASE

Semantic Validation (final)

---
# ACHIEVED (2026-05-25)

## TiKV PCR fixes
| Fix | File | What |
|-----|------|------|
| Relay observability | span_bridge.rs | Arc<AtomicU64> counters survive relay restarts |
| TSO bumper | task.rs | 1s interval, 500K batch, <2s visibility |
| WriteRef fallback DEFAULT CF | delegate.rs | Both CFs with correct TS suffix |
| Full scan cross-CF synthesis | span_bridge.rs | Synthesize DEFAULT CF from short_value + old_value_cb |
| PD schema version sync | schema_sync.rs | Read source PD etcd v3 → write target PD |

## TiDB fixes (offcial-tidb-8.5.6)
| Fix | File | What |
|-----|------|------|
| Online FullLoad trigger | domain.go | 0 diffs → force FullLoad |
| PCR read-only bootstrap | main.go | DefaultNotFound → createReadOnlyDomain |
| Both TiDBs use same version | - | v8.5.6 source + target |

## DDL Regression
| Operation | Result | Mechanism |
|-----------|--------|-----------|
| Full scan startup | ✅ | cross-CF fix + read-only domain |
| CREATE TABLE | ✅ | SpanBridge rediscovery + full scan |
| DROP TABLE | ✅ | DeleteRange CDC |
| TRUNCATE TABLE | ✅ | DeleteRange + scan |
| Lightning LOCAL | ✅ | Rediscovery + full scan |
| INSERT/UPDATE/DELETE | ✅ | Live CDC + TSO bumper |
| ALTER TABLE online | ⚠️ | FullLoad runs but KV data incomplete |
| CREATE INDEX online | ⚠️ | Same as above |

## Demo Limitations
1. ALTER TABLE / CREATE INDEX online not visible (need TiDB full restart on fresh PCR)
2. TiDB restart after PCR activity unreliable (meta key DefaultNotFound)
3. Target TiDB must use PCR read-only mode

