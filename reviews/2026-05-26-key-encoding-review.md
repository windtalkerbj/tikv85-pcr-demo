# Review: delegate.rs DEFAULT CF key encoding fix

**Date**: 2026-05-26
**Phase**: Semantic Validation (Review)
**Status**: ✅ 关键路径通过，2 个 non-blocking 风险

---

## 1. DEFAULT CF key 格式一致性

**验证点**: 修复后的 DEFAULT CF key 是否与 source RocksDB 一致

- Source DEFAULT CF key: `z<mDB:1<encoded>>_(!start_ts)8B`
- Fix 后 target: `user_key + (!start_ts).to_be_bytes()` — 与 full scan 路径 `raw_data + !start_ts` 一致
- **通过** ✅。不再通过 `Key::from_raw` 重编码，避免了 memcomparable 二次编码。

## 2. WRITE CF key 格式一致性

**验证点**: delegate 发出的 WRITE CF 是否与 full scan 路径一致

- Full scan: `add_kv("write", k, v)` — `k` 是 RocksDB key（已含 `z` 前缀）
- Fix 后: `batcher.add_kv(write_key, ...)` — `write_key = cf_key.to_vec()`（CDC observer key，已含 `z`）
- **通过** ✅。不再追加额外 `z`。

## 3. Timestamp encoding

**验证点**: DEFAULT CF 的 timestamp 编码是否正确

- 使用 `!start_ts.to_be_bytes()` — 与 TiKV API v1 的 `append_ts` 一致
- **通过** ✅

## 4. 跨 CF 原子性

**验证点**: DEFAULT CF 和 WRITE CF 是否原子写入

- 两者在同一个 `LogicalMutation::Put` match arm 中，通过同一个 batcher 的两次 `add_kv` 写入
- batcher flush 发生在两者之后 → 同批次原子写入
- **通过** ✅

## 5. Delete path

**验证点**: Delete 操作的 DEFAULT CF tombstone

- tombstone: `vec![]` + OpType::Delete
- TiDB 读 WRITE CF → WriteType::Delete → 不读 DEFAULT CF → 不需要 DEFAULT CF 值
- **通过** ✅

## 6. Rollback path

**验证点**: Rollback 的 MVCC 语义

- 当前为 no-op — 正确。Rollback 表示 Prewrite 被撤销，不应有 committed state
- Full scan 可能已复制 Prewrite 的 DEFAULT CF，但 TiDB 通过 WRITE CF 检查 commit 状态，不会读到 stale DEFAULT
- **通过** ✅

## 7. ⚠️ short_value vs old_value_cb 路径分歧

**验证点**: 大 value（non-short_value）的 DEFAULT CF 复制路径

- short_value 路径: 用 embedded value 作为 DEFAULT CF — 正确
- old_value_cb 路径: 从 source RocksDB 读 DEFAULT CF — 依赖 source 的 snapshot consistency
- **风险**: old_value_cb 可能读到与 WRITE CF 不一致的 DEFAULT CF（如果 source 上有并发 compaction/GC）
- **评估**: Demo 级别可接受。生产级需要 snapshot-consistent read。
- **结论**: ⚠️ 已知风险，不妨碍 Demo

## 8. ⚠️ TiDB bare domain 的 schema 缺失

**验证点**: createReadOnlyDomain 创建的 bare domain 能否正确服务查询

- Bare domain 有 infoCache（空）和 sysSessionPool（lazy factory）
- 首次查询触发 session 创建 → CreateSession 可能读 mDB key → DefaultNotFound
- Schema reload 机制（5s interval）触发 `loadInfoSchema` → FullLoad → 可能 DefaultNotFound
- **结论**: ⚠️ 启动不 panic，但运行时 schema 加载可能失败。需进一步验证 FullLoad 是否能通过修复后的 key 正确读取 mDB。

---

# Summary

| # | 检查项 | 结果 |
|---|--------|------|
| 1 | DEFAULT CF key 格式 | ✅ |
| 2 | WRITE CF key 格式 | ✅ |
| 3 | Timestamp encoding | ✅ |
| 4 | 跨 CF 原子性 | ✅ |
| 5 | Delete path | ✅ |
| 6 | Rollback path | ✅ |
| 7 | old_value_cb 一致性 | ⚠️ Demo 可接受 |
| 8 | TiDB schema 加载 | ⚠️ 待进一步验证 |

**判定**: 修复方案在 Demo 级别正确。2 个 ⚠️ 风险已知，不妨碍 Demo 运行。

**建议**: handoff 回 Builder 执行 regression（live CDC + DDL + TiDB restart after live CDC）。
