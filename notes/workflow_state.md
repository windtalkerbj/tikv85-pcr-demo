# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11 根因需要重新假设

---

# BUILDER HANDOFF (2026-05-27)

Builder 层面的排查和修复尝试已完成，但 #11 仍未解决。需要 Researcher 重新分析。

## 已排除的假设

1. **Prewrite 未捕获** — raftstore apply.rs:1839 拒绝 Prewrite（原版 TiKV 行为），delegate 的 CmdType::Prewrite 分支是 dead code。但 DML + CREATE TABLE/INDEX 都正常，说明 Commit 路径的 short_value 合成 DEFAULT CF 对大多数 key 是够的。

2. **old_value_cb 失败** — PCR metrics 显示 `short_value_missing_count=0`、`old_value_cb_failures=0`、`write_ref_parse_fallbacks=0`。所有 mDB key 都有 short_value，old_value_cb fallback 从未触发。添加的 fallback 代码不生效。

3. **tidb.go error catch** — 添加 "index out of range" catch 后仍然 32 errors。BootstrapSession 的 Init 在读取 mDB 数据时崩溃，不是 DefaultNotFound。

## Builder 已实施但无效的修复

- delegate.rs: old_value_cb 失败时写空 DEFAULT CF（未触发）
- tidb.go: catch "index out of range" + DefaultNotFound（仍 32 errors）
- span_bridge.rs: #10 full scan mDB fallback（有效）

## 未解释的现象

- CREATE INDEX alone → restart OK
- ALTER INDEX INVISIBLE + DROP INDEX → restart crash
- 两者走相同的 delegate 代码路径，但结果不同
- 重启时 BootstrapSession 读 mDB 数据 crash，不是 DefaultNotFound

## Researcher 任务

提出新假设，解释为什么 ALTER INDEX/DROP INDEX 产生不可读的 mDB 数据，而 CREATE INDEX 正常。
