# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11 根因需要重新假设

---

# WHAT WAS TRIED (2026-05-27)

1. **old_value_cb fallback** (delegate.rs, commit `55aa55f`)
   → 无效。old_value_cb 从未触发——所有 Commit short_value=Some。

2. **caps_change lock_tracker → Prepared** (delegate.rs, commit `dec21a4`)
   → 无效。Region 2 的 capture_change 早已 acknowledged（ObserveLevel=All），
   不是 LockRelated 卡住的问题。

3. **诊断日志**：large short_value commit（val_len > 200B）
   → 2 次触发：均为 DDL history entry（239B, region 12）。
   TableInfo JSON（500-800B）的 Commit **从未进入 PCR batcher**。
   3 个 PCR counter 全零佐证。

# KEY EVIDENCE

- Region 2 capture_change acknowledged 2 次（13:38 & 13:57）
- 系统目录 region（4,8,12,16,40）有 sink_data 活动，cmd_types 只有 Put/Delete
- 无 Prewrite 事件（raftstore apply.rs:1839 拒绝，原版 TiKV 行为）
- TableInfo JSON Commit 不进入 PCR 路径的原因待查——不是 observer 过滤

# DDL REGRESSION

| DDL | 重启后 |
|-----|--------|
| CREATE TABLE | ✅ |
| ALTER TABLE ADD COLUMN | ✅ |
| DROP TABLE | ✅ |
| TRUNCATE TABLE | ✅ |
| CREATE INDEX | ✅ |
| ALTER INDEX INVISIBLE + DROP INDEX | ❌ #11 |

# COMPLETED FIXES

- #10 FullLoad mDB DefaultNotFound ✅ (span_bridge.rs)
- Live CDC key encoding ✅ (delegate.rs + logical_mutation.rs)
- TiDB createReadOnlyDomain ✅ (main.go)
- lock_tracker → Prepared ✅ (delegate.rs, 有效改进但未修 #11)
- old_value_cb fallback ✅ (delegate.rs, 预防性改进)
