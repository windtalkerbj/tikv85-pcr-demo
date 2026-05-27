# CURRENT OWNER

Researcher

---

# CURRENT PHASE

Research — #11 需要新假设

---

# DIAGNOSTIC FINDINGS (2026-05-27)

添加了 `large short_value commit` 诊断（delegate.rs, val_len > 200B 时触发）：

- 总计 2 次触发：`val_len=239B`，`region_id=12`，key_prefix `[74,80...]`
- 均为 DDL history entry，**不是 TableInfo JSON**
- TableInfo JSON（500-800B）的 Commit **从未进入 PCR batcher**
- 3 个 counter 全零佐证

### 结论
Reviewer 原假设（short_value=None → old_value_cb 未调用）**不准确**。
真实问题：TableInfo JSON Commit 事件**根本没有到达 delegate 的 PCR 代码路径**。
不是 short_value 处理问题——是 delegate PCR 覆盖或 observer 过滤问题。

### 待验证
1. TableInfo key 所在 region 的 delegate 在 Commit 时未 PCR 启用
2. CDC observer ObserveLevel 过滤了该 Commit
3. 该 Commit 使用非 CmdType::Put 类型

---

# DDL REGRESSION

| DDL | 重启后 | 
|-----|--------|
| CREATE TABLE | ✅ |
| ALTER TABLE ADD COLUMN | ✅ |
| DROP TABLE | ✅ |  
| TRUNCATE TABLE | ✅ |
| CREATE INDEX | ✅ |
| ALTER INDEX INVISIBLE + DROP INDEX | ❌ #11 |

---

# COMPLETED FIXES

- #10 FullLoad mDB DefaultNotFound ✅
- Live CDC key encoding ✅
- TiDB createReadOnlyDomain ✅
