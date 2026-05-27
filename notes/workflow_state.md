# CURRENT OWNER

Builder

---

# CURRENT PHASE

Build — #11 根因已定位，待修

---

# #11 根因确认（2026-05-27）

**Prewrite 被 raftstore apply 层拒绝，CDC observer 永远收不到 Prewrite 命令。**

```rust
// raftstore/src/store/fsm/apply.rs:1839 — 原版 TiKV 行为，非 PCR 引入
CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
    Err(box_err!("invalid cmd type, message maybe corrupted"))
}
```

**影响链路**：Prewrite 写 DEFAULT CF → Commit 写 WRITE CF。Commit 生成 CmdType::Put，delegate 从 WriteRef 的 short_value 合成 DEFAULT CF。大部分 key（含 mDB key）有 short_value → DML + CREATE TABLE/INDEX 正常。ALTER INDEX/DROP INDEX 产生的 mDB key 可能无 short_value → old_value_cb 在 source 读不到 → DEFAULT CF 缺失 → 重启 DefaultNotFound。

**修复方向**：delegate 的 Commit 路径（LogicalMutation::from_write_cf → old_value_cb fallback or short_value）已全覆盖。需确保所有 mDB WriteRef 格式都被正确处理。

---

# COMPLETED

- #10 FullLoad mDB DefaultNotFound ✅
- Live CDC key encoding fix ✅ 
- DDL 回归 5/6 通过 ✅
