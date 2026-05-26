# CURRENT OWNER

Builder

---

# CURRENT PHASE

Build — Regression 完成

---

# REVIEWER RULING (2026-05-26)

- 第一次 review: key encoding fix ✅ 6/8 通过，2 ⚠️ non-blocking
- 第二次 review: 发现 delegate WRITE CF 错误去掉 `z` 前缀，修复后全部通过

---

# REGRESSION RESULTS (2026-05-26, 第三次迭代)

第三次编译（正确修复：CDC observer key 无 `z` → delegate 补 `z` + logical_mutation 直接拼接不从 from_raw 重编码）：

| 测试 | 结果 |
|------|------|
| Full scan + TiDB 启动 | ✅ 无 crash |
| Live CDC INSERT/UPDATE/DELETE | ✅ 数据即时可见 |
| Live CDC 后 TiDB 重启 | ✅ 无 crash，数据完整 |
| CREATE TABLE during PCR | ✅ 重启后可见 |
| ALTER TABLE ADD COLUMN | ✅ 重启后可见 |
| DROP TABLE | ✅ 重启后确认已删除 |
| TRUNCATE TABLE | ✅ 重启后可见 |
| CREATE INDEX | ⚠️ 磁盘空间不足（非 PCR 问题）|

---

# FILES CHANGED (final)

| 文件 | 修改 |
|------|------|
| `tikv85/components/cdc/src/logical_mutation.rs` | `default_key` 改为 `z + user_key + !start_ts`，不通过 `Key::from_raw` 重编码 |
| `tikv85/components/cdc/src/delegate.rs` | WRITE CF happy path: 保留 `z` 前缀（正确）；fallback: 保留 `z` 前缀（正确） |
| `offcial-tidb-8.5.6/cmd/tidb-server/main.go` | `createReadOnlyDomain` 创建 bare domain，不 nil panic |

# KNOWN LIMITATIONS

- DDL 需要重启 target TiDB 才能看到（bare domain 不做 online schema reload）
- CREATE INDEX 因磁盘空间不足无法测试
