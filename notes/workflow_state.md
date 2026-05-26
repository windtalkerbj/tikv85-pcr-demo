# CURRENT OWNER

Builder

---

# CURRENT PHASE

Build — DDL 在线可见性部分完成，FullLoad DefaultNotFound 待修

---

# COMPLETED TODAY (2026-05-26)

## 1. Key encoding fix for live CDC (3 次迭代)

**最终正确修复** (commit `b70042a`):
- `logical_mutation.rs`: `default_key` = `z + user_key + !start_ts`，不通过 `Key::from_raw`
- `delegate.rs`: 保留 WRITE CF `z` 前缀（CDC observer key 无 `z`）
- `delegate.rs`: fallback 路径保留 `z` 前缀

## 2. TiDB createReadOnlyDomain + StartSchemaLoad

- `main.go`: bare domain + `StartSchemaLoad()`（5s reload loop）
- `tidb.go`: 已恢复（被 sed 损坏后从 .bak 恢复，PCR 改动在之前已 commit）

## 3. 验证结果

| 测试 | 结果 |
|------|------|
| Full scan + TiDB 启动 | ✅ |
| Live CDC DML | ✅ 即时可见 |
| Live CDC 后 TiDB 重启 | ✅ 无 crash |
| CREATE TABLE 在线可见 | ✅ schema diff load 成功 |
| ALTER TABLE 在线可见 | ❌ FullLoad DefaultNotFound |
| DROP TABLE | ✅ 重启后可见 |
| TRUNCATE | ✅ 重启后可见 |

---

# CURRENT BLOCKER

FullLoad 反复失败：mDB key DEFAULT CF 在 target RocksDB 上不存在或值错误。
span_bridge.rs full scan 的 no_short_value fallback 用 WriteRef metadata 作为 DEFAULT CF 值写入，
TiDB 读 WRITE CF → WriteRef 有 short_value → 但代码路径中仍有某个地方绕过了 short_value 直接读 DEFAULT CF。

error 示例：`DefaultNotFound { key: [109, 68, 66, 58, 49, 49, 50, ...] }` = `mDB:112`

---

# LESSONS LEARNED (2026-05-26)

1. 先用 `develop/pcr-configs/` + `tikv85/target/debug/tikv-server`，不手写 config
2. CDC observer key 无 `z` 前缀 → delegate 必须补 `z`
3. sed 改 Go 代码极易损坏，≤3 行改动用 Edit tool，批量符号替换才用 sed
4. 日志分析超 3 条无结论 → 直接重启测试
5. 每轮只改一个概念，编译通过再改下一个
