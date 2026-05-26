# CURRENT OWNER

Builder

---

# CURRENT PHASE

Build 完成 — #10 FullLoad DefaultNotFound 已修

---

# COMPLETED TODAY (2026-05-26)

## 1. Key encoding fix for live CDC (commit `b70042a`)

- `logical_mutation.rs`: `default_key` = `z + user_key + !start_ts`
- `delegate.rs`: 保留 WRITE CF `z` 前缀（CDC observer key 无 `z`）

## 2. span_bridge.rs FullLoad DefaultNotFound fix (待 commit)

- 2 行改动：DEFAULT CF key 用 start_ts 替 commit_ts，value 用空值替 WriteRef metadata
- FullLoad 成功 (11.6ms)，0 条 schema DefaultNotFound

## 3. TiDB createReadOnlyDomain + StartSchemaLoad

- `main.go`: bare domain + 5s periodic schema reload
- CREATE TABLE / ALTER TABLE 在线可见

## 4. 全部验证通过

| 测试 | 结果 |
|------|------|
| Full scan + TiDB 启动 | ✅ |
| Live CDC DML | ✅ 即时可见 |
| Live CDC 后 TiDB 重启 | ✅ |
| CREATE TABLE 在线 | ✅ |
| ALTER TABLE ADD COLUMN 在线 | ✅ |
| DROP TABLE | ✅ |
| TRUNCATE | ✅ |
| FullLoad DefaultNotFound | ✅ 0 条 |
