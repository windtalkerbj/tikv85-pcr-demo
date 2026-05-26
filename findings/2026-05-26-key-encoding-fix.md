# Finding: live CDC 路径 key 编码修复（历经 3 次迭代）

**Date**: 2026-05-26
**Phase**: Build
**Status**: ✅ Fixed & verified (commit `b70042a`)

---

## Root Cause

两个问题叠加：

### 问题 1：CDC observer key 无 `z` 前缀（被误判）

**事实**：CDC observer 传来的 key 是 `memcomparable(user_key) + TS`，**不带** `z` DATA_PREFIX。
（`z` 由 `handle_put` 在 apply 写 RocksDB 时追加。）

**后果**：delegate 必须补 `z`。原代码中 `wk.push(b'z')` 是正确的。

**误判经过**：
- 第一次分析看到 full scan 路径 key 以 `7a`（z）开头 → 认为 delegate 的 `z` 是多余的 → 删掉
- 结果：live CDC 写入的 key 缺少 `z` 前缀，TiDB 找不到

### 问题 2：`Key::from_raw` 对已 memcomparable 编码的 key 二次编码

**事实**：`truncate_ts_for(cf_key)` 返回的 `user_key` 已是 memcomparable 编码格式。

**原代码**：
```rust
let default_key = Key::from_raw(user_key)
    .append_ts(write.start_ts)
    .into_encoded();
```

`Key::from_raw` 对 `user_key` 再做 memcomparable 编码 → 含 `\x00`/`\xff` 等字节的 mDB key 被变换 → key 不一致 → DefaultNotFound。

---

## 最终正确修复（第 3 次迭代）

### logical_mutation.rs

```rust
// 最终正确版本：z + memcomparable(user_key) + !start_ts
let build_default_key = |start_ts: TimeStamp| {
    let mut dk = Vec::with_capacity(1 + user_key.len() + 8);
    dk.push(b'z');   // CDC observer key 无 z → 补上
    dk.extend_from_slice(user_key);  // 已是 memcomparable，不重编码
    dk.extend_from_slice(&(!start_ts.into_inner()).to_be_bytes());
    dk
};
```

### delegate.rs

- WRITE CF happy path & fallback: **保留** `wk.push(b'z')`（正确）
- DEFAULT CF fallback: **保留** `dk.push(b'z')`（正确）

### TiDB main.go

`createReadOnlyDomain`：创建 bare domain + `StartSchemaLoad()`（5s periodic reload），DDL 变更通过周期 schema reload 在线可见。

---

## 迭代记录

| 迭代 | 理解 | 改动 | 结果 |
|------|------|------|------|
| 1 | CDC key **有** z → 去掉 delegate 的 z | 删 `wk.push(b'z')`, `dk.push(b'z')` | ❌ target 上 key 缺 z |
| 2 | CDC key **有** z → 只修 DEFAULT CF | WRITE CF 去掉 z，DEFAULT CF 去掉 z | ❌ 同上 |
| 3 | CDC key **无** z → 补 z，只修 from_raw | WRITE CF 保留 z，DEFAULT CF 改为 `z + user_key + !start_ts` | ✅ 全部通过 |

---

## Validation

- DML full scan + TiDB 启动 ✅
- Live CDC INSERT/UPDATE/DELETE 即时可见 ✅
- Live CDC 后 TiDB 重启无 crash ✅
- CREATE TABLE 在线可见（diff load）✅

---

## Remaining Issue

FullLoad 在 mDB key 上持续 DefaultNotFound——span_bridge.rs 的 no_short_value fallback 写入的 DEFAULT CF 值不正确。不影响 DML 和 CREATE TABLE（diff load 替代 FullLoad），但 ALTER TABLE 等需要 FullLoad 的 DDL 在线不可见。
