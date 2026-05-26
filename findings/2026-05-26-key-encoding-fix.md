# Finding: live CDC 路径 key 编码与 full scan 路径不一致

**Date**: 2026-05-26
**Phase**: Build
**Status**: ✅ Fixed & verified

---

## Root Cause

CDC delegate（live CDC 路径）和 SpanBridge（full scan 路径）对 WRITE CF / DEFAULT CF 的 key 编码不同，导致 target RocksDB 上同一 key 的 WRITE CF 和 DEFAULT CF 以不同格式存储。

### 具体问题

**1. WRITE CF key 多 `z` 前缀**

- Full scan: `add_kv("write", k, v)` — `k` 直接从 RocksDB 读取，已含 `z` 前缀
- delegate.rs: `wk.push(b'z'); wk.extend_from_slice(&write_key)` — `write_key` 已是 RocksDB key（含 `z`），再加 `z` → `zz...`

**2. DEFAULT CF key 通过 `Key::from_raw` 重编码**

- Full scan: `raw_data + !start_ts` — `raw_data = k[..len-8]`，保留原始编码
- delegate.rs: `Key::from_raw(user_key).append_ts(start_ts).into_encoded()` — `user_key` 已被 `truncate_ts_for` 截断但仍是编码格式，`from_raw` 对其再做 memcomparable 编码 → key 变形

**3. mDB meta key 是触发条件**

- 用户数据 key 多为 ASCII，memcomparable 编码是 identity，`from_raw` 重编码无影响
- mDB key 含二进制编码字节（如 `\x00`, `\xff`），memcomparable 编码会变换这些字节 → key 不一致 → TiDB 读 DEFAULT CF 时 DefaultNotFound

### 证据

- Source RocksDB 的 WRITE CF key 以 `z` 开头（hex `7a`）— span_bridge.rs diagnostic log 确认
- `Key::truncate_ts_for` 返回的 `user_key` 仍含 `z` 前缀 — 注释说 "without z prefix" 与代码实际行为不符
- TiDB 重启日志：`DefaultNotFound { key: [109, 68, 66, 58, ...] }` — `mDB:` key

---

## Fix

### logical_mutation.rs

```rust
// Before (broken):
let default_key = Key::from_raw(user_key)
    .append_ts(write.start_ts)
    .into_encoded();

// After (fixed):
let mut default_key = Vec::with_capacity(user_key.len() + 8);
default_key.extend_from_slice(user_key);
default_key.extend_from_slice(&(!write.start_ts.into_inner()).to_be_bytes());
```

### delegate.rs

WRITE CF: 直接用 `write_key`/`raw_key`，不追加 `z`。
DEFAULT CF (fallback): 直接用 `data_key`，不追加 `z`。

### TiDB main.go

`createReadOnlyDomain` 改为创建 bare domain + lazy session factory，避免 BootstrapSession 的 schema load 触发 DefaultNotFound 导致 nil domain → `createServer` panic。

---

## Validation

- Target TiDB (PCR_READ_ONLY=1) 启动成功，无 crash
- `SELECT * FROM test_pcr.t1` 返回 3 rows
- PCR full scan 66 regions, Standby ready
- 集群使用 `develop/pcr-configs/pcr-{src,tgt}-tikv.toml` 和 `tikv85/target/debug/tikv-server`

---

## Tradeoffs

- `Key::from_raw` 提供 API v1/v2 可移植性 — 但我们固定用 API v1，此抽象在此场景多余
- Bare domain 不加载 schema → TiDB 启动后 schema cache 为空，需要后续 schema reload 补齐（已有 5s reload 机制）
- lazy factory 的 CreateSession 可能失败 → 查询报错而非 panic（更好的行为）
