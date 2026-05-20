# PCR Live CDC 路径修复全程

## 问题现象

全量扫描正常，Pause+Resume delta scan 正常，但 PCR 运行期间的 INSERT/UPDATE/DELETE 无法到达目标端 TiDB。

## 数据流

```
TiDB DML → TiKV Raft → Apply → CmdBatch → CDC Observer
  → delegate.on_batch() → sink_data() → PCR batcher
  → gRPC → Consumer → SstBatcher → DirectIngest → 目标 RocksDB
```

## 坑 1：Prewrite 命令被静默跳过

### 现象
Consumer 收不到任何 live CDC 数据。

### 排查
`delegate.rs` 的 `sink_data` 函数对每个 Raft 命令做 match：
- `CmdType::Put` → 正常处理
- `CmdType::Prewrite` → **没有分支**，落入 `_ => debug!("skip")`

### 根因
TiDB 的事务写（INSERT/UPDATE/DELETE）走 Percolator 2PC：
- Prewrite 阶段产生 `CmdType::Prewrite`（写入 DEFAULT CF + LOCK CF）
- Commit 阶段产生 `CmdType::Put(cf="write")` + `CmdType::Delete(cf="lock")`

`CmdType::Prewrite` 不在 match 分支中，被静默跳过，DEFAULT CF 数据丢失。

### 修复
`delegate.rs` 加 Prewrite 分支，从 lock 中提取 start_ts，手动编码 key，写入 PCR batcher。

---

## 坑 2：Key Memcomparable 编码不匹配

### 现象
consumer 收到 live CDC 事件，flush 到 RocksDB，但 TiDB SELECT 看不到数据。

### 排查
Delta scan（pause+resume）工作正常，说明 scan 路径正确。对比两条路径的 key 编码：

**Scan 路径**：直读 RocksDB → key 格式 `z + rawkey + ts_suffix`
**Prewrite handler**：`Key::from_raw(rawkey).append_ts(ts).into_encoded()`

`Key::from_raw()` 没有简单加 `z` 前缀，而是调用了 `codec::bytes::encode_bytes()`——**memcomparable 编码**：
```
每 8 字节分组 → 加 0xFF 标记 → 不足 8 字节用 0x00 补齐
例如: b"t_114_r_1" (10 bytes) → 18 bytes (编码后)
```

### 根因
API v1 的 RocksDB 存储 key 是 `z + rawkey + ts`（不做 memcomparable 变换）。
`Key::from_raw()` 做了 memcomparable 变换。两路径产生的 key 完全不同。

### 修复
绕过 `Key::from_raw()`，直接拼 API v1 格式：
```rust
let mut key = Vec::with_capacity(1 + raw_key.len() + 8);
key.push(b'z');
key.extend_from_slice(raw_key);
key.extend_from_slice(&(!start_ts).to_be_bytes());
```

---

## 坑 3：Commit 写入缺少 `z` 前缀

### 现象
Prewrite 修复后仍不工作，consumer flush CF=lock（不是 CF=default）。

### 排查
Commit 产生 `CmdType::Put(cf="write")`，走现有 `sink_txn_put` 处理。但 `put.get_key()` 来自 Raft CmdBatch，**没有 `z` 前缀**——`z` 前缀是 apply 写 RocksDB 时才由 `data_key_with_buffer()` 添加的。

Scan 路径读 RocksDB 返回的 key **有 `z` 前缀**。两条路写入的 key 不一致。

### 根因
`data_key_with_buffer` (keys/src/lib.rs:213)：
```rust
buffer.extend_from_slice(DATA_PREFIX_KEY); // 加 'z'
buffer.extend_from_slice(key);
```
CDC delegate 拿到的 `put.get_key()` 是 apply 之前的原始 key，不带 `z`。

### 修复
`sink_txn_put` 和 `sink_raw_put` 的 PCR batcher 路径加 `z` 前缀：
```rust
let mut key_with_prefix = Vec::with_capacity(1 + put.get_key().len());
key_with_prefix.push(b'z');
key_with_prefix.extend_from_slice(put.get_key());
```

---

## 坑 4：LOCK CF 导致目标 TiKV FATAL

### 现象
目标 TiKV 启动后 FATAL：`"txn record found but not expected"`。

### 排查
Prewrite 写入 LOCK CF（锁信息）。PCR 将 LOCK CF 数据复制到目标 RocksDB。目标 TiDB 尝试解析锁信息时发现对应的事务不存在（这是源端的事务），触发 panic。

### 根因
LOCK CF 是**源端临时事务状态**。锁信息只对源集群有意义，复制到目标集群会产生孤儿锁。

### 修复
PCR batcher 跳过 LOCK CF：
```rust
if put.get_cf() != "lock" {
    batcher.add_kv(key, value, OpType::Put, cf);
}
```

---

## 坑 5（核心）：Observer 级别锁定在 LockRelated，DEFAULT CF 被拦截

### 现象
上面四个坑全修了，live CDC 仍然不工作。Delta scan 完美。Observer 日志显示 SCHEDULING 事件逐渐降为零（大部分 region 不再收到事件）。

### 排查
1. 研究 `CmdBatch` 的 `ObserveLevel`：`None < LockRelated < All`
2. `LockRelated` = 只投递 LOCK CF + WRITE CF，**不投递 DEFAULT CF**
3. CDC 需要 `All` 级别才能收到完整的 MVCC 数据
4. PCR 订阅时调用 `on_start_pcr_stream` → `capture_change` 升级 observer → `ObserveLevel::All`

**但关键代码有 bug**：

```rust
if is_new_delegate {  // ← 只有"新建"的 delegate 才执行
    capture_change(...);  // 升级到 ObserveLevel::All
}
```

5. TiKV 启动时 RTS（resolved-ts）已经为每个 region 注册了 observer，级别 `LockRelated`
6. PCR 来订阅时，delegate **已存在**，`is_new_delegate = false`，`capture_change` **永远不执行**
7. Observer 级别停留在 `LockRelated`，DEFAULT CF 被永远拦截

### 验证
- `capture_change acknowledged` 日志：每个 region 都有（因为每个 region 都被扫描过至少一次）
- `SCHEDULING` 日志逐渐减少到只有 region 34（扫描未完成的极少数 region）
- Delta scan 依然工作（它绕过 observer 直读 RocksDB）

### 根因
`on_start_pcr_stream` 中 `if is_new_delegate` 条件阻止了对已有 delegate 的 ObserveLevel 升级。

### 修复
去掉 `is_new_delegate` 条件，所有 region 无条件发送 `capture_change` 升级到 `All`。

---

## 最终效果

| DML 类型 | 修复前 | 修复后 |
|----------|--------|--------|
| INSERT | ✗ | ✓ (3-16s) |
| UPDATE | ✗ | ✓ (3-16s) |
| DELETE | ✗ | ✓ (5s) |
| TRUNCATE | ✓ (delta scan) | ✓ (live + delta) |
| DROP TABLE | ✗ | 待验证 |

## 涉及文件

| 文件 | 改动内容 |
|------|----------|
| `components/cdc/src/delegate.rs` | Prewrite handler、`z` 前缀、LOCK CF 过滤 |
| `components/cdc/src/observer.rs` | LockRelated 批次放行 |
| `components/cdc/src/endpoint.rs` | 去掉 `is_new_delegate` 条件，始终升级 ObserveLevel |
| `components/cdc/src/pcr_service.rs` | Bridge task 不因 CDC channel 关闭退出 |
| `components/stream-ingest/src/lib.rs` | Flush interval 200ms |
