# PCR Live CDC 路径分析：为什么 INSERT/UPDATE/DELETE 无法实时复制

## 一、TiDB 事务实现

TiDB 使用 Percolator 2PC 模型，一次 SQL INSERT 分两阶段：

### Prewrite 阶段
TiDB → TiKV Prewrite RPC → Raft propose → Raft commit → Raft apply
- DEFAULT CF: 写入 `z{raw_key}{!start_ts}` = 实际数据
- LOCK CF: 写入 `z{raw_key}` = 锁信息（start_ts, primary_key, ttl）

### Commit 阶段
TiDB → TiKV Commit RPC → **绕过数据 region 的 Raft**
- 协调者直接写 WRITE CF: `z{raw_key}{!commit_ts}` = {write_type, start_ts}
- 清理 LOCK CF: 删除锁

### 关键点
Commit 的 WRITE CF 写入**不经过数据 region 的 Raft 状态机**。协调者通过 storage 层直接写 RocksDB。

### MVCC 可见性
TiDB SELECT 时：
1. 读 WRITE CF 找 `{key}` 最新 `commit_ts <= read_ts` 的 entry
2. 取出 `{write_type, start_ts}`
3. 读 DEFAULT CF: `{key}{!start_ts}` 获取实际数据
4. 如果 WRITE CF 没有对应 commit record → 视为未提交 → 不可见

---

## 二、当前 PCR 设计

### 数据流
```
Source TiKV                          Target TiKV
Raft Apply → CdcObserver
  → delegate.on_batch()
    → sink_data() → CmdType::Put  → sink_put() → PCR batcher
                  → CmdType::Prewrite → 手动提取 key/value → PCR batcher
                  → CmdType::Prewrite/Commit/... → _ => skip
  → batcher.flush() → PcrEvent (KvBatch)
  → emit_pcr_event() → gRPC PcrStream.Subscribe()
    ═══════════════════════════════════→
                                        StreamSubscriber → StreamIngestTask
                                          → handle_event() → SstBatcher.add_kv()
                                          → batcher.flush() → SST → DirectIngest
                                          → RocksDB (bypass Raft)
```

### 两条数据路径

| 路径 | 机制 | 数据来源 | 写入 |
|------|------|----------|------|
| 全量扫描 | `scan_default_cf` + `scan_write_cf_raw` | 直读 RocksDB | DEFAULT CF + WRITE CF |
| Delta 扫描 | `scan_delta_entries` | 直读 RocksDB（since checkpoint_ts） | DEFAULT CF + WRITE CF |
| Live CDC | `delegate.on_batch()` → `sink_data()` | Raft CmdBatch | ? |

### 目标 TiDB schema 同步
B 方案：`loadSchemaTickerOnly()` — 定时从 RocksDB 读 schema meta KV，更新 `information_schema`。

---

## 三、技术难题

**Live CDC 路径的 INSERT/UPDATE/DELETE 数据到达目标 RocksDB，但 TiDB SELECT 不可见。**

### 排查过程

1. **CmdType 漏处理**：`sink_data` 只匹配 `CmdType::Put`。TiDB 事务走 `CmdType::Prewrite`，被 `_ => debug!("skip")` 跳过。→ 加 Prewrite 分支 ✓（已验证 INSERT 数据到达 consumer）

2. **Observer 级别过滤**：Prewrite 批次可能是 `LockRelated` 级别（非 `All`），observer 过滤掉。→ 放行 LockRelated（后证明所有批次都是 All，此修复非必需）

3. **Key 编码不一致**：Prewrite handler 使用 `Key::from_raw(raw_key).append_ts(start_ts)`，调用 `encode_bytes()` 做 memcomparable 编码（8字节分组 + 0xFF标记 + 补齐），而 RocksDB API v1 格式为 `z + raw_bytes + ts`。→ 改为直接拼 RocksDB 格式 ✓

4. **Flush 延迟**：factory 函数硬编码 `min_flush_interval = 5s`（config 中 200ms 被覆盖）。→ 修正 ✓

5. **双线程 batcher**：tokio runtime 2 worker threads，不确定是否影响。→ 改为 1 线程（A/B 验证排除线程因素）

### 真正根因

**`CmdType::Commit` 在 TiKV raftstore 中不存在。**

TiDB 2PC 的 Commit 阶段**不经过 Raft 状态机**（见第一节）。coordinator 通过 storage 层直接写 WRITE CF。CDC observer 只能捕获 Raft apply 产生的 CmdBatch——Commit 不产生 Raft 日志，因此 CmdBatch 中**没有 Commit 条目**。

```
Prewrite: Raft → CmdType::Prewrite → handler → DEFAULT CF ✓
Commit:   绕过 Raft → 无 CmdBatch        → WRITE CF ✗
```

**结果**：Live CDC 只复制了 DEFAULT CF 数据，缺少 WRITE CF commit record。TiDB MVCC 读找不到 commit record → 数据视为未提交 → 不可见。

### 为什么 Delta Scan 能工作

Delta scan（`scan_delta_entries`）直读 RocksDB 的 WRITE CF + DEFAULT CF，通过 RocksDB 迭代器扫描：
- 扫描 WRITE CF，筛选 `commit_ts > since_ts && WriteType::Put`
- 对每个 WRITE CF entry，用 `start_ts` 回读 DEFAULT CF 获取值
- 同时扫描 DEFAULT CF 中的非短值条目

因为 Commit 已写入 RocksDB（虽然不经过 Raft），delta scan 的 RocksDB 迭代器能看到完整的 MVCC 数据。

---

## 四、结论

| 路径 | DEFAULT CF | WRITE CF | TiDB 可见 |
|------|-----------|----------|-----------|
| 全量扫描 | ✓ | ✓ | ✓ |
| Delta 扫描 | ✓ | ✓ | ✓ |
| Live CDC | ✓（Prewrite handler） | ✗（Commit 不走 Raft） | ✗ |

Live CDC 路径无法复制 2PC Commit 的 WRITE CF 写入，这是**TiKV 架构限制**，不是 PCR 实现 bug。

### 方向

实现定时后台 delta scan（类 CRDB rangefeed periodic snapshot），自动定期直读 RocksDB 捕获增量写入，取代 live CDC 路径。
