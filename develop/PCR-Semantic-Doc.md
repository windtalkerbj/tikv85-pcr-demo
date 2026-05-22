# PCR Demo Semantic Document

版本：2026-05-22 | 状态：功能完备

## 1. 复制模型

### 支持

- **Span-based subscription**：consumer 一条 gRPC stream 订阅全 key range `["", "")`
- **Source-side region resolution**：SpanBridge 通过 PD HTTP API 解析 region 拓扑
- **Full scan + Live CDC**：启动时全量扫描 RocksDB，之后通过 CDC observer 增量复制
- **Streaming scan O(1MB)**：RocksDB iterator 直接流式构建 1MB PcrKvBatch，避免 O(region_size) 内存
- **SST ingest bypass Raft**：consumer 直接写 target RocksDB，不经过 Raft 状态机
- **At-least-once**：允许重复 replay，不允许丢失
- **TSO gap 实时可见**：consumer 每 5s 通过 PD gRPC `batch_get_tso(100K)` 推进目标 PD 时钟，DML 数据 5-10s 内目标端可见

### 不支持

- **Exactly-once**：不保证精确一次语义
- **Global ordering**：不保证跨 region 的全局写入顺序
- **Transaction-level atomicity**：单条 KV 突变独立复制，不保证事务边界
- **Snapshot isolation**：不保证全量扫描和 live CDC 之间的 MVCC 隔离
- **Merge**：region merge 未测试

## 2. 数据正确性

### 支持

- **Committed MVCC state only**：PCR 只复制 WRITE CF 语义，DEFAULT CF 不直接复制（防 phantom row）
- **Large value CDC (>255B)**：`short_value=None` 时通过 `old_value_cb` 回查 DEFAULT CF at start_ts ✅
- **Region-affine ordering**：同一 region 的写入按 Raft apply 顺序到达 consumer
- **Split-aware delegate attach**：PcrRegistry auto_match_pcr，child delegate 零窗口获得 PCR sink
- **DELETE correctness**：LogicalMutation::Delete 生成 DEFAULT CF tombstone
- **Rollback safety**：Rollback 不产生 phantom row

### 不支持

- **Prewrite before Commit**：Prewrite 阶段的 DEFAULT CF Put 不会被复制
- **瞬时 DDL lifecycle**：CREATE + DROP 在 DDL discover 窗口内的表不可见

## 3. Split / Merge

### 支持

- Split detection via PcrRegistry auto_match_pcr ✅
- Region rediscovery：SpanBridge 每 30s 重解析，CREATE TABLE 产生的新 region 自动发现并全量扫 ✅
- Split under load 验证：124 split 零数据丢失

### 不支持

- Merge：未测试

## 4. DDL 复制

### 支持

- DDL discover（event-driven polling, 1s 间隔）
- Schema sync bump（目标 PD HTTP API）
- CREATE TABLE during PCR ✅（SpanBridge 30s region rediscovery）
- ADD/DROP INDEX ✅（DDL discover 检测并同步）

### 不支持

- TRUNCATE 物理数据清理（架构边界——不走 Raft 路径，Demo 接受旧数据残留）

## 5. 生命周期控制

### 支持

- Create → Subscribing ✅
- Pause → Resume（checkpoint 断点续传）✅
- Cutover（latest 立即完成）✅
- Activate（释放写保护）✅
- pcr-ctl CLI 全命令支持 ✅

## 6. 可观测性

### 支持

- `pcr-ctl status`：Standby Read 状态 + lag + 人类可读复制时间 + per-region frontier ✅
- `pcr-ctl standby-status`：一行输出，脚本轮询用 ✅
- `pcr-ctl list`：实时 API 驱动的任务列表 ✅
- `/pcr/status` HTTP API：lag_seconds + frontier + 完整状态 ✅

## 7. 性能边界

| 场景 | 测量值 |
|------|--------|
| TPCC 1仓 full scan | ~10s (streaming, 4 workers) |
| TPCC 5仓 full scan | ~25s |
| INSERT 10,000 | ~5s live CDC 收敛 |
| UPDATE 1,000 | ~5s live CDC 收敛 |
| DELETE 10,000 | ~10s live CDC 收敛 |

## 8. 架构冻结范围

以下组件已稳定，不再做大改动：

- **PcrRegistry** + auto_match_pcr
- **SpanBridge**（scan + 30s rediscovery + checkpoint ticker + relay）
- **LogicalMutation**（WriteRef → Put/Delete/Rollback + old_value_cb fallback）
- **Consumer parallel ingest**（4 workers + round-robin dispatch）
- **DDL discover**（MAX job_id + cache diff + schema bump）
- **TSO bumper**（batch_get_tso 100K per 5s）

## 9. 已知限制

| # | 限制 | 影响 | 状态 |
|---|------|------|:--:|
| 1 | TSO gap | 跨独立 TSO 域复制，通过 batch_get_tso bumper 缓解 | ✅ |
| 2 | Write guard 阻塞 TiDB 启动 | 已修复 | ✅ |
| 3 | TRUNCATE 物理清理 | 架构边界，Demo 不修 | ❌ |
| 4 | Relay force-flush 依赖性 | 条件 flush 下部分 delegate 不产出事件，根因未定 | ⚠️ |
| 5 | pause/resume 在 span 模式下 checkpoint 需 checkpoint ticker 支持 | 已修复 | ✅ |
