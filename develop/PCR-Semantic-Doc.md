# PCR Demo Semantic Document

版本：2026-05-19 | 状态：架构收敛

## 1. 复制模型

### 支持

- **Span-based subscription**：consumer 一条 gRPC stream 订阅全 key range `["", "")`
- **Source-side region resolution**：SpanBridge 通过 PD HTTP API 解析 region 拓扑
- **Full scan + Live CDC**：启动时全量扫描 RocksDB，之后通过 CDC observer 增量复制
- **SST ingest bypass Raft**：consumer 直接写 target RocksDB，不经过 Raft 状态机
- **At-least-once**：允许重复 replay，不允许丢失

### 不支持

- **Exactly-once**：不保证精确一次语义
- **Global ordering**：不保证跨 region 的全局写入顺序
- **Transaction-level atomicity**：单条 KV 突变独立复制，不保证事务边界

## 2. 数据正确性

### 支持

- **Committed MVCC state only**：PCR 只复制 WRITE CF 语义（committed write），不复制 LOCK CF intent
- **Region-affine ordering**：同一 region 的写入按 Raft apply 顺序到达 consumer
- **Split-aware delegate attach**：region split 后 child delegate 自动继承 PCR sink（PcrRegistry auto_match_pcr），零窗口
- **DELETE correctness**：source delegate 解析 WriteRef → 生成 DEFAULT CF tombstone（LogicalMutation），不依赖 target TSO
- **Rollback safety**：Rollback 不产生 phantom row（Lock short_value 路径已删除）

### 不支持

- **Snapshot isolation**：不保证全量扫描和 live CDC 之间的 MVCC 隔离
- **Large value live CDC (>255B)**：WriteRef short_value=None 时依赖全量扫描兜底，live CDC 不能独立复制大值
- **Prewrite before Commit**：Prewrite 阶段的 DEFAULT CF Put 会被复制（非 Lock 提取路径），但 visibility 由 WRITE CF gated

## 3. Split / Merge

### 支持

- **Split detection via PcrRegistry**：child delegate 出生时 auto_match_pcr，自动获得 shared sink
- **Parent delegate cleanup**：deregister 后旧 delegate 的 sender clone 随 Arc drop 自然释放
- **Split under load**：W级 DML 下 124 次 split 零数据丢失

### 不支持

- **Merge**：region merge 未测试，理论上 auto_match_pcr 覆盖
- **Split during ingest**：split 窗口内的 in-flight SST ingest 可能与新 region 边界冲突

## 4. DDL 复制

### 支持

- **DDL discover（event-driven polling）**：轮询 `mysql.tidb_ddl_history` 的 `MAX(job_id)`（1s 间隔），变化时触发 table cache diff
- **Schema sync bump**：通过 PD HTTP API 递增 `/tidb/ddl/global_schema_version`，触发 target TiDB schema reload
- **CREATE TABLE during PCR**：新表数据通过 live CDC 复制（key range 落在已有 region 内）
- **ADD/DROP INDEX**：索引 DDL 通过 DDL discover 检测并同步

### 不支持

- **瞬时 DDL lifecycle**：CREATE + DROP 在 DDL discover 窗口内的表不可见（snapshot-based 天生限制）
- **DDL event replay**：不做完整 DDL job parsing，不做 CREATE/DROP/TRUNCATE 精确分类
- **TRUNCATE data cleanup**：不为目标端旧 table_id 数据生成 DeleteRange（由目标 GC worker 自然处理）

## 5. 一致性保证

### 保证

| 保证项 | 机制 |
|--------|------|
| Region-local ordering | CDC observer per-region → delegate → shared sink → consumer worker (hash dispatch) |
| Split consistency | PcrRegistry auto_match_pcr，split 后 child delegate 立即获得 sink |
| DELETE consistency | WriteRef tombstone materialization（LogicalMutation::Delete） |
| Full scan completeness | SpanBridge resolve_regions → 每个 region 的 RocksDB snapshot scan |

### 不保证

| 不保证项 | 原因 |
|----------|------|
| Cross-region ordering | 不同 region 的 CDC events 在 relay 中可能交错 |
| MVCC snapshot isolation | 全量扫描和 live CDC 共享同一 consumer，无 resolved-ts boundary |
| Schema instant visibility | Target TiDB schema reload 依赖 PD TSO 自然推进 |
| Duplicate-free replay | At-least-once 模型，consumer 断连重连后可能 replay 已 ingest 的数据 |

## 6. 性能边界

| 场景 | 测量值 | 配置 |
|------|--------|------|
| INSERT 5000 | 33s (4 workers) | relay batch 64/1ms |
| DELETE 2000 | <3s | 同上 |
| TPCC 1仓 full scan | ~20min | 600K rows, 73 regions |
| Region split (W级) | 124 split, 零丢失 | PcrRegistry auto-match |

## 7. 已知限制

| # | 限制 | 影响 | 生产级解法 |
|---|------|------|-----------|
| 1 | Live CDC TSO gap | source commit_ts > target PD TSO 时数据不可见。根因：PCR 跨两个独立 TSO 域，target 无 resolved_ts 通知机制 | **Demo 不修**。缓解：read barrier（轮询 `@@tidb_current_ts` ≥ replicated_ts 再验证）。生产级需 target-side timestamp remapping，非 30 行 patch |
| 2 | Write guard 阻塞 TiDB 启动 ✅ | 已修复：TiKV 只拦截 request_source 非空的写入（用户 DML/DDL），放行 TiDB 内写（request_source 为空） | `src/storage/mod.rs` 1 行 |
| 3 | Consumer 单 runtime | 全量扫描和 live CDC 共享同一 event loop | Snapshot/live split pipeline |
| 4 | 大值 live CDC | WriteRef 无 short_value 时不复制 DEFAULT CF 值 | Commit 时从 RocksDB 读 |
| 5 | 瞬时 DDL 不可见 | CREATE+DROP 在窗口内丢失 | DDL job event replay |

## 8. 架构冻结范围

以下组件已稳定，不再做大改动：

- **PcrRegistry** + auto_match_pcr（endpoint.rs）
- **SpanBridge**（scan + writer + relay batch，~215 行）
- **LogicalMutation**（WriteRef → Put/Delete/Rollback）
- **Consumer parallel ingest**（4 workers, hash dispatch）
- **DDL discover**（MAX job_id + cache diff + schema bump）

以下组件允许优化但不允许结构变化：

- **Relay pipeline**（batch size tuning）
- **SstBatcher**（buffer size, sort optimization）
- **DirectIngest**（ingest batching）
