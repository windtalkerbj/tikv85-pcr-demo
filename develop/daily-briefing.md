# PCR 项目每日工作简报

## 概述

**项目**：TiKV Physical Cluster Replication（PCR）—— TiKV 字节级集群复制  
**目标**：参照 CockroachDB PCR 架构，在 TiKV 上实现物理集群复制能力  
**周期**：2026-04-26 ~ 2026-05-07  

---

## Phase 1：可行性研究（04-26 ~ 04-28）

**阅读 CockroachDB PCR 源码**，理解其架构：
- Producer（源端）：range feed 流式推送 KV 事件
- Consumer（目标端）：StreamIngestionProcessor 接收事件 → SSTBatcher 批量写 SST → 直接 Ingest 到 Pebble（绕过 Raft）
- 核心洞察：PCR 在 Consumer 端绕过 SQL 层和 Raft 层，直接写存储引擎

**TiKV PCR 可行性评估**：
- TiKV CDC 组件已有 KV change event 捕获能力（类似 CRDB range feed）
- `sst_importer` 已有 SST 导入能力（但为 BR 设计，非流式）
- raftstore-v2 的 TabletRegistry 可支持 per-Region 直接写入
- 关键缺失：Consumer 端流式 SST 生成 + Direct Ingest 路径
- 预估工作量：14-18 周

**产出**：
- `pcr-architecture.md`：CRDB PCR 架构深入分析
- `tikv-pcr-feasibility.md`：TiKV PCR 可行性评估
- `ticdc-pipeline-comparison.md`：TiCDC Pipeline 能否复用于 PCR

---

## Phase 2：原型开发（04-29 ~ 05-01）

**新建 `components/stream-ingest/` crate**（17 文件）：
- SstBatcher：KV 缓冲 → 排序 → SST 生成 → DirectIngest
- DirectIngest：Epoch 校验 + IngestLatch + ingest_external_file_cf（绕过 Raft）
- StreamSubscriber：gRPC 多分区订阅 + 自动重连
- StreamIngestTask：tokio 事件循环，dispatch KV/SST/Checkpoint/Split 事件
- CheckpointManager：PD MetaStore 持久化

**CDC Producer 扩展**：
- `pcr_event_batcher.rs`：PCR 事件批处理（22 tests）
- `pcr_service.rs`：PcrStream gRPC 服务端
- `delegate.rs`：新增 IngestSst 处理分支

**Proto 定义**：`pcrpb.proto` — PcrStream gRPC 服务 + 6 种事件消息

**Server 集成**：
- `server2.rs`：stream-ingest 初始化 + PcrService gRPC 注册
- SstImporter 注入 CDC Endpoint

**E2E 验证**（04-29）：
- gRPC 通道建立，PCR 基本流程跑通
- 发现 CDC observer max_level < All 导致不推送事件（修复：ObserveHandle::fresh_handle）

---

## Phase 3：Snapshot 方案 + 断点续传（05-01 ~ 05-03）

**Snapshot 方案迁移**（05-01）：
- 从 RocksDB Secondary Instance 改为 RocksSnapshot 直接读 engine
- 零文件开销，即时一致性
- 新增 `pcr_snapshot.rs`：scan_default_cf + scan_write_cf_since
- 遇到 SST 排序错误（dedup_by 修复）、MVCC write CF 解码问题（WriteRef::parse）
- 新增 `pcr_metrics.rs`（Producer 独立指标）

**断点续传（Checkpoint & Resume）需求明确**（05-01）：
- Pause → 源端持续写入 → Resume 只需同步增量
- 初始方案：pause 后 CDC observer 继续推送 → 数据丢失（max_level 限制）
- 改为：resume 时扫描 Write CF，commit_ts > checkpoint_ts 过滤增量
- 问题：66 个 Region 各扫一遍全库 → Bridge/Ingested = 14:1 冗余

---

## Phase 4：Bounded Scan 攻关（05-04 ~ 05-06）

**核心问题**：每个 Region 只扫自己 key range，避免 66x 冗余。

**8 套方案演进**：

| # | 方案 | 结果 |
|---|------|------|
| 1 | seek(raw PD key) | iterator invalid |
| 2 | encode + MAX_TS/zero_TS seek | iterator invalid |
| 3 | IterOptions lower/upper bound | upper_bound 过滤全部 key |
| 4 | 参照 TiCDC EncodeBytes 格式 | 仍有双编码 |
| 5 | 去掉双编码：PD key + MAX_TS | lower_bound work |
| 6 | 去掉 code-level to-check（实验） | 66/66 Region 找到数据 |
| 7 | RocksDB upper_bound 不可用 → 代码级 to 检查 | 最终方案 |

**关键发现**：
1. **双编码问题**：PD key 已经是 memcomparable 编码，调用 `Key::from_raw()` 二次编码导致 seek target 错位
2. **RocksDB upper_bound 不兼容**：写 CF custom comparator 不接受 `iterate_upper_bound`
3. **最终方案**：`Key::from_encoded_slice(pd_key).append_ts(TimeStamp::max())` + lower_bound + 代码级 `user_key >= to`

**create/resume 生命周期重构**：
- `create` → 全量扫描（start_ts=0）
- `resume` → 增量扫描（从 checkpoint ts）
- `start` → deprecated，委托 create
- 修改文件：http_control.rs, task.rs（新增 CreateReplication 变体, force_full_scan 参数）

---

## Phase 5：验证 + 文档（05-06 ~ 05-07）

**多 Region delta scan 验证**：
- 3 个 bounded Region 各自找到独立增量数据
- 去掉 to-check 验证：66/66 Region 全部找到数据
- 确认 bounded scan 逻辑完全正确

**bug 修复**：
- pcr-ctl `#[arg(help)]` → `#[command(about)]` — 编译通过
- `scan_default_cf` 删除冗余 upper_bound
- 配置文件移至 `develop/pcr-configs/`，防误删

**文档产出**：
- `pcr_checkpoint_resume_devlog.md`：断点续传开发历程（技术版）
- `pcr_checkpoint_resume.pptx`：面向售前/售后的 BT 断点续传类比演示
- `pcr-unimplemented.md`：未实现功能清单
- 每日状态快照：memory/pcr-status-*.md

---

## 关键指标

| 指标 | 值 |
|------|-----|
| 全量同步 | 50K 行 → 17K KVs |
| 增量同步 | 10K 行 → 10K KVs |
| Bridge/Ingested | 14:1 → ~2:1 |
| SST Errors | 0（全程） |
| 方案迭代 | 8 套 |
| 开发周期 | ~10 天 |

## 未完成项

- PcrSstChunk / PcrDeleteRange（Lightning/DROP TABLE 支持）
- Cutover / Activate 端到端测试
- 多节点集群测试
- 数据一致性校验
- RocksDB upper_bound 兼容性修复（写 CF comparator）

---

## 技术收获

1. **TiKV key 三层模型**：raw → encoded → MVCC，层间不可混淆
2. **写 CF comparator 局限性**：支持 lower_bound，不支持 upper_bound
3. **PD API 返回 encoded key**：非 raw key
4. **编码 vs 双编码**：RocksDB comparator 需要完整 key，prefix seek 行为与预期不符
5. **部分重启灾难**：TiKV/TiDB/PD 必须同步重启，否则 store 地址冲突、cluster ID 不匹配
