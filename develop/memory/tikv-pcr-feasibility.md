---
name: TiKV PCR 可行性评估
description: TiKV 已有能力、关键缺失项、改造工作量估算
type: reference
originSessionId: ea6f15c4-edb6-4a68-8e5c-0de7678209d5
---
# TiKV PCR 可行性评估

## 已有对应能力

| CockroachDB PCR 组件 | TiKV 对应 | 状态 |
|---|---|---|
| RangeFeed 数据捕获 | `cdc` 组件 — `CdcObserver` 挂在 Raft apply 路径，捕获 KV 变更 + ResolvedTS | 已有 |
| 分区流式传输 | `cdc::channel` + `EventBatcher` + gRPC `EventFeed` | 已有 |
| SST 直接写入 | `sst_importer` — `IngestFile` API，但设计用于 BR 快照恢复，非流式 | 部分有 |
| Key 重映射 | BR restore 有 key rewrite 逻辑 | 部分有 |
| Checkpoint/断点续传 | `backup-stream` 的 `CheckpointManager` | 已有 |
| Raft 旁路写入 | **不存在** — TiKV 所有写入都经过 Raft | 缺失 |

## 关键缺失

### 1. Raft 旁路写入路径（风险最高）
- 需要在 raftstore-v2 中新增"直接 ingest"路径，允许 SST 文件不经 Raft 状态机直接写入 RocksDB
- 必须处理与正在进行的 Raft apply、compaction、snapshot 的并发安全
- CRDB 做法：Pebble 有原生 `IngestExternalFile` 支持；TiKV 的 Raft-RocksDB 集成更深

### 2. 流式 SST Batcher（中等）
- CRDB 的 `SSTBatcher`：KV 缓冲 → 排序 → SST 生成 → 发送 AddSSTable
- TiKV sst_importer 设计用于外部上传文件，非流式
- 需要新组件：接收 KV 事件流 → 内存排序 → 生成 SST → 本地 ingest

### 3. CDC Producer 扩展
- 全量初始扫描（CDC 已有 Initializer）
- 事件流中增加 SST 事件类型
- 批量事件格式

### 4. Wire Protocol 扩展
- 扩展 `kvproto/cdcpb`，增加：批量传输、SST 事件类型、Split 事件、SpanConfig 事件

### 5. Key 重映射
- 目标集群需要对源端 key 做映射

## 工作量估算

| 项目 | 复杂度 | 估计周数 |
|---|---|---|
| Raft 旁路写入 | 高 | 3-4 |
| 流式 SST Batcher | 中 | 2-3 |
| CDC Producer 扩展 | 中 | 1-2 |
| Wire Protocol 扩展 | 低 | 1 |
| Key 重映射 | 中 | 1-2 |
| Checkpoint/Cutover 控制 | 中 | 2-3 |
| 集成测试 + 稳定性 | 高 | 4+ |
| **总计** | — | **~14-18 周** |
