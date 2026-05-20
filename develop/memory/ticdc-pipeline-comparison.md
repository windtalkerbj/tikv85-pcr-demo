---
name: TiCDC Pipeline 与 PCR Consumer Pipeline 对比
description: TiCDC 的 PULL→SORTER→MOUNTER→SINKER 能否复用于 PCR consumer 的分析
type: reference
originSessionId: ea6f15c4-edb6-4a68-8e5c-0de7678209d5
---
# TiCDC Pipeline vs PCR Consumer

## TiCDC Pipeline

```
PULL → SORTER → MOUNTER → SINKER
(KV流) (按commit_ts全局排序) (解码KV→Row变更) (写MySQL/Kafka)
```

- **SORTER 按时间戳排序**：把各 Region 乱序到达的 KV 事件重排成全局 commit_ts 顺序，保证下游 SQL 事务一致性
- **MOUNTER 解码**：将 Raw KV 与 Table Schema 关联，还原为行级变更
- **SINKER**：走 SQL 协议写入下游

## PCR Consumer Pipeline

```
STREAM → SORT → GENERATE → INGEST
(KV流)  (按Key排序)  (生成SST)   (Ingest到RocksDB)
```

## 关键差异

| | TiCDC SORTER | PCR 需要 |
|---|---|---|
| 排序维度 | 按 commit_ts（时间） | 按 Key（字节序） |
| 排序目的 | 还原事务顺序写 SQL | 满足 SST 文件格式要求 |
| 需要 MOUNTER | 必须（KV→Row 解码） | **不需要**（保持原始 MVCC KV） |
| 输出 | SQL INSERT/UPDATE/DELETE | SST 文件 → IngestExternalFile |

## 复用结论

**Pipeline 分阶段架构模式可复用**：分阶段流水线 + 阶段间 channel 通信 + 背压 + 内存配额管理

**每个 Stage 的实际逻辑需全新编写**：
1. SORT 阶段 — TiCDC 按时间排序，PCR 按 Key 排序（比较器不同）
2. MOUNTER — 完全省略（PCR 保持原始 MVCC 格式）
3. SINKER — TiCDC 写 SQL，PCR 通过 IngestExternalFile 写 SST 到 RocksDB

## 建议的 TiKV PCR Consumer Pipeline

```
CDC Stream → EventSort → SstFileWriter → IngestSST
 (gRPC)      (Key排序)   (RocksDB API)   (绕过Raft)
```

- 不需要全局 SORTER — 每个分区独立排序
- 不需要 MOUNTER — 原始 MVCC KV 直接写入 SST
- INGEST：复用 `sst_importer` 但新增本地直接 ingest 路径（当前 sst_importer 设计用于 Upload→Ingest 的外部文件流程）
