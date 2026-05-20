---
name: CockroachDB PCR 架构分析
description: PCR 生产者、消费者、线协议及核心设计原则的深入分析
type: reference
originSessionId: ea6f15c4-edb6-4a68-8e5c-0de7678209d5
---
# CockroachDB PCR 架构分析

## 核心设计原则

- **复制粒度**：MVCC key-value（字节级），非 SQL 行
- **源端捕获**：RangeFeed（KV 层 changefeed）按 Range 订阅，非 binlog
- **目标写入**：SST 直接 Ingest 到 Pebble，**绕过 SQL 和 Raft**
- **分区**：按 Key Range 分区，每个 Range 是独立流分区
- **一致性保证**：ResolvedTS checkpoint，支持断点续传
- **Key 映射**：消费端做 Tenant Key Rewriting
- **去重**：StreamSeq + MVCC 幂等写入

## Producer 端（`pkg/ccl/crosscluster/producer/`）

关键文件：
- `event_stream.go` — 核心：包装 RangeFeed，回调函数（onValue, onSSTable, onDeleteRange, onCheckpoint, onFrontier），通过 `crdb_internal.stream_partition()` 流式输出
- `stream_event_batcher.go` — 将 KV、SST、DelRange、SplitPoint、SpanConfig 攒批为 `StreamEvent_Batch`
- `replication_manager.go` — `StartReplicationStream` 入口，创建 producer job

数据流：RangeFeed per span → `onValue()`/`onSSTable()` 回调 → `streamEventBatcher.addKV/addSST` → 达到阈值 flush → protobuf 序列化 + snappy 压缩 → `streamCh` → pgwire cursor

## Consumer 端（`pkg/ccl/crosscluster/physical/`）

关键文件：
- `stream_ingestion_processor.go` — 主事件循环处理器（1200+ 行）
  - `Start()`：创建 SSTBatcher，订阅各分区，合并订阅流，启动 consumeEvents + flushLoop goroutine
  - `consumeEvents()`：从合并订阅通道读取事件，按类型分发
  - `handleEvent()`：KVEvent → buffer；SSTableEvent → ScanSST → buffer；CheckpointEvent → flush
  - `flushBuffer()`：按 Key 排序 KV，通过 SSTBatcher.AddMVCCKey + Flush 写入
- `streamclient/partitioned_stream_client.go` — 通过 pgx 连接源集群，订阅 `crdb_internal.stream_partition`，解析 `StreamEvent` protobuf

## SSTBatcher（`pkg/kv/bulk/sst_batcher.go`）

关键组件 — `MakeStreamSSTBatcher()` 创建 batcher，`ingestAll=true`，`disableScatters=true`：
1. `AddMVCCKey()` — 追加到内部 SST writer（Pebble 格式）
2. `Flush()` — 完成 SST，发送 `AddSSTableRequest` 到 KV 层
3. `addSSTable()` — 调用 Pebble 的 `IngestExternalFile`（直接写入 LSM，绕过 Raft）
4. 当 SST 超过 Range 大小时自动处理分裂

## Wire Protocol（`pkg/repstream/streampb/stream.proto`）

`StreamEvent` 消息：
- `Batch batch` — 包含：DeprecatedKeyValues, KVs, Ssts, DelRanges, SpanConfigs, SplitPoints
- `StreamCheckpoint checkpoint` — 每个 Span 的 ResolvedSpans + RangeStats
- `uint64 stream_seq` — 单调递增序列号，用于去重
- `int64 emit_unix_nanos` — 源端发送时间戳
