# PCR 前期研究文档

> 合并自 `memory/pcr-architecture.md`、`memory/tikv-pcr-feasibility.md`、`memory/ticdc-pipeline-comparison.md`

---

## 一、CockroachDB PCR 架构分析

### 核心设计原则

- **复制粒度**：MVCC key-value（字节级），非 SQL 行
- **源端捕获**：RangeFeed（KV 层 changefeed）按 Range 订阅，非 binlog
- **目标写入**：SST 直接 Ingest 到 Pebble，**绕过 SQL 和 Raft**
- **分区**：按 Key Range 分区，每个 Range 是独立流分区
- **一致性保证**：ResolvedTS checkpoint，支持断点续传
- **Key 映射**：消费端做 Tenant Key Rewriting
- **去重**：StreamSeq + MVCC 幂等写入

### Producer 端

| 文件 | 功能 |
|------|------|
| `event_stream.go` | 包装 RangeFeed，回调函数（onValue, onSSTable, onDeleteRange, onCheckpoint），通过 `crdb_internal.stream_partition()` 流式输出 |
| `stream_event_batcher.go` | KV、SST、DelRange、SplitPoint 攒批为 `StreamEvent_Batch` |
| `replication_manager.go` | `StartReplicationStream` 入口，创建 producer job |

### Consumer 端

| 文件 | 功能 |
|------|------|
| `stream_ingestion_processor.go` | 主事件循环（1200+行），consumeEvents + flushLoop |
| `partitioned_stream_client.go` | 通过 pgx 订阅源集群，解析 StreamEvent protobuf |
| `sst_batcher.go` | `MakeStreamSSTBatcher()` → AddMVCCKey → Flush → IngestExternalFile（绕过 Raft） |

### Wire Protocol

```
StreamEvent:
  - Batch: KVs, Ssts, DelRanges, SpanConfigs, SplitPoints
  - StreamCheckpoint: ResolvedSpans + RangeStats
  - stream_seq: 去重
  - emit_unix_nanos: 源端时间戳
```

---

## 二、TiKV PCR 可行性评估

### 已有能力

| TiKV 组件 | 对应 CRDB 组件 | 成熟度 |
|-----------|---------------|--------|
| `cdc` | RangeFeed | 成熟 |
| `backup-stream` | StreamProducer | 部分 |
| `sst_importer` | SSTBatcher.Ingest | 成熟（BR用） |
| `raftstore-v2` + `TabletRegistry` | 直接写 Pebble Tablet | 新 |
| `resolved_ts` | ResolvedTS | 成熟 |

### 关键缺失项（评估时）

| 缺失 | 预估工期 |
|------|----------|
| stream-ingest crate (SstBatcher + DirectIngest) | 4-6 周 |
| PCR gRPC 协议 + PcrService | 2-3 周 |
| CDC IngestSst 支持 | 1-2 周 |
| Checkpoint 持久化 + 断点续传 | 2-3 周 |
| **总计** | **14-18 周** |

### TiKV vs CRDB 关键差异

| 维度 | CRDB | TiKV |
|------|------|------|
| 存储引擎 | Pebble（Go原生） | RocksDB（C++ via rust-rocksdb）|
| SST Ingest | `Pebble.IngestExternalFile` | `rocksdb.ingest_external_file_cf` |
| Raft 绕过 | 直接写 Pebble（Consumer 无 Raft） | 需要 DirectIngest 路径 |
| CDC | RangeFeed 内置 | `cdc` 组件独立 |
| Region/Range | ~512 MiB Range | ~96 MiB Region 默认 |
| 编程语言 | Go | Rust |

---

## 三、TiCDC Pipeline 能否复用于 PCR Consumer？

### 结论：不能直接复用，但架构可参考

TiCDC 使用 **PULL → SORTER → MOUNTER → SINKER** 四阶段 pipeline：

```
TiKV CDC → Puller → Sorter → Mounter → Sinker → Downstream (MySQL/TiDB/Kafka)
```

| 阶段 | 能否复用于 PCR | 原因 |
|------|---------------|------|
| Puller | 部分 | gRPC 订阅逻辑可参考，但需要对接 PCR 的 PcrStream 协议 |
| Sorter | 否 | 按 commit_ts 排序，PCR 不需要 |
| Mounter | 否 | 解码 SQL row，PCR 是字节级不需要 |
| Sinker | 否 | 写 SQL/TiDB，PCR 写 RocksDB |

**PCR 需要的是**：Puller（参考）+ SSTBatcher（自研）+ DirectIngest（自研），而非 TiCDC 的完整 pipeline。

---

## 四、最终决策

采用 **raftstore-v2 为基础，CDC 扩展为 Producer，新建 stream-ingest crate 为 Consumer** 的架构。详细设计见 `witty-waddling-cupcake.md`。
