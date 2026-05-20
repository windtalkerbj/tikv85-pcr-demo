# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a side-by-side research/comparison repository containing two distributed database source trees:
- `cockroach24/` — CockroachDB v24.x, a distributed SQL database written in Go
- `tikv85/` — TiKV v8.5.x, a distributed transactional KV store written in Rust (CNCF graduated)

The primary research direction is **Physical Cluster Replication (PCR)** — understanding how CockroachDB implements byte-level cluster replication and assessing whether TiKV can be extended to support a similar capability.

## CockroachDB (`cockroach24/`)

### Build System
- **Build tool**: `./dev` (a Bazel wrapper, generated Go binary)
- **Module**: `github.com/cockroachdb/cockroach` — Go 1.22
- Useful commands:
  - `./dev build` — build the `cockroach` binary
  - `./dev build short` — faster build (race detector off)
  - `./dev test pkg/<path>...` — run tests for a package
  - `./dev test pkg/<path>... --test_filter=<TestName>` — run a single test
  - `./dev bench pkg/<path>...` — run benchmarks
  - `./dev generate go` — regenerate protobuf/stringer code
  - `./dev lint` — run linters

### Architecture (key to PCR)
- **KV layer** (`pkg/kv/`): Distributed transactional KV store, with `kv.DB` as the entry point for reads/writes
- **Storage engine** (`pkg/storage/`): Pebble (LSM-tree, RocksDB-derived) via the `storage.Engine` interface; provides MVCCKeyValue, SST ingestion
- **Raft** (`pkg/raft/`): Raft consensus implementation
- **Range splits** (`pkg/roachpb/`): Data organized into ranges (~512 MiB), each replicated via Raft
- **CCL (enterprise)** (`pkg/ccl/`): Enterprise features behind license, including PCR

### PCR Implementation (`pkg/ccl/crosscluster/physical/`)

PCR replicates KV byte-level data from a **source (producer)** cluster to a **destination (consumer)** cluster. Key architecture:

1. **Stream client** (`streamclient/`): Connects to the source cluster, subscribes to a replication stream partitioned by key ranges (partitioned_stream_client.go). Uses gRPC/pgwire for the subscription.

2. **Stream ingestion processor** (`stream_ingestion_processor.go`): The core consumer-side event loop. It:
   - Subscribes to multiple partitions via `streamclient.Client.Subscribe()`
   - Merges partitioned event streams via `MergedSubscription`
   - Receives events: `KVEvent` (point KVs), `SSTableEvent` (bulk SSTs), `DeleteRangeEvent`, `CheckpointEvent`, `SplitEvent`
   - Buffers KVs locally in a `streamIngestionBuffer`, sorts by key, then flushes via `bulk.SSTBatcher` (which generates SST files and ingests them into Pebble)
   - Tracks progress with a `span.Frontier` (resolved timestamps per span)

3. **SSTBatcher** (`pkg/kv/bulk/`): Batches MVCC key-values, sorts them, generates SST files via Pebble's SST writer, and ingests them directly into the storage engine (bypassing the Raft replication layer entirely on the consumer side).

4. **Key rekeying**: During ingestion, tenant keys are rewritten (`backupccl.KeyRewriter`) to map source tenant IDs to destination tenant IDs.

5. **Producer side** (`producer/`): Serves the replication stream — scans source ranges and emits KV/SST events at a consistent resolved timestamp.

6. **Protobuf definitions** (`pkg/repstream/streampb/`): Wire protocol for replication streams.

**Design insight**: PCR bypasses the SQL layer entirely on the consumer — it writes MVCC key-values directly into the storage engine via SST ingestion. This is what makes it "physical" (byte-level) rather than "logical" (SQL-level). The consumer does not run SQL transactions; it directly applies raw KV batches at the storage layer.

## TiKV (`tikv85/`)

### Build System
- **Rust toolchain**: pinned to nightly-2023-12-28 (`rust-toolchain.toml`)
- **Build tool**: `make` wrapping Cargo
- **Module**: workspace with 70+ crates under `components/`
- Useful commands (from `AGENTS.md`):
  - `make build` — development build
  - `make release` — optimized release build (thinLTO)
  - `make dev` — format + clippy + tests (PR gate)
  - `make test` — full test suite
  - `./scripts/test $TESTNAME -- --nocapture` — run a specific test
  - `make format` — rustfmt
  - `make clippy` — clippy with TiKV config

### Architecture (analogous to PCR)

- **raftstore** (`components/raftstore/`): Raft consensus store — handles Raft log replication, snapshotting, region splits/merges
- **raftstore-v2** (`components/raftstore-v2/`): Next-gen raftstore with improved batching and async apply
- **cdc** (`components/cdc/`): Change Data Capture — streams KV change events to downstream consumers (TiCDC). Uses a `ChangeLog` trait to track resolved timestamps.
- **backup-stream** (`components/backup-stream/`): Streaming backup — captures KV changes and uploads to external storage
- **backup** (`components/backup/`): BR (Backup & Restore) — bulk data movement via SST files
- **sst_importer** (`components/sst_importer/`): Ingests SST files into RocksDB
- **raft_log_engine** (`components/raft_log_engine/`): Raft log persistent storage
- **pd_client** (`components/pd_client/`): Client for Placement Driver (scheduling/coordination)
- **resolved_ts** (`components/resolved_ts/`): Resolved timestamp tracking for changefeeds
- **engine_traits** (`components/engine_traits/`): Abstract storage engine interface
- **engine_rocks** (`components/engine_rocks/`): RocksDB implementation

### Key differences from CockroachDB for PCR feasibility

1. **Storage engine**: TiKV uses RocksDB (C++ via rust-rocksdb bindings), CockroachDB uses Pebble (Go-native). Both are LSM-tree based and support SST ingestion.

2. **Raft integration**: In CockroachDB PCR, the consumer bypasses Raft — ingested SSTs are written directly to the storage engine. TiKV would need a similar "direct write" path that doesn't go through the Raft state machine.

3. **CDC infrastructure**: TiKV's `cdc` component already captures KV change events with resolved timestamps — analogous to CockroachDB's range feed. The `backup-stream` component is the closest equivalent to PCR's producer.

4. **SST ingestion**: TiKV has `sst_importer` for ingesting SST files, but it's designed for snapshot restore (BR), not continuous stream ingestion.

5. **Region-based vs Range-based**: Both use range/region-based sharding with Raft replication. TiKV's Region ~= CockroachDB's Range.

### PCR Implementation on TiKV (已实现原型代码)

基于 raftstore-v2 的 PCR 方案设计详见 `/Users/cjn/.claude/plans/witty-waddling-cupcake.md`。实现状态：**全部核心功能已编码，待 kvproto pcrpb 独立 PR 后可编译**。

#### 新建 crate：`components/stream-ingest/`（Consumer 端，17 文件）

```
components/stream-ingest/
├── Cargo.toml
├── build.rs                    ← protoc 检测编译脚本
├── src/
│   ├── lib.rs                  ← create_stream_ingest_task() 工厂
│   ├── config.rs               ← StreamIngestConfigManager（引用 tikv）
│   ├── sst_batcher.rs          ← KV 缓冲→排序→SST 生成→DirectIngest
│   ├── direct_ingest.rs        ← DirectIngestContext + Epoch 校验 + IngestLatch
│   ├── task.rs                 ← StreamIngestTask 主控 + Runnable + 事件循环
│   ├── subscriber.rs           ← gRPC 多分区订阅管理 + 自动重连
│   ├── checkpoint.rs           ← PD MetaStore checkpoint 持久化
│   ├── metrics.rs              ← 8 个 Prometheus 指标
│   ├── errors.rs               ← 9 种错误类型
│   └── pcrpb_gen/              ← PCR protobuf 生成代码（手动编写）
│       ├── mod.rs / pcrpb.rs / pcrpb_grpc.rs
└── tests/
    ├── unit_tests.rs           ← 48 tests（SstBatcher/Checkpoint/Dispatcher/MVCC编码）
    ├── pcrpb_tests.rs          ← 12 tests（PcrEvent 构建器/oneof/字段）
    └── integration_tests.rs    ← 集成测试桩（需 TiKV test harness）
```

#### CDC Producer 扩展（`components/cdc/src/`，5 文件）

| 文件 | 内容 |
|------|------|
| `pcr_event_batcher.rs` | PCR 事件批处理（22 tests），对标 CRDB `streamEventBatcher` |
| `pcr_service.rs` | PcrStream gRPC 服务端 + `SubscribeHandler` |
| `pcr_types.rs` | PCR 事件类型桩 |
| `delegate.rs` | +`sst_importer` + `pcr_batcher` + `pcr_event_sink` 字段，`sink_data()` 新增 `IngestSst`/`Delete` 分支，`sink_put` 双写 PCR |
| `endpoint.rs` | +`sst_importer` 字段，+`Task::StartPcrStream` 变体，+`set_sst_importer()` |

#### Proto 定义

- `proto/pcrpb/pcrpb.proto` — PcrStream gRPC 服务 + 6 种事件消息

#### 已修改的 TiKV 核心文件（6 个）

| 文件 | 改动 |
|------|------|
| `Cargo.toml`（根） | +workspace member + dependency |
| `components/server/Cargo.toml` | +`stream-ingest` dependency |
| `components/cdc/Cargo.toml` | +`sst_importer` dependency |
| `components/server/src/server2.rs` | +stream_ingest 字段/init/register，SstImporter 注入到 CDC，PcrService gRPC 注册 |
| `components/server/src/server.rs` | 同上（v1 模式） |
| `src/config/mod.rs` | +`StreamIngestConfig` 定义/Default/validate，+`Module::StreamIngest`，挂入全局 validate() |

#### 完整数据流

```
Source TiKV                          Target TiKV
Raft Apply → CdcObserver
  → Delegate::sink_data()
    ├─ CmdType::Put → sink_put() → PcrEventBatcher.add_kv()
    └─ CmdType::IngestSst → sink_ingest_sst() → SstImporter
  → emit_pcr_event() → gRPC PcrService.Subscribe()
    ═══════════════════════════════════→
                                        StreamSubscriber → StreamIngestTask
                                          → SstBatcher (sort+generate SST)
                                            → DirectIngestContext
                                              → ingest_external_file_cf (bypass Raft)
```

#### 配置

```toml
[stream-ingest]
enable = false
max-kv-buffer-size = "128MB"
min-flush-interval = "5s"
source-address = "source-pd:2379"
checkpoint-interval = "10s"
```

### TiKV's AGENTS.md

The `tikv85/AGENTS.md` (at project root) is the primary AI agent guide for that codebase. It covers build, testing, code style, security policies, and deployment in detail. Refer to it when working in the TiKV tree.
