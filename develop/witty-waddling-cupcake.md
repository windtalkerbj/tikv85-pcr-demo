# TiKV PCR 详细设计方案

## Context

基于已批准的总体架构方案（raftstore-v2 为基础，CDC 扩展为 Producer，新增 `stream-ingest` crate 为 Consumer），进行详细级设计——定义具体的 Rust trait/struct、protobuf 消息、gRPC 服务、代码修改点。

---

## 一、Proto 定义（`kvproto/pcrpb/pcrpb.proto`）

### 1.1 事件消息

```protobuf
syntax = "proto3";
package pcrpb;
option go_package = "github.com/pingcap/kvproto/pkg/pcrpb";

import "metapb.proto";
import "gogoproto/gogo.proto";

// PcrStream 是 PCR 复制流的 gRPC 服务
service PcrStream {
    // Subscribe 从源集群订阅 PCR 事件流
    rpc Subscribe(PcrSubscribeRequest) returns (stream PcrEvent) {}
}

message PcrSubscribeRequest {
    uint64 stream_id = 1;
    uint64 start_ts  = 2;  // 从此 TS 开始订阅（断点续传）
    PcrPartitionSpec partition = 3;
}

message PcrPartitionSpec {
    uint64 region_id        = 1;
    bytes  start_key        = 2;
    bytes  end_key          = 3;
    uint64 region_epoch_conf_ver = 4;
    uint64 region_epoch_version  = 5;
}

// ---- 事件类型 ----

message PcrEvent {
    uint64 stream_seq = 1;  // 单调递增，Consumer 去重用

    oneof event {
        PcrKvBatch     kv_batch      = 2;
        PcrSstChunk    sst_chunk     = 3;
        PcrCheckpoint  checkpoint    = 4;
        PcrDeleteRange delete_range  = 5;
        PcrSplit       split         = 6;
    }
}

message PcrKvBatch {
    repeated PcrKV kvs = 1;   // 批量 MVCC KV
}

message PcrKV {
    bytes key     = 1;   // MVCC 编码后的 key（含时间戳）
    bytes value   = 2;   // MVCC 编码后的 value
    OpType op     = 3;   // Put / Delete
}

enum OpType {
    PUT    = 0;
    DELETE = 1;
}

message PcrSstChunk {
    bytes data      = 1;   // 完整 SST 文件内容
    bytes start_key = 2;
    bytes end_key   = 3;
    uint64 write_ts = 4;   // SST 中所有 KV 的统一 MVCC 时间戳
}

message PcrCheckpoint {
    uint64 resolved_ts          = 1;  // 此 TS 之前的所有数据已发出
    repeated uint64 region_ids  = 2;  // checkpoint 覆盖的 Region
}

message PcrDeleteRange {
    bytes  start_key = 1;
    bytes  end_key   = 2;
    uint64 ts        = 3;
}

message PcrSplit {
    bytes split_key = 1;
}
```

---

## 二、新建 `components/stream-ingest/` crate

### 2.1 Cargo.toml

```toml
[package]
name = "stream-ingest"
version = "0.0.1"
edition = "2021"
license = "Apache-2.0"

[dependencies]
engine_traits = { workspace = true }
engine_rocks = { workspace = true }
kvproto = { workspace = true }
tikv = { workspace = true }
tikv_util = { workspace = true }
tikv_kv = { workspace = true }
raftstore = { workspace = true }
raftstore-v2 = { workspace = true }
sst_importer = { workspace = true }
grpcio = { workspace = true }
pd_client = { workspace = true }
collections = { workspace = true }
online_config = { workspace = true }
protobuf = { version = "2.8", features = ["bytes"] }
futures = "0.3"
tokio = { version = "1", features = ["sync", "time", "rt"] }
slog = { workspace = true }
fail = { workspace = true }

[features]
default = ["test-engine-kv-rocksdb", "test-engine-raft-raft-engine"]
test-engine-kv-rocksdb = ["tikv/test-engine-kv-rocksdb"]
test-engine-raft-raft-engine = ["tikv/test-engine-raft-raft-engine"]
failpoints = ["tikv/failpoints"]
```

### 2.2 `lib.rs` — 公共接口

```rust
// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

mod config;
mod checkpoint;
mod direct_ingest;
mod metrics;
mod sst_batcher;
mod subscriber;
mod task;

pub use config::{StreamIngestConfig, StreamIngestConfigManager};
pub use metrics::STREAM_INGEST_METRICS;
pub use task::{Task, StreamIngestTask};

use engine_traits::KvEngine;
use tikv_util::worker::Runnable;

/// 创建 StreamIngestTask 的工厂函数
pub fn create_stream_ingest_task<E: KvEngine>(
    cfg: StreamIngestConfig,
    tablet_registry: TabletRegistry<E>,
    pd_client: Arc<dyn PdClient>,
) -> StreamIngestTask<E> {
    StreamIngestTask::new(cfg, tablet_registry, pd_client)
}
```

---

## 三、Consumer 端核心组件详细设计

### 3.1 `SstBatcher` — `src/sst_batcher.rs`

```rust
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use engine_traits::{KvEngine, SstWriter, SstWriterBuilder, SstExt};
use tikv_kv::MvccKeyValue;  // 假设扩展 engine_traits 或本地定义

/// MvccKeyValue 本地定义（如 engine_traits 未提供）
#[derive(Clone)]
pub struct MvccKeyValue {
    pub key: Vec<u8>,     // MVCC 编码 key（含时间戳后缀）
    pub value: Vec<u8>,   // MVCC 编码 value
}

/// 触发 flush 的条件
#[derive(Debug, PartialEq)]
enum FlushReason {
    KvBufferFull,       // KV buffer >= max_kv_buffer_size
    RangeKeyBufFull,     // RangeKey buffer >= max_rk_buffer_size
    CheckpointArrived,   // 收到 Checkpoint 事件
    RangeBoundary,       // 跨越 Region 边界
}

/// 流式 SST 批量写入器
pub struct SstBatcher<E: KvEngine> {
    // KV 缓冲
    kv_buffer: Vec<MvccKeyValue>,
    kv_buffer_size: usize,
    max_kv_buffer_size: usize,      // 默认 128MB

    // RangeKey 缓冲（范围删除）
    range_key_buffer: Vec<(Vec<u8>, Vec<u8>, u64)>,  // (start, end, ts)
    range_key_buffer_size: usize,
    max_rk_buffer_size: usize,      // 默认 32MB

    // SST 写入器（延迟创建）
    sst_writer: Option<E::SstWriter>,

    // 当前写入的目标范围
    current_start_key: Vec<u8>,
    current_end_key: Vec<u8>,

    // Tablet 注册表（用于 flush 时查找目标 Region）
    tablet_registry: Arc<TabletRegistry<E>>,

    // 统计
    total_kvs_ingested: u64,
    total_bytes_ingested: u64,
    total_flushes: u64,
}

impl<E: KvEngine> SstBatcher<E> {
    pub fn new(
        tablet_registry: Arc<TabletRegistry<E>>,
        max_kv_buffer_size: usize,
        max_rk_buffer_size: usize,
    ) -> Self {
        Self {
            kv_buffer: Vec::with_capacity(1024),
            kv_buffer_size: 0,
            max_kv_buffer_size,
            range_key_buffer: Vec::new(),
            range_key_buffer_size: 0,
            max_rk_buffer_size,
            sst_writer: None,
            current_start_key: Vec::new(),
            current_end_key: Vec::new(),
            tablet_registry,
            total_kvs_ingested: 0,
            total_bytes_ingested: 0,
            total_flushes: 0,
        }
    }

    /// 添加单个 MVCC KV 到缓冲
    pub fn add_kv(&mut self, kv: MvccKeyValue) -> Result<FlushReason> {
        self.kv_buffer_size += kv.key.len() + kv.value.len();
        self.kv_buffer.push(kv);

        if self.kv_buffer_size >= self.max_kv_buffer_size {
            Ok(FlushReason::KvBufferFull)
        } else {
            Ok(FlushReason::KvBufferFull) // 调用方检查
        }
    }

    fn do_flush(&mut self) -> Result<Vec<(u64, u64, usize)>> {
        // 1. 按 Key 排序 KV
        self.kv_buffer.sort_by(|a, b| a.key.cmp(&b.key));

        // 2. 查找该 key range 对应的 Tablet
        let region_id = self.lookup_region(&self.current_start_key)?;
        let tablet = self.tablet_registry
            .get(region_id)
            .ok_or_else(|| Error::TabletNotFound(region_id))?;

        // 3. 创建 SST writer
        let mut writer = E::SstWriterBuilder::new()
            .set_db(tablet.latest().unwrap())
            .set_cf("default")
            .set_in_memory(true)
            .build(&self.temp_sst_path(region_id))?;

        // 4. 写入所有 KV
        for kv in &self.kv_buffer {
            writer.put(&kv.key, &kv.value)?;
        }

        // 5. 完成 SST，获取数据
        let (info, reader) = writer.finish_read()?;
        let mut sst_data = Vec::new();
        reader.read_to_end(&mut sst_data)?;

        // 6. 调用 DirectIngest
        let result = direct_ingest::ingest_sst(
            &self.tablet_registry,
            region_id,
            &sst_data,
            &self.current_start_key,
            &self.current_end_key,
        )?;

        // 7. 重置缓冲
        self.reset_buffers();

        Ok(vec![result])
    }

    /// 提交当前缓冲（供外部调用）
    pub fn flush(&mut self) -> Result<Vec<(u64, u64, usize)>> {
        if self.kv_buffer.is_empty() && self.range_key_buffer.is_empty() {
            return Ok(vec![]);
        }
        self.total_flushes += 1;
        self.total_kvs_ingested += self.kv_buffer.len() as u64;
        self.do_flush()
    }

    fn reset_buffers(&mut self) {
        self.kv_buffer.clear();
        self.kv_buffer_size = 0;
        self.range_key_buffer.clear();
        self.range_key_buffer_size = 0;
    }

    fn lookup_region(&self, key: &[u8]) -> Result<u64> {
        // 通过 PD client 或本地路由表查找 key 所属 Region
        // 在 raftstore-v2 中可通过 StoreMeta 维护的路由表本地查询
        todo!("Use StoreMeta or PD client for region lookup")
    }

    fn temp_sst_path(&self, region_id: u64) -> String {
        format!("/tmp/pcr_sst_{}_{}.sst", region_id, self.total_flushes)
    }
}
```

### 3.2 `DirectIngest` — `src/direct_ingest.rs`

```rust
use engine_traits::{KvEngine, ImportExt, TabletRegistry};
use std::sync::Arc;

/// 直接向目标 Region 的 Tablet 中 Ingest SST 文件（绕过 Raft 状态机）
///
/// # 安全约束
/// 1. RegionEpoch 必须匹配——防止写入已分裂/合并的 Region
/// 2. 本节点必须是该 Region 的 Leader
/// 3. 必须获取 IngestLatch 防止并发冲突
/// 4. 不能与正在进行的 Raft Snapshot 冲突
///
/// # 参数
/// * `tablet_registry` — raftstore-v2 的 TabletRegistry
/// * `region_id` — 目标 Region ID
/// * `sst_data` — SST 文件原始字节
/// * `expected_start_key` — 预期的 Region start_key（用于 epoch 校验）
/// * `expected_end_key` — 预期的 Region end_key
pub fn ingest_sst<E: KvEngine>(
    tablet_registry: &TabletRegistry<E>,
    region_id: u64,
    sst_data: &[u8],
    expected_start_key: &[u8],
    expected_end_key: &[u8],
) -> Result<(u64, u64, usize)> {
    // 1. 查找 Tablet
    let tablet = tablet_registry
        .get(region_id)
        .ok_or(Error::TabletNotFound(region_id))?;

    // 2. Epoch 校验
    //    (需要通过 raftstore-v2 获取当前的 RegionEpoch 进行比较)
    //    epoch_check(region_id, expected_start_key, expected_end_key)?;

    // 3. 获取 IngestLatch（按 key range 加锁防止并发）
    let _latch = tablet
        .latest()
        .ok_or(Error::TabletNotAvailable(region_id))?
        .acquire_ingest_latch(Range::new(expected_start_key, expected_end_key));

    // 4. 将 SST 写入临时文件
    let tmp_path = format!("/tmp/pcr_ingest_{}_{}.sst", region_id, uuid::Uuid::new_v4());
    std::fs::write(&tmp_path, sst_data)?;

    // 5. 调用 RocksDB IngestExternalFile
    let engine = tablet.latest().unwrap();
    engine.ingest_external_file_cf(
        "default",
        &[&tmp_path],
        None,   // range
        true,   // force_allow_write — PCR 数据可以覆盖已有数据
    )?;

    // 6. 清理临时文件
    std::fs::remove_file(&tmp_path).ok();

    // 7. 返回 (region_id, ingested_bytes, ingested_kvs)
    Ok((region_id, sst_data.len() as u64, 0))
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tablet not found for region {0}")]
    TabletNotFound(u64),
    #[error("tablet not available for region {0}")]
    TabletNotAvailable(u64),
    #[error("region epoch mismatch for region {0}")]
    EpochMismatch(u64),
    #[error("ingest error: {0}")]
    IngestError(String),
}
```

### 3.3 `StreamSubscriber` — `src/subscriber.rs`

```rust
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender, UnboundedReceiver};
use grpcio::{ChannelBuilder, Environment, ClientStreamingSink};

/// 分区订阅信息
pub struct PartitionSubscription {
    pub region_id: u64,
    pub start_ts: u64,
    // 源节点的 gRPC 地址
    pub source_addr: String,
}

/// 合并后的 PCR 事件
pub struct PcrEventWithMeta {
    pub event: pcrpb::PcrEvent,
    pub region_id: u64,
}

/// 分区流订阅管理器
pub struct StreamSubscriber {
    // 每个分区的 gRPC 流句柄
    active_streams: Vec<PartitionStream>,
    // 合并后的事件通道（给 StreamIngest Task 消费）
    merged_events: UnboundedReceiver<PcrEventWithMeta>,
    sender: UnboundedSender<PcrEventWithMeta>,
}

impl StreamSubscriber {
    /// 创建新的订阅管理器
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            active_streams: Vec::new(),
            merged_events: rx,
            sender: tx,
        }
    }

    /// 订阅一个分区
    pub async fn subscribe(
        &mut self,
        partition: PartitionSubscription,
    ) -> Result<()> {
        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env).connect(&partition.source_addr);
        let client = pcrpb::PcrStreamClient::new(channel);

        let mut req = pcrpb::PcrSubscribeRequest::default();
        req.set_start_ts(partition.start_ts);
        // ... set partition spec ...

        let (tx, rx) = client.subscribe(&req)?;

        let sender = self.sender.clone();
        let region_id = partition.region_id;

        // 后台 tokio task 持续接收事件并入合并通道
        tokio::spawn(async move {
            while let Some(event) = rx.next().await {
                if let Ok(event) = event {
                    let _ = sender.send(PcrEventWithMeta { event, region_id });
                }
            }
        });

        Ok(())
    }

    /// 获取合并后的事件接收端
    pub fn events(&mut self) -> &mut UnboundedReceiver<PcrEventWithMeta> {
        &mut self.merged_events
    }

    /// 关闭所有订阅
    pub fn shutdown(&mut self) {
        // 关闭所有活跃的 gRPC stream
        self.active_streams.clear();
    }
}
```

### 3.4 `StreamIngestTask` — `src/task.rs`

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

/// Frontier 跟踪每个 Region 的复制进度
type Frontier = BTreeMap<u64, u64>;  // RegionId -> ResolvedTs

/// StreamIngestTask 是 Consumer 端的主控任务
pub struct StreamIngestTask<E: KvEngine> {
    // 订阅管理
    subscriber: StreamSubscriber,

    // SST 批量写入器
    batcher: SstBatcher<E>,

    // 全局 frontier: 每个 Region -> 已完成的 resolved_ts
    frontier: Frontier,

    // Checkpoint 管理器
    checkpoint_mgr: CheckpointManager,

    // 配置
    cfg: StreamIngestConfig,

    // 状态
    state: TaskState,
}

#[derive(PartialEq)]
enum TaskState {
    Initializing,   // 初始化中，建立各分区连接
    Subscribing,    // 已订阅，正在接收事件
    CuttingOver,    // 正在 cutover
    Completed,      // 已完成
    Failed,         // 失败
}

impl<E: KvEngine> StreamIngestTask<E> {
    pub fn new(
        cfg: StreamIngestConfig,
        tablet_registry: Arc<TabletRegistry<E>>,
        pd_client: Arc<dyn PdClient>,
    ) -> Self {
        let subscriber = StreamSubscriber::new();
        let batcher = SstBatcher::new(
            tablet_registry,
            cfg.max_kv_buffer_size.0 as usize,
            cfg.max_range_key_buffer_size.0 as usize,
        );
        Self {
            subscriber,
            batcher,
            frontier: BTreeMap::new(),
            checkpoint_mgr: CheckpointManager::new(pd_client),
            cfg,
            state: TaskState::Initializing,
        }
    }

    /// 主事件循环
    pub async fn run(&mut self) -> Result<()> {
        self.state = TaskState::Subscribing;

        loop {
            tokio::select! {
                Some(event_with_meta) = self.subscriber.events().recv() => {
                    self.handle_event(event_with_meta).await?;
                }
                _ = tokio::signal::ctrl_c() => {
                    break;
                }
            }
        }

        self.state = TaskState::Completed;
        Ok(())
    }

    /// 处理单个 PCR 事件
    async fn handle_event(&mut self, event_with_meta: PcrEventWithMeta) -> Result<()> {
        let PcrEventWithMeta { event, region_id } = event_with_meta;

        match event.event {
            Some(pcrpb::PcrEvent_oneof_event::kv_batch(batch)) => {
                // 将 KV 加入 SstBatcher
                for kv in batch.kvs {
                    self.batcher.add_kv(MvccKeyValue {
                        key: kv.key,
                        value: kv.value,
                    })?;
                }
                // 检查是否需要 flush
                if self.batcher.kv_buffer_size >= self.batcher.max_kv_buffer_size {
                    let results = self.batcher.flush()?;
                    self.record_ingest_results(results);
                }
            }
            Some(pcrpb::PcrEvent_oneof_event::sst_chunk(chunk)) => {
                // 快速路径：直接 Ingest SST
                if self.is_sst_within_region(&chunk, region_id) {
                    let result = direct_ingest::ingest_sst(
                        self.batcher.tablet_registry(),
                        region_id,
                        &chunk.data,
                        &chunk.start_key,
                        &chunk.end_key,
                    )?;
                    self.record_ingest_results(vec![result]);
                } else {
                    // 跨 Region 边界 → 扫描 SST 并分解为 KVs
                    let kvs = scan_sst_to_kvs(&chunk.data, &chunk.start_key, &chunk.end_key)?;
                    for kv in kvs {
                        self.batcher.add_kv(kv)?;
                    }
                    self.batcher.flush()?;
                }
            }
            Some(pcrpb::PcrEvent_oneof_event::checkpoint(cp)) => {
                // 推进 frontier
                for rid in &cp.region_ids {
                    self.frontier.insert(*rid, cp.resolved_ts);
                }
                // 刷新缓冲并记录 checkpoint
                self.batcher.flush()?;
                self.checkpoint_mgr.record_checkpoint(&self.frontier).await?;
            }
            Some(pcrpb::PcrEvent_oneof_event::delete_range(dr)) => {
                // 加入 range key buffer（需要先扩展 SstBatcher 支持）
            }
            Some(pcrpb::PcrEvent_oneof_event::split(split)) => {
                // Region 分裂 → 更新本地路由信息
            }
            None => {}
        }

        Ok(())
    }

    fn record_ingest_results(&mut self, results: Vec<(u64, u64, usize)>) {
        for (region_id, bytes, kvs) in results {
            STREAM_INGEST_METRICS.ingested_bytes.inc_by(bytes);
            STREAM_INGEST_METRICS.ingested_kvs.inc_by(kvs as u64);
        }
    }

    fn is_sst_within_region(&self, chunk: &pcrpb::PcrSstChunk, region_id: u64) -> bool {
        // 通过 PD 或本地路由检查 SST 的 key range 是否落在 region_id 对应范围内
        true // 简化实现
    }
}
```

### 3.5 `CheckpointManager` — `src/checkpoint.rs`

```rust
/// Checkpoint 管理器——将复制进度持久化到 PD MetaStore
pub struct CheckpointManager {
    pd_client: Arc<dyn PdClient>,
    last_recorded_ts: Option<u64>,
}

impl CheckpointManager {
    pub fn new(pd_client: Arc<dyn PdClient>) -> Self {
        Self { pd_client, last_recorded_ts: None }
    }

    /// 记录当前 frontier 到 PD
    pub async fn record_checkpoint(&mut self, frontier: &Frontier) -> Result<()> {
        // 计算全局最小 resolved_ts
        let min_ts = frontier
            .values()
            .min()
            .copied()
            .unwrap_or(0);

        // 去重——相同 ts 不重复写 PD
        if self.last_recorded_ts == Some(min_ts) {
            return Ok(());
        }
        self.last_recorded_ts = Some(min_ts);

        // 序列化 frontier → 写入 PD MetaStore
        let key = format!("/pcr/checkpoint/{}", self.task_id);
        let value = serde_json::to_vec(frontier)?;
        self.pd_client.meta_put(&key, &value).await?;

        Ok(())
    }

    /// 从 PD 恢复最近的 checkpoint
    pub async fn load_checkpoint(&self) -> Result<Frontier> {
        let key = format!("/pcr/checkpoint/{}", self.task_id);
        match self.pd_client.meta_get(&key).await {
            Ok(data) => Ok(serde_json::from_slice(&data)?),
            Err(_) => Ok(BTreeMap::new()), // 首次启动，无历史 checkpoint
        }
    }
}
```

### 3.6 `Config` — `src/config.rs`

```rust
use online_config::ConfigManager;
use tikv_util::config::{ReadableDuration, ReadableSize};
use tikv_util::worker::Scheduler;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct StreamIngestConfig {
    /// 是否启用 PCR 消费端任务
    #[online_config(skip)]
    pub enable: bool,

    /// KV 缓冲最大大小（达到后触发 flush）
    pub max_kv_buffer_size: ReadableSize,       // 默认 128MB

    /// RangeKey 缓冲最大大小
    pub max_range_key_buffer_size: ReadableSize, // 默认 32MB

    /// 最小 flush 间隔
    pub min_flush_interval: ReadableDuration,    // 默认 5s

    /// 目标源集群的 gRPC 地址
    #[online_config(skip)]
    pub source_address: String,

    /// Checkpoint 记录间隔
    pub checkpoint_interval: ReadableDuration,   // 默认 10s
}

impl Default for StreamIngestConfig {
    fn default() -> Self {
        Self {
            enable: false,
            max_kv_buffer_size: ReadableSize::mb(128),
            max_range_key_buffer_size: ReadableSize::mb(32),
            min_flush_interval: ReadableDuration::secs(5),
            source_address: String::new(),
            checkpoint_interval: ReadableDuration::secs(10),
        }
    }
}

/// ConfigManager 实现（用于动态配置热更新）
pub struct StreamIngestConfigManager {
    config: StreamIngestConfig,
    scheduler: Scheduler<Task>,
}

impl ConfigManager for StreamIngestConfigManager {
    fn dispatch(&mut self, change: online_config::ConfigChange) -> online_config::Result<()> {
        change.apply(&mut self.config);
        // 通知 StreamIngestTask 配置已变更
        self.scheduler.schedule(Task::ConfigChange(self.config.clone()))?;
        Ok(())
    }
}
```

---

## 四、CDC Producer 端修改

### 4.1 `delegate.rs` — 新增 `CmdType::IngestSst` 分支

**修改位置**: `components/cdc/src/delegate.rs` 第 871-877 行

```rust
// ---- 当前代码 ----
for mut req in requests {
    match req.get_cmd_type() {
        CmdType::Put => self.sink_put(req.take_put(), &mut rows_builder)?,
        _ => debug!("cdc skip other command"; ...),
    };
}

// ---- 修改后 ----
for mut req in requests {
    match req.get_cmd_type() {
        CmdType::Put => self.sink_put(req.take_put(), &mut rows_builder)?,

        // === 新增 ===
        CmdType::IngestSst => {
            let ingest_req = req.take_ingest_sst();
            let sst_meta = ingest_req.get_sst();
            self.sink_ingest_sst(sst_meta, &mut rows_builder)?;
        }

        CmdType::Delete => {
            // 可选：PCR 可能需要范围删除事件
            self.sink_delete(req.take_delete(), &mut rows_builder)?;
        }

        _ => debug!("cdc skip other command";
            "region_id" => self.region_id,
            "command" => ?req),
    };
}
```

### 4.2 新增 `sink_ingest_sst()` 方法

```rust
impl Delegate {
    /// 处理 IngestSst 命令——从 SST 文件中提取 KV 并发送 PCR 事件
    fn sink_ingest_sst(
        &mut self,
        sst_meta: &SstMeta,
        rows_builder: &mut RowsBuilder,
    ) -> Result<()> {
        // 1. 通过 SstImporter 打开 SST 文件
        let sst_reader = E::SstReader::open(
            &self.get_sst_path(sst_meta),
            self.key_manager.clone(),
        )?;

        // 2. 遍历 SST 中的 KV pairs
        let mut iter = sst_reader.iter();
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();

            // 3. 生成 PcrKV（不是 EventRow）
            //    在 PCR 模式下，直接发送原始 MVCC KV
            self.pcr_batcher.add_kv(PcrKV {
                key: key.to_vec(),
                value: value.to_vec(),
                op: OpType::PUT,
            });

            iter.next();
        }

        // 或者：直接发送完整的 SST 数据
        // self.pcr_batcher.add_sst_chunk(sst_data, start_key, end_key, ts);

        Ok(())
    }
}
```

### 4.3 新增 `pcr_event_batcher.rs`

```rust
/// PCR 事件批处理器
pub struct PcrEventBatcher {
    batch: PcrEventBatch,
    size: usize,
    batch_byte_size: usize,  // 达到后 flush
}

struct PcrEventBatch {
    kvs: Vec<PcrKV>,
    sst_chunks: Vec<PcrSstChunk>,
    delete_ranges: Vec<PcrDeleteRange>,
    split_points: Vec<Vec<u8>>,
}

impl PcrEventBatcher {
    pub fn new(batch_byte_size: usize) -> Self { ... }

    pub fn add_kv(&mut self, kv: PcrKV) {
        self.size += kv.key.len() + kv.value.len();
        self.batch.kvs.push(kv);
    }

    pub fn add_sst_chunk(&mut self, data: Vec<u8>, start: Vec<u8>, end: Vec<u8>, ts: u64) {
        self.size += data.len();
        self.batch.sst_chunks.push(PcrSstChunk {
            data, start_key: start, end_key: end, write_ts: ts,
        });
    }

    pub fn should_flush(&self) -> bool {
        self.size >= self.batch_byte_size
    }

    pub fn flush(&mut self) -> PcrEvent {
        // 序列化为 protobuf 并重置
        let event = PcrEvent {
            stream_seq: self.next_seq(),
            event: Some(PcrEvent_oneof_event::kv_batch(PcrKvBatch {
                kvs: std::mem::take(&mut self.batch.kvs),
            })),
        };
        self.size = 0;
        event
    }
}
```

### 4.4 新增 `pcr_service.rs` — gRPC 服务

```rust
/// PCR gRPC 服务——允许 Consumer 订阅 Partition 事件流
#[derive(Clone)]
pub struct PcrService {
    scheduler: Scheduler<Task>,
}

impl PcrService {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl pcrpb::PcrStream for PcrService {
    fn subscribe(
        &mut self,
        ctx: grpcio::RpcContext<'_>,
        req: pcrpb::PcrSubscribeRequest,
        sink: grpcio::ServerStreamingSink<pcrpb::PcrEvent>,
    ) {
        let scheduler = self.scheduler.clone();

        // 创建独立的 PCR event stream
        let (tx, rx) = mpsc::unbounded_channel();

        // 通知 CDC endpoint 开始为此分区生成 PCR 事件
        let task = Task::StartPcrStream {
            region_id: req.get_partition().get_region_id(),
            start_ts: req.get_start_ts(),
            event_sink: tx,
        };
        scheduler.schedule(task);

        // 从 channel 读取事件并写入 gRPC sink
        while let Ok(event) = rx.recv() {
            if sink.send((event, WriteFlags::default())).is_err() {
                break;
            }
        }
    }
}
```

---

## 五、Server 集成（`server2.rs`）

### 5.1 修改 `TikvConfig`

在 `src/config/mod.rs` 中：

```rust
pub struct TikvConfig {
    // ... 已有字段 ...
    #[online_config(submodule)]
    pub stream_ingest: StreamIngestConfig,  // ← 新增
}

// Module 枚举新增
pub enum Module {
    // ... 已有变体 ...
    StreamIngest,  // ← 新增
}

// From<&str> 实现新增
impl From<&str> for Module {
    fn from(m: &str) -> Module {
        match m {
            // ...
            "stream_ingest" => Module::StreamIngest,
            // ...
        }
    }
}
```

### 5.2 修改 `server2.rs` — `init_servers`

```rust
// StreamIngest（PCR Consumer）—— 条件初始化
self.stream_ingest_scheduler = if self.core.config.stream_ingest.enable {
    let ingest_cfg = self.core.config.stream_ingest.clone();
    let tablet_registry = self.tablet_registry.as_ref().unwrap().clone();
    let pd_client = self.pd_client.clone();

    // 创建任务并启动
    let task = stream_ingest::create_stream_ingest_task(
        ingest_cfg,
        tablet_registry,
        pd_client.clone(),
    );

    let mut worker = Box::new(
        self.core.background_worker.lazy_build("stream-ingest")
    );
    let scheduler = worker.scheduler();
    worker.start(task);

    self.cfg_controller.as_mut().unwrap().register(
        tikv::config::Module::StreamIngest,
        Box::new(StreamIngestConfigManager::new(scheduler.clone(), self.core.config.stream_ingest.clone())),
    );

    Some(scheduler)
} else {
    None
};
```

### 5.3 修改 `server2.rs` — `register_services`

```rust
// PCR gRPC 服务
if let Some(sched) = self.pcr_scheduler.take() {
    let pcr_service = pcrpb::create_pcr_stream(PcrService::new(sched));
    if servers.server.register_service(pcr_service).is_some() {
        fatal!("failed to register pcr service");
    }
}
```

---

## 六、Cargo 工作区修改

### 6.1 根 `Cargo.toml`

```toml
[workspace]
members = [
    # ... 已有 members（按字母顺序插入）...
    "components/stream-ingest",
    # ...
]

[workspace.dependencies]
# ... 已有依赖 ...
stream-ingest = { path = "components/stream-ingest", default-features = false }
```

### 6.2 常用依赖模式汇总

| 依赖 | 引用方式 | 用途 |
|------|---------|------|
| `engine_traits` | `workspace = true` | `KvEngine` trait、`ImportExt`、`SstExt` |
| `kvproto` | `workspace = true` | gRPC 服务存根、protobuf 类型 |
| `raftstore-v2` | `workspace = true` | `TabletRegistry`、`StoreMeta` 路由 |
| `sst_importer` | `workspace = true` | `SstImporter`、SST 文件读写 |
| `tikv_kv` | `workspace = true` | `Modify` 枚举、`WriteData` |
| `tikv` | `workspace = true` | 顶层配置类型 |
| `grpcio` | `workspace = true` | gRPC 框架 |
| `pd_client` | `workspace = true` | PD 通信 |
| `tikv_util` | `workspace = true` | `worker::Runnable`、`Scheduler`、`ReadableSize` |
| `online_config` | `workspace = true` | `ConfigManager`、`OnlineConfig` derive |

---

## 七、错误类型定义

```rust
// stream-ingest/src/errors.rs

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tablet not found for region {0}")]
    TabletNotFound(u64),

    #[error("tablet not available for region {0}")]
    TabletNotAvailable(u64),

    #[error("region epoch mismatch: expected conf_ver={0}, version={1}, got conf_ver={2}, version={3}")]
    RegionEpochMismatch(u64, u64, u64, u64),

    #[error("SST ingest failed: {0}")]
    IngestError(String),

    #[error("SST writer error: {0}")]
    SstWriterError(String),

    #[error("gRPC subscription error: {0}")]
    SubscriptionError(String),

    #[error("checkpoint error: {0}")]
    CheckpointError(String),

    #[error("serde error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

---

## 八、验证方式

### 8.1 单元测试

```
components/stream-ingest/
├── tests/
│   ├── sst_batcher_test.rs    // SstBatcher 排序、flush、跨边界测试
│   ├── direct_ingest_test.rs  // TabletDirectIngest epoch 校验、并发安全测试
│   └── integration_test.rs    // 端到端：源 TiKV CDC → 目标 TiKV Ingest
```

### 8.2 验证检查点

- [ ] `SstBatcher` 按 Key 排序正确性（生成 SST → 用 SstReader 验证顺序）
- [ ] `TabletDirectIngest` Epoch 校验正确拒绝过期请求
- [ ] `TabletDirectIngest` 与 Raft apply 并发无冲突
- [ ] CDC `IngestSst` 分支正确从 SST 提取 KV
- [ ] `SstChunk` 快速路径 vs ScanSST 分解路径结果一致
- [ ] Checkpoint 断点续传：中断 → 恢复 → 从 checkpoint 位置继续
- [ ] Lightning local 模式数据 → PCR 可同步
- [ ] Cutover 后目标数据与源数据一致（checksum 验证）

---

## 九、新增文件清单

| 文件 | 类型 | 说明 |
|------|------|------|
| `components/stream-ingest/Cargo.toml` | 新建 | crate 定义 |
| `components/stream-ingest/src/lib.rs` | 新建 | 公共接口 |
| `components/stream-ingest/src/sst_batcher.rs` | 新建 | 流式 SST 缓冲写入器 |
| `components/stream-ingest/src/direct_ingest.rs` | 新建 | Tablet 直接 Ingest 路径 |
| `components/stream-ingest/src/subscriber.rs` | 新建 | gRPC 多分区订阅管理 |
| `components/stream-ingest/src/task.rs` | 新建 | StreamIngestTask 主控 |
| `components/stream-ingest/src/checkpoint.rs` | 新建 | Checkpoint 管理与恢复 |
| `components/stream-ingest/src/config.rs` | 新建 | 配置定义 + ConfigManager |
| `components/stream-ingest/src/metrics.rs` | 新建 | Prometheus 指标 |
| `components/stream-ingest/src/errors.rs` | 新建 | 错误类型 |
| `components/cdc/src/pcr_event_batcher.rs` | 新建 | PCR 事件批处理 |
| `components/cdc/src/pcr_service.rs` | 新建 | PCR gRPC 服务 |
| `kvproto/pcrpb/pcrpb.proto` | 新建 | PCR 协议定义 |
| `components/cdc/src/delegate.rs` | 修改 | 新增 IngestSst 处理分支 |
| `components/cdc/src/observer.rs` | 修改 | 新增 PCR observer 注册 |
| `src/config/mod.rs` | 修改 | 新增 StreamIngestConfig |
| `components/server/src/server2.rs` | 修改 | 注册 StreamIngest 任务和 gRPC 服务 |
| `Cargo.toml`（根） | 修改 | 新增 workspace member 和 dependency |
