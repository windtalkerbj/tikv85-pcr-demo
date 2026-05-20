# PCR 架构快照 — 2026-05-08

> **回溯说明**：此文档是架构重设计前的完整状态快照。若新方案走不通，执行 `claude --resume ea6f15c4-edb6-4a68-8e5c-0de7678209d5` 回到当前会话，并参考此文档。

---

## 一、整体架构

```
Source TiKV (raft-kv v1)                Target TiKV (raft-kv2 + stream-ingest)
┌──────────────────────┐                ┌──────────────────────────────┐
│ CDC Endpoint          │                │ StreamIngestTask              │
│  ├─ Delegate          │   gRPC PcrStream │  ├─ StreamSubscriber         │
│  │  └─ sink_data()    │ ═══════════════▶│  │  └─ 多分区订阅 + 自动重连  │
│  ├─ PcrEventBatcher   │                │  ├─ SstBatcher               │
│  └─ PcrService        │                │  │  └─ KV→排序→SST→DirectIngest│
│     └─ Subscribe()    │                │  ├─ CheckpointManager        │
│                       │                │  └─ Frontier (per Region)    │
│ RocksDB Snapshot       │                │                              │
│  ├─ scan_default_cf   │                │ HTTP API :20190              │
│  └─ scan_write_cf_since│               │  ├─ POST /pcr/control        │
│                       │                │  └─ GET  /pcr/status         │
└──────────────────────┘                └──────────────────────────────┘
```

**关键设计决策**：
- Source 用 raft-kv v1（单 RocksDB），Target 用 raft-kv v2（TabletRegistry）
- Consumer 绕过 Raft：SST 文件通过 `ingest_external_file_cf` 直接写 RocksDB
- PCR HTTP API 端口 `20190`，独立于 TiKV 服务端口
- PCR gRPC 服务注册在 Target TiKV 的 gRPC server 上

---

## 二、核心组件

### 2.1 Source 端（`components/cdc/src/`）

| 文件 | 职责 | 关键函数/结构 |
|------|------|-------------|
| `pcr_service.rs` | PcrStream gRPC 服务端 | `Service::subscribe()` — 接收订阅请求，spawn bridge task |
| `pcr_snapshot.rs` | RocksDB 快照扫描 | `scan_default_cf()`, `scan_write_cf_since()` |
| `pcr_event_batcher.rs` | 事件批处理 | 1MB 缓冲区 + 150ms 定时 flush |
| `pcr_metrics.rs` | Producer 指标 | `bridge_bytes_sent`, `snapshot_regions_scanned` |
| `delegate.rs` | CDC Delegate 扩展 | `sink_put` 双写 PCR, IngestSst 处理 |
| `endpoint.rs` | CDC Endpoint 扩展 | `Task::StartPcrStream` 变体 |

### 2.2 Target 端（`components/stream-ingest/src/`）

| 文件 | 职责 |
|------|------|
| `task.rs` | StreamIngestTask 主控 + `Runnable` 实现 + 事件循环 |
| `subscriber.rs` | gRPC 多分区订阅，PartitionSubscription，自动重连 |
| `sst_batcher.rs` | KV 缓冲→排序→SST 生成→调用 DirectIngest |
| `direct_ingest.rs` | Epoch 校验 + IngestLatch + `ingest_external_file_cf` |
| `checkpoint.rs` | PD MetaStore checkpoint 持久化 |
| `http_control.rs` | PCR HTTP API（create/pause/resume/cutover/activate） |
| `config.rs` | StreamIngestConfig |
| `metrics.rs` | Consumer 指标 |

### 2.3 Server 集成

| 文件 | 改动 |
|------|------|
| `components/server/src/server2.rs` | `stream_ingest_scheduler` 字段，`init_servers()` 初始化 StreamIngestTask + HTTP API，`register_services()` 注册 PcrService gRPC |
| `src/config/mod.rs` | `StreamIngestConfig` 定义，`Module::StreamIngest` 枚举 |

---

## 三、Bounded Scan 最终方案

### 编码

```rust
fn encode_bound(pd_key: &[u8]) -> Vec<u8> {
    Key::from_encoded_slice(pd_key)  // PD key 已是 memcomparable，不做二次编码
        .append_ts(TimeStamp::max()) // encode_u64_desc(MAX) = 0x00*8
        .into_encoded()
}
```

### 扫描

```
1. lower_bound = encode_bound(from)  → RocksDB set_lower_bound
2. seek(lower_bound)                 → 定位到 Region 起点
3. while loop:
   - decode_ts → filter by since_ts
   - truncate_ts_for → user_key
   - user_key >= to ? break : continue  (代码级过滤，不用 RocksDB upper_bound)
```

### 为什么不直接用 RocksDB upper_bound

写 CF 的 custom comparator 不支持 `iterate_upper_bound`——设置后所有 key 被过滤。

---

## 四、断点续传生命周期

```
Idle ──create──▶ Creating ──全量scan_default_cf──▶ Subscribing
                   │                                 │
                   │                            pause │ │ resume(delta)
                   │                                 ▼ │
                   └───────────────────────────── Paused
                                                   
Subscribing ──cutover──▶ CuttingOver ──activate──▶ Activated
```

- `create`: start_ts=0，全量扫描 default CF（unbounded range）
- `resume`: start_ts=checkpoint_ts，增量扫描 write CF（per-region bounded）
- `pause`: 记录 checkpoint 到 PD MetaStore

---

## 五、代码修改清单

### 新建文件

```
components/stream-ingest/           (17 files)
  src/lib.rs, config.rs, sst_batcher.rs, direct_ingest.rs,
  task.rs, subscriber.rs, checkpoint.rs, metrics.rs,
  http_control.rs, errors.rs, pcrpb_gen/

components/cdc/src/
  pcr_event_batcher.rs, pcr_service.rs, pcr_snapshot.rs, pcr_metrics.rs

proto/pcrpb/pcrpb.proto

cmd/pcr-ctl/src/main.rs

metrics/grafana/tikv_pcr.json
```

### 修改文件

```
components/cdc/src/delegate.rs       ← IngestSst + PCR 双写
components/cdc/src/endpoint.rs       ← Task::StartPcrStream
components/cdc/src/lib.rs            ← pub mod 导出
components/cdc/Cargo.toml            ← sst_importer 依赖

components/server/src/server2.rs     ← stream-ingest 初始化/注册

src/config/mod.rs                    ← StreamIngestConfig

Cargo.toml (根)                      ← workspace member

components/server/Cargo.toml         ← stream-ingest 依赖
```

---

## 六、已知限制

| 限制 | 影响 | 替代方案 |
|------|------|---------|
| RocksDB upper_bound 不可用 | bounded scan 用代码级 to 检查 | 已验证代码级过滤正确 |
| PcrSstChunk 未实现 | Lightning 批量导入不复制 | 待实现 |
| PcrDeleteRange 未实现 | DROP TABLE 不复制 | 待实现 |
| Cutover/Activate 未端到端测试 | 已有代码未验证 | 待测试 |
| 单机 1+1 部署 | 多节点行为未验证 | 待扩展 |

---

## 七、测试环境

### 集群拓扑

| 组件 | 端口 | 数据目录 |
|------|------|---------|
| Source PD | 2379 | /tmp/pcr-src-pd |
| Target PD | 2380 | /tmp/pcr-tgt-pd |
| Source TiKV | 20162 | /tmp/pcr-src-data |
| Target TiKV | 20161 | /tmp/pcr-tgt-data |
| TiDB | 4000 | /tmp/pcr-tidb-data |
| Prometheus | 9091 | /tmp/pcr-prom-data |
| Grafana | 3001 | /tmp/pcr-grafana-data |
| PCR HTTP API | 20190 | — |

### 配置文件（持久化）

```
develop/pcr-configs/
  pcr-src-tikv.toml      ← source: raft-kv, simple config
  pcr-tgt-tikv.toml      ← target: raft-kv2 + stream-ingest
  pcr-prometheus.yml     ← scrape tikv-src:40185, tikv-tgt:40186
  pcr-grafana.ini        ← port 3001
```

### 注意事项

- tiup 通过 launchd 注册为 macOS 服务，重启后自动拉起抢占端口 20161/20162
- 测试前需 `launchctl bootout gui/$(id -u)/com.tidb.tikv-{20161,20162}`
- 部分重启会导致 duplicate store address / cluster ID mismatch

---

## 八、会话回溯

当前会话 ID：`ea6f15c4-edb6-4a68-8e5c-0de7678209d5`

mem0 记忆：41 条（PCR 全部关键知识已持久化）

相关文档：
- `develop/pcr-research.md` — 前期研究
- `develop/daily-briefing.md` — 全周期简报
- `develop/pcr_checkpoint_resume_devlog.md` — 断点续传开发历程
- `develop/pcr-unimplemented.md` — 未实现功能
- `develop/thinking/` — 每日思索过程
