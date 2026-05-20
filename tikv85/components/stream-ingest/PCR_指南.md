# TiKV PCR（物理集群复制）配置与使用手册

## 概述

TiKV PCR 实现集群间字节级实时复制。源集群的 KV 变更为 MVCC 原始格式通过 gRPC 流传输到目标集群，目标集群绕过 Raft 共识层直接写入 RocksDB。

### 架构角色

| 角色 | 配置项 | 说明 |
|------|--------|------|
| **Producer** | CDC 扩展 (`pcr_service`) | 源集群，捕获 KV 变更并通过 gRPC 流出 |
| **Consumer** | StreamIngest (`stream-ingest`) | 目标集群，接收 KV 事件流并直接 Ingest |

> **注意**：同一个 TiKV 节点可以**同时**作为 Producer 和 Consumer。例如集群 A 向集群 B 复制的同时，集群 B 也可以向集群 C 复制。Consumer 侧的 `stream-ingest.enable` 和 Producer 侧的 gRPC 服务是独立开关。

---

## 一、配置参数

### 1.1 Consumer 侧：`[stream-ingest]`

控制目标集群上的 PCR 消费端行为。

```toml
[stream-ingest]
# === 启用开关 ===
# 是否启用 PCR Consumer 任务。设为 true 后，TiKV 启动时会创建
# StreamIngestTask 并注册 PcrStream gRPC 服务。
# 类型: bool  默认: false  热更新: 否
enable = true

# === 缓冲区 ===
# KV 缓冲最大大小，达到后触发 flush（排序→生成SST→DirectIngest）。
# 更大的值意味着更少但更大的 SST 文件，减少 Ingest 频率但增加内存占用。
# 类型: size  默认: "128MB"  范围: 64MB ~ 1GB  热更新: 是
max-kv-buffer-size = "128MB"

# RangeKey（范围删除）缓冲最大大小。与 KV 缓冲独立触发。
# 类型: size  默认: "32MB"  范围: 16MB ~ 256MB  热更新: 是
max-range-key-buffer-size = "32MB"

# === 刷新控制 ===
# 最小 flush 间隔。Checkpoint 事件到达时，如果距上次 flush 不足此间隔则延迟。
# 避免过于频繁的 SST 生成和 Ingest。
# 类型: duration  默认: "5s"  范围: 1s ~ 60s  热更新: 是
min-flush-interval = "5s"

# === 连接 ===
# 源集群 PD 地址。Consumer 通过此地址发现源集群的 TiKV 节点并建立 gRPC 订阅。
# 格式: "host:port"（例如 "source-pd:2379"）
# 类型: string  默认: ""  热更新: 否
source-address = "source-pd.cluster-a:2379"

# === Checkpoint ===
# Checkpoint 记录间隔。目标集群 TiKV 定期将复制进度（每个 Region 的
# resolved_ts）持久化到目标集群 PD 的 MetaStore，用于：
#   - 断点续传：目标 TiKV 重启后从 PD 读取最后进度继续复制
#   - Cutover 判断：确认所有 Region 已追上 cutover_ts
# 注意：checkpoint 写入的是目标集群 PD，不是源集群。
# 类型: duration  默认: "10s"  范围: 5s ~ 300s  热更新: 是
checkpoint-interval = "10s"

# === 并发 ===
# 订阅线程数。控制同时连接到源集群的 gRPC stream 数量。
# 更大的值意味着更高的订阅并行度，但也会增加连接开销。
# 类型: int  默认: CPU核心数  范围: 1 ~ 64  热更新: 否
num-subscription-threads = 8
```

### 1.2 Producer 侧（依赖已有 CDC 配置）

Producer 侧复用 TiKV CDC 组件，无需额外专用配置。PcrStream gRPC 服务随 `stream-ingest.enable` 自动注册。相关已有配置：

```toml
[cdc]
# CDC 增量扫描线程数。影响 PCR 初始全量扫描速度。
incremental-scan-threads = 4
# CDC 增量扫描并发度。
incremental-scan-concurrency = 6
# CDC sink 内存配额。影响 PCR 事件批处理缓冲。
sink-memory-quota = "512MB"

# Resolved TS 间隔。决定 PCR Checkpoint 事件的频率，
# 间隔越短，复制延迟越低但开销越大。
min-ts-interval = "1s"

[resolved-ts]
# Resolved TS 是否启用。PCR 依赖此功能。
enable = true
# Resolved TS 高级间隔（影响 checkpoint 粒度）。
advance-ts-interval = "1s"
```

---

## 二、使用说明

### 2.1 部署前检查

**源集群**：
```bash
# CDC 和 Resolved TS 必须启用
$ tikv-ctl --host=source-tikv:20160 config get cdc.min-ts-interval
$ tikv-ctl --host=source-tikv:20160 config get resolved-ts.enable
```

**目标集群**：
```bash
# 目标集群必须使用 raftstore-v2 (Partitioned Raft KV)
$ tikv-ctl --host=target-tikv:20160 config get storage.engine
# 应返回: partitioned-raft-kv 或 raft-kv2
```

### 2.2 启动 PCR 复制

**步骤 1：配置目标集群**

在所有目标 TiKV 节点的配置文件中添加：

```toml
[stream-ingest]
enable = true
source-address = "source-pd:2379"
max-kv-buffer-size = "128MB"
checkpoint-interval = "10s"
```

**步骤 2：重启目标 TiKV**

```bash
# 滚动重启目标集群的 TiKV 节点
tiup cluster restart target-cluster --role tikv
```

重启后：
- 目标 TiKV 启动 `StreamIngestTask`
- CDC Endpoint 被注入 `SstImporter`（用于读取 Lightning/DDL SST）
- `PcrStream` gRPC 服务在目标 TiKV 上注册

**步骤 3：初始化复制流**

使用 `pcr-ctl start` 创建 PCR 任务：

```bash
# 全库复制
pcr-ctl -p target-pd:2379 start \
  -s source-pd:2379 \
  -t "tpcc.*" \
  -r 24h \
  -n tpcc-replica

# 多表复制
pcr-ctl -p target-pd:2379 start \
  -s source-pd:2379 \
  -t "tpcc.orders" \
  -t "tpcc.customer" \
  -t "tpcc.warehouse" \
  -r 7d

# 只读 standby（可用于查询分流）
pcr-ctl -p target-pd:2379 start \
  -s source-pd:2379 \
  -t "analytics.*" \
  -r 24h \
  --read-only
```

初始化过程中：
1. Consumer 连接源集群 PcrService，对每个 Region 发起 `Subscribe` gRPC 调用
2. 源 CDC 开始初始全量扫描 + 增量流
3. Consumer 收到 KV 事件 → SstBatcher 缓冲 → 排序 → 生成 SST → DirectIngest → RocksDB

### 2.3 监控复制状态

**通过 `pcr-ctl status`**（推荐）：

```bash
# 查看所有任务概览
pcr-ctl -p target-pd:2379 list

# 查看特定任务状态
pcr-ctl -p target-pd:2379 status tpcc-replica

# 查看详细每 Region 进度
pcr-ctl -p target-pd:2379 status tpcc-replica --detailed

# 持续刷新监控（类似 top）
pcr-ctl -p target-pd:2379 status tpcc-replica --watch

# JSON 格式输出（便于脚本集成）
pcr-ctl -p target-pd:2379 list --json
```

**输出示例**：

```
┌── PCR Task Status ──────────────────────────┐
│ Task:    tpcc-replica                        │
│ Status:  RUNNING                             │
│ Lag:     3.2s                                │
│ Regions: 128 / 128 synced                    │
│ Ingest:  2.4 GB / 15.8M KVs                  │
├──────────────────────────────────────────────┤
│ Per-Region Progress:                         │
│  R1    ts=426624231625982140  lag=2.1s  ██████████
│  R2    ts=426624231625982150  lag=0.5s  ██████████
│  R3    ts=426624231625982130  lag=5.8s  ██████░░░░
│  R4    ts=426624231625982145  lag=3.2s  ██████░░░░
└──────────────────────────────────────────────┘
```

**通过 PD API 查询**：

```bash
curl http://target-pd:2379/pd/api/v1/pcr/checkpoint/pcr_default
```

**Prometheus 指标**：

| 指标 | 含义 |
|------|------|
| `pcr_ingested_bytes_total` | 已 Ingest 的总字节数 |
| `pcr_ingested_kvs_total` | 已 Ingest 的 KV 对数 |
| `pcr_flush_count_total` | SstBatcher Flush 次数 |
| `pcr_flush_latency_seconds` | 每次 Flush 延迟（p50/p99） |
| `pcr_buffer_size_bytes` | 当前 SstBatcher 缓冲大小 |
| `pcr_active_subscriptions` | 活跃的源 Region 订阅数 |
| `pcr_checkpoint_lag_seconds` | Checkpoint 延迟（当前时间 - resolved_ts） |

**Grafana**：指标按 `tikv_*` 前缀导出，可添加到 TiKV Dashboard。

### 2.4 pcr-ctl 命令参考

`pcr-ctl` 是 PCR 的命令行管理工具，部署在目标集群侧，通过 PD 进行任务管理。

#### 全局参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-p, --pd` | 目标集群 PD 地址（任务管理入口） | `127.0.0.1:2379` |

#### start — 创建复制任务

```bash
pcr-ctl start [OPTIONS] -s <SOURCE_PD> -t <TABLES>...
```

| 参数 | 说明 | 必填 |
|------|------|------|
| `-s, --source-pd` | 源集群 PD 地址 | 是 |
| `-t, --source-tables` | 源表名（格式 `db.*` 或 `db.table`，可多次指定） | 是 |
| `-r, --retention` | 复制保留窗口 | 否（默认 `24h`） |
| `-n, --task-name` | 任务名称（自动生成） | 否 |
| `--read-only` | 创建只读 standby | 否 |

#### status — 查看复制状态

```bash
pcr-ctl status [TASK] [OPTIONS]
```

| 参数 | 说明 |
|------|------|
| `TASK` | 任务名或 ID（默认 `all` 显示全部） |
| `-d, --detailed` | 显示每 Region 详细进度 |
| `-w, --watch` | 持续刷新（类似 `top`） |

#### pause / resume — 暂停与恢复

```bash
pcr-ctl pause <TASK>
pcr-ctl resume <TASK>
```

暂停后源端 CDC 订阅保持但目标端停止 Ingest；恢复后从 checkpoint 继续。

#### cutover — 切换接管

```bash
pcr-ctl cutover <TASK> --latest
pcr-ctl cutover <TASK> -t "2026-04-27T12:00:00Z"
```

Cutover 后目标数据变为可读写，源端 CDC 订阅关闭。此操作不可逆。

#### list — 列出所有任务

```bash
pcr-ctl list [OPTIONS]
```

| 参数 | 说明 |
|------|------|
| `-a, --active` | 仅显示活跃任务 |
| `--json` | JSON 格式输出 |

#### delete — 删除任务

```bash
pcr-ctl delete <TASK>
pcr-ctl delete <TASK> -f   # 跳过确认
```

删除任务及其所有 checkpoint 数据。不可恢复。

### 2.5 Cutover 流程

Cutover 将目标集群从只读 standby 切换为可读写，通过 `pcr-ctl cutover` 触发：

```bash
# 切到最新已同步时间点
pcr-ctl -p target-pd:2379 cutover tpcc-replica --latest

# 切到指定系统时间点
pcr-ctl -p target-pd:2379 cutover tpcc-replica -t "2026-04-27T12:00:00Z"
```

执行后可 watch 等待完成：

```bash
pcr-ctl -p target-pd:2379 status tpcc-replica --watch
```

内部流程：

```
1. pcr-ctl cutover 命令 → PD 记录 cutover_ts
2. Consumer 停止接受新 KV → flush 所有 buffer
3. 等待 min(frontier) >= cutover_ts
4. 关闭所有源端 gRPC 订阅 → 调用 complete_replication_stream()
5. 最终 flush + ingest 所有剩余 SST
6. 更新 PD 元数据：标记任务完成
7. 目标集群现在可正常接受读写
```

### 2.6 断点续传

Consumer 崩溃或重启后自动恢复。Checkpoint 存储在**目标集群 PD** 中：

```
1. 目标 TiKV 重启 → StreamIngestTask 初始化
2. 从目标 PD 读取 last checkpoint: {RegionId → ResolvedTs}
3. 对每个源 Region，向源集群 CDC 从 resolved_ts + 1 开始订阅
4. 重新初始化 SstBatcher（buffer 为空，零数据丢失）
5. 继续事件循环
```

已 Ingest 的数据不会被重复写入——CDC 订阅从 checkpoint 位置开始，且 SSTBatcher 的 `stream_seq` 去重机制会跳过已持久化的数据。

> **注意**：checkpoint 记录在目标 PD 中，源集群 PD 不存储复制进度。如果目标 PD 数据丢失（罕见），需要从源集群重新初始化复制。

---

## 三、配置场景示例

### 场景 1：低延迟 OLTP 复制

```toml
[stream-ingest]
enable = true
source-address = "source-pd:2379"
max-kv-buffer-size = "64MB"      # 更小的 buffer → 更频繁 flush → 更低延迟
min-flush-interval = "1s"        # 更短的间隔
checkpoint-interval = "5s"

[cdc]
min-ts-interval = "500ms"        # 更频繁的 resolved_ts

[resolved-ts]
advance-ts-interval = "500ms"
```

### 场景 2：大数据量批量导入

```toml
[stream-ingest]
enable = true
source-address = "source-pd:2379"
max-kv-buffer-size = "512MB"     # 更大的 buffer → 更大的 SST → 更高吞吐
min-flush-interval = "10s"       # 合并更多 KV
checkpoint-interval = "30s"      # 减少 PD 写入频率
num-subscription-threads = 16
```

### 场景 3：最小资源占用

```toml
[stream-ingest]
enable = true
source-address = "source-pd:2379"
max-kv-buffer-size = "64MB"
min-flush-interval = "5s"
checkpoint-interval = "30s"
num-subscription-threads = 2      # 限制连接数

[cdc]
sink-memory-quota = "256MB"      # 限制 CDC sink 内存
```

### 2.7 DDL 同步

PCR 采用 **Approach B（tikv-only）** 方案实现 DDL 同步，无需修改 TiDB 或 PD。

**原理**：

TiDB 所有 DDL 元数据（CREATE TABLE、ALTER TABLE、ADD INDEX 等）通过系统表存储，底层是常规 KV 写入。PCR 的 CDC 捕获已自动覆盖这些 KV。

| 阶段 | DDL 可见性 | 说明 |
|------|-----------|------|
| 复制期间 | DDL KV 数据实时到达目标 TiKV，但目标 TiDB **缓存旧 Schema** | 数据已同步，Schema 未刷新 |
| Cutover 后 | 重启目标 TiDB → 从 KV 读取系统表 → **Schema 自动一致** | 零额外代码 |

**DDL 操作流程**：

```bash
# 1. 源端执行 DDL
mysql> ALTER TABLE orders ADD COLUMN priority INT;

# 2. DDL KV 数据被 PCR 实时复制到目标 TiKV（自动，无需操作）

# 3. Cutover 后重启目标 TiDB
tiup cluster restart target-cluster --role tidb

# 4. 目标 TiDB 读取已复制的系统表 → Schema 自动同步
mysql> DESC orders;  # priority 列已出现
```

**未来增强**（TiDB PR ~1 周）：

目标 TiDB 轮询 PCR 发布的 schema version key，实现复制期间 Schema 实时可见，避免等 cutover。

---

## 四、故障处理

### Consumer 跟不上源端写入速度

**现象**：`pcr_checkpoint_lag_seconds` 持续增长。

**排查**：
```bash
# 检查 SST Ingest 延迟
$ tikv-ctl metrics | grep pcr_flush_latency

# 检查 RocksDB compaction 压力
$ tikv-ctl metrics | grep rocksdb_compaction
```

**调优**：
- 增大 `max-kv-buffer-size`（更大的 SST → 更少 Ingest → 更高吞吐）
- 增加 `num-subscription-threads`
- 检查目标集群磁盘 I/O 是否瓶颈

### 源集群 Region 分裂导致路由错误

**现象**：Consumer 日志出现 "epoch mismatch"。

**处理**：自动恢复——Consumer 收到 Split 事件后更新路由表，重新订阅正确的 Region 分区。已缓冲但不再属于当前 Region 的 KV 在 Epoch 校验时被拒绝，CDC 会重新发送。

### PD 连接丢失

**现象**：Checkpoint 记录失败。

**处理**：Checkpoint 失败不会中断复制。SstBatcher 继续缓冲和 Ingest。PD 恢复后，下一次 checkpoint 自动补录。

---

## 五、当前限制与后续工作

| 限制 | 影响 | 计划 |
|------|------|------|
| 需 raftstore-v2 | v1 共享 RocksDB 不支持旁路 Ingest | 后续评估 v1 兼容 |
| `kvproto/pcrpb` 待公开 | PCR 类型定义尚未作为独立 crate 发布 | 提 PR 到 pingcap/kvproto |
| 无跨集群加密 | gRPC 流未加密 | 复用 TiKV TLS 配置 |
| DDL 实时可见性 | DDL 元数据已实时复制。目标 TiDB 重启（cutover 后）自动读取正确 Schema。复制期间 Schema 变化不立即可见——这是 tikv-only 方案的已知权衡 | TiDB PR (~1周): 轮询 PCR schema version 实现复制期间实时可见 |
