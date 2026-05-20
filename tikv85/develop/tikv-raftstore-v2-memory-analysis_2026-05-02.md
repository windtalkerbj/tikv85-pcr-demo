# TiKV raftstore-v2 内存消耗分析

**分析时间**: 2026-05-02

---

## 问题

PCR 测试中目标 TiKV（raftstore-v2 / partitioned-raft-kv）频繁 OOM 崩溃，而源 TiKV（raft-kv v1）稳定运行。需理解根因并找到可用的开发测试配置。

---

## 崩溃时内存消耗

测试环境：MacBook 16GB RAM，同时运行源 PD + 目标 PD + 源 TiKV + 目标 TiKV + TiDB + Prometheus + Grafana。

```
默认 block-cache:          16GB × 0.45 = 7,200 MB
write-buffer (default CF):  128MB × 5 =   640 MB
write-buffer (write CF):    128MB × 5 =   640 MB
write-buffer (lock CF):      32MB × 5 =   160 MB
write-buffer (raft CF):     128MB × 5 =   640 MB
RocksDB index/filter:                   ~  500 MB
memtable + 其他:                         ~  300 MB
OS + 后台进程:                           ~2,000 MB
───────────────────────────────────────────────
默认配置总计:                            ~12 GB+
```

16GB 减去系统/IDE/浏览器（~3GB）和其他进程（~6GB），可用 < 4GB，12GB 需求远超可用。

---

## raftstore-v2 与 raft-kv v1 内存差异

| 维度 | raft-kv (v1) | raftstore-v2 |
|------|-------------|-------------|
| RocksDB 实例 | 1 个共享 KV DB | **每 Tablet 独立 RocksDB** |
| Memtable 内存 | 1 套 × 4 CFs | N 套 × 4 CFs（N = tablet 数） |
| Block cache | 1 个全局共享 | 共享但有 per-tablet 碎片开销 |
| 文件描述符 | 1 套 SST/WAL 文件 | 每个 tablet 独立 SST/WAL |
| 额外结构 | 无 | TabletRegistry, TabletSingleton, per-tablet Raft 状态 |

**根因**：每个 Tablet 是独立 RocksDB 实例。默认参数下，每 tablet 仅 memtable 就需 4(CF) × 128MB × 5 = 2.5GB。5 个 tablet = 12.5GB 仅 memtable。

raftstore-v2 本身设计合理，但假设生产环境 32GB+ 内存。在 16GB Mac 上同时跑源+目标两个 TiKV，默认配置必然 OOM。

---

## 默认配置 vs 勉强可用配置

| 参数 | 默认 | 勉强可用 | 缩减倍率 |
|------|------|---------|---------|
| `block-cache.capacity` | 7,200 MB (45% RAM) | 64 MB | ×1/115 |
| `defaultcf.write-buffer-size` | 128 MB | 8 MB | ×1/16 |
| `defaultcf.max-write-buffer-number` | 5 | 2 | ×1/2.5 |
| `writecf.write-buffer-size` | 128 MB | 8 MB | ×1/16 |
| `writecf.max-write-buffer-number` | 5 | 2 | ×1/2.5 |
| `lockcf.write-buffer-size` | 32 MB | 4 MB | ×1/8 |
| `lockcf.max-write-buffer-number` | 5 | 2 | ×1/2.5 |
| `max-open-files` | 40,960 | 512 | ×1/80 |
| `max-background-jobs` | 4 | 2 | ×1/2 |
| **估算总内存** | **~10 GB** | **~150 MB** | **×1/70** |

### 勉强可用配置（`/tmp/pcr-tgt-tikv.toml`）

```toml
[storage]
engine = "partitioned-raft-kv"
data-dir = "/tmp/pcr-tgt-tikv"
reserve-space = "0KiB"

[raftstore]
bootstrap-pre-split-count = 1
hibernate-regions = true

[pd]
endpoints = ["127.0.0.1:5381"]

[stream-ingest]
enable = true
source-address = "127.0.0.1:40162"
max-kv-buffer-size = "8MB"
min-flush-interval = "100ms"
checkpoint-interval = "10s"

[server]
addr = "127.0.0.1:40161"
status-addr = "127.0.0.1:20180"

[rocksdb]
wal-dir = ""
max-open-files = 512
max-background-jobs = 2

[rocksdb.defaultcf]
write-buffer-size = "8MiB"
max-write-buffer-number = 2

[rocksdb.writecf]
write-buffer-size = "8MiB"
max-write-buffer-number = 2

[rocksdb.lockcf]
write-buffer-size = "4MiB"
max-write-buffer-number = 2
```

### 实测内存占用（macOS, debug build）

| 进程 | RSS | 说明 |
|------|-----|------|
| 源 TiKV (raft-kv) | 127 MB | 默认 block-cache，单 RocksDB 实例 |
| 目标 TiKV (raftstore-v2) | 65 MB | 极小 block-cache，5 tablets |

debug build 下的 RSS 不代表 release build 的内存占用（debug 无 LTO 优化，内存布局更宽松），但相对比例可参考。

---

## 结论

1. raftstore-v2 不是「有 bug 导致内存泄漏」，而是 **per-tablet RocksDB 实例天然需要更多内存**
2. 缩减 block-cache（7.2GB → 64MB）是降低内存的最大杠杆
3. 缩减 write-buffer（128MB×5 → 8MB×2）解决 per-tablet 的 memtable 堆积
4. 开发测试用极小配置即可，生产环境需 ≥32GB
