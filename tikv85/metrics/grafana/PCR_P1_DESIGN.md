# P1 可观测性设计文档

## 概述

对标 CRDB 的 commit_latency / flush_hist_nanos / per-CF breakdown，为 TiKV PCR 补齐延迟分析和吞吐分解能力。

## 设计

### 1. 事件分发延迟 `pcr_dispatch_latency_seconds`

**类型**：Histogram (0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)

**上报点**：`task.rs` 事件循环，`handle_event()` 前后打点

**语义**：channel 收到事件 → handle 完成的时间。不含 channel 排队时间。

**Grafana**：P50/P95/P99 三线图

### 2. SST 分阶段延迟

三个独立 Histogram，拆分 `pcr_flush_latency_seconds`：

| 指标 | 阶段 | 操作 | bucket |
|------|------|------|--------|
| `pcr_sst_sort_latency_seconds` | sort | KV 按 key 排序 | 0.001~0.5 |
| `pcr_sst_generate_latency_seconds` | generate | SST writer 创建→finish_read→序列化 | 0.001~0.5 |
| `pcr_sst_ingest_latency_seconds` | ingest | RocksDB ingest_external_file | 0.001~0.5 |

**上报点**：`sst_batcher.rs` flush()，三个 `Instant::now()` 分别打点

**计算公式**：sort + generate + ingest ≈ flush_latency

### 3. CF 级别吞吐分解

| 指标 | 类型 | label |
|------|------|-------|
| `pcr_cf_ingested_bytes_total` | IntCounterVec | cf=default/write/lock |
| `pcr_cf_ingested_kvs_total` | IntCounterVec | cf=default/write/lock |

**上报点**：`sst_batcher.rs` flush()，每个 CF 的 SST ingest 后累加

**用途**：
- 识别哪类 CF 数据占吞吐大头
- 正常：default CF > write CF >> lock CF
- 异常：lock CF 占比过高 → 源端锁冲突严重

## 数据流

```
Consumer event loop                SstBatcher.flush()
       │                                  │
  dispatch_start                     t_sort = now()
       │                             kvs.sort()
  handle_event()                     sort_latency.observe()
       │                                  │
  dispatch_end                       t_gen = now()
  dispatch_latency.observe()         SST writer → finish_read()
                                     generate_latency.observe()
                                          │
                                     t_ingest = now()
                                     ingest_external_file()
                                     ingest_latency.observe()
                                          │
                                     cf_ingested_bytes{cf}.inc()
                                     cf_ingested_kvs{cf}.inc()
```

## Grafana 面板

| 面板 | PromQL | 说明 |
|------|--------|------|
| Dispatch Latency | `histogram_quantile(0.50/0.95/0.99, rate(bucket[2m]))` | P50/P95/P99 |
| SST Phased Latency | `histogram_quantile(0.50, rate(bucket[2m]))` | sort/generate/ingest 三线 |
| Ingest Bytes by CF | `rate(pcr_cf_ingested_bytes_total[2m])` | default/write/lock 三线 |
