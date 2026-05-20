# Consumer Parallel Ingest 设计方案 (Scheme A: Hash Dispatch)

日期：2026-05-19 | 状态：设计完成，待实施

## 问题

Consumer SstBatcher 单线程串行处理所有 region 的 KV：
- 全局 sort（O(N log N)，buffer 128MB ≈ 1M KVs）
- 串行 SST build + ingest（阻塞事件循环）
- TPCC 1仓全量扫描 62M KVs 需数十分钟

## 方案

Per-region worker dispatch：`region_id % N` 确定性路由。

```
event loop
  ↓ dispatch by region_id % N
  ↓
  ┌─ worker[0]: batcher → sort → SST → ingest
  ├─ worker[1]: batcher → sort → SST → ingest
  ├─ worker[2]: batcher → sort → SST → ingest
  └─ worker[3]: batcher → sort → SST → ingest
```

每个 worker 拥有独立的 `SstBatcher` + mpsc channel。同 region 始终同 worker，天然串行化，无并发 ingest 冲突。

## 改动

### PcrComponents

```rust
struct RegionWorker<E: KvEngine> {
    batcher: SstBatcher<E>,
    tx: tokio::sync::mpsc::UnboundedSender<PcrEventWithMeta>,
}

struct PcrComponents<E: KvEngine> {
    // 删除: batcher: SstBatcher<E>,
    // 新增:
    workers: Vec<RegionWorker<E>>,
    worker_count: usize,
    // ... 其余字段不变
}
```

### handle_event()

```rust
async fn handle_event(&mut self, event_with_meta: PcrEventWithMeta) -> Result<()> {
    let worker_idx = event_with_meta.region_id as usize % self.worker_count;
    let _ = self.workers[worker_idx].tx.send(event_with_meta);
    Ok(())
}
```

### Worker task

```rust
async fn run_worker<E: KvEngine>(
    mut batcher: SstBatcher<E>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<PcrEventWithMeta>,
    flush_interval_ms: u64,
) {
    while let Some(event_with_meta) = rx.recv().await {
        let region_id = event_with_meta.region_id;
        batcher.current_region_id = region_id;
        // ... 原 handle_event 的 KvBatch/SstChunk/Checkpoint 处理逻辑
        // time-based flush 检查
        // spawn_blocking(batcher.flush())
    }
}
```

### 初始化

```rust
const WORKER_COUNT: usize = 4;

let mut workers = Vec::with_capacity(WORKER_COUNT);
for _ in 0..WORKER_COUNT {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let batcher = SstBatcher::new(ingest_ctx.clone(), max_kv_buffer_size, max_range_buffer_size);
    workers.push(RegionWorker { batcher, tx });
    tokio::spawn(run_worker(batcher, rx, flush_interval_ms));
}
```

## 不改动的

- gRPC proto / PCR 协议
- SpanBridge
- Delegate / LogicalMutation
- PcrRegistry / auto_match_pcr
- DDL discover / schema_sync
- SstBatcher 内部实现

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| 同 region 并发 ingest | region_id % N 确定性路由，天然隔离 |
| 内存放大（N × 128MB） | N=4 时 ~512MB，Demo 机器可接受 |
| Worker 间负载不均 | TPCC 数据分布均匀，单仓无热点 |
| Checkpoint 一致性 | 保留在主 loop，各 worker 独立 flush |

## 预期收益

- 62M KVs 全量扫描：30min → 5-8min（5-6x）
- CPU 利用率：单核 → 多核

## 文件改动

| 文件 | 改动 | 行数 |
|------|------|------|
| `stream-ingest/src/task.rs` | RegionWorker + dispatch + spawn | +60 |
| 其他 | 无 | 0 |

## 被否方案

| 方案 | 原因 |
|------|------|
| B: spawn_blocking per flush | 共享 batcher &mut self 重新串行化，假并行 |
| C: snapshot/live split pipeline | 过早，需 resolved-ts 协调，引入 consistency 风险 |
