# PCR 项目问题追踪

## 开放

### #7. Relay 批量化（Demo 接受）

10K INSERT 150s 收敛延迟。根因：单 relay task 串行 parse all delegate events。PcrEventBatcher 已具备 1MB/150ms 双阈值 delegate 端聚合，进一步优化（byte forwarding、multi-relay）对 Demo ROI 低。接受为 Demo 级延迟，文档标注。

### #8. 大值 live CDC 路径（正确性）

WriteRef.short_value = None（>255B）时，live CDC 不复制 DEFAULT CF 值。需在 Commit 时从 RocksDB snapshot 读取 DEFAULT CF at start_ts。TPCC CUSTOMER.c_data(500B) 触发。

### #9. TRUNCATE / DeleteRange 旧数据清理（架构边界，非 bug）

TRUNCATE/DROP 本质是 table_id rotate——旧 table_id 被废弃，新 table_id 创建。SQL 层旧数据已不可见，功能正确性不受影响。物理清理走 GC worker → UnsafeDestroyRange gRPC → write_modifies()，绕过 Raft，PCR observer 不可见。

PCR 不复制非 Raft 路径的 RocksDB maintenance operation。这是架构边界，不是 bug。目标 GC worker 因 TSO 域不同未必执行源端 GC job。Demo 接受旧数据残留（占空间、不影响查询）。可选增强：consumer 维护 dropped_table_ids tombstone metadata。

## 已解决

### 2026-05-20

9. scan_region 流式化 → 新增 `pcr_snapshot::stream_cf_full`，复用 `scan_default_cf` 相同 snapshot/iterator 逻辑，callback 替代 Vec 收集。scan_region 改为 `spawn_blocking` 内直接构建 1MB PcrKvBatch → blocking_send。内存 O(1MB)，5 仓可用 ✅
10. BufferFull 条件判断 bug → `should_flush(BufferFull)` 无条件返回 true，导致每 event 都立即 flush，64MB buffer 形同虚设。改为 `kv_buffer_size >= max_kv_buffer_size` ✅
11. gRPC writer panic → `sink.send()` 失败后再次 poll 已 consume 的 CqFuture，FATAL panic 崩 pcr-bridge 线程。改为 `break 'writer` 退出循环 ✅
12. Worker 真并行 → `worker_threads(1)→(4)`。单 OS thread 时 `flush()` 同步阻塞独占线程，4 worker 串行化。4 thread 后真并行 ✅
13. 5 仓 TPCC 收敛 → 284s (178s prepare + 106s PCR)，9/9 ✅
14. Consumer dispatch → `handle_data_event` 用 `AtomicU64` 自增实现 round-robin，span mode 下 region_id=0 时 4 worker 均匀分片 ✅
15. #8 大值 live CDC → WRITE CF commit 为唯一复制入口（移除 DEFAULT CF 直接复制，消除 phantom row 风险）。`short_value=None` 时通过 `old_value_cb` 回查 DEFAULT CF at start_ts。单条 INSERT 精确收敛，5 仓 benchmark 差 <0.002%。剩余极小 gap 来自 TSO 域不同（已知限制 #1） ✅

### 2026-05-18/19

1. Split 后 child delegate 无 PCR sink → PcrRegistry + auto_match_pcr ✅
2. topo ticker starvation → SpanBridge per-region 管理删除 ✅
3. DELETE 可见性延迟 → LogicalMutation tombstone ✅
4. W级 split 验证 → 124 split, SRC=TGT ✅
5. DDL discover 轮询盲区 → MAX(job_id) trigger + cache diff + schema bump ✅
6. Prewrite Lock short_value → 删除 Lock CF 提取，committed MVCC only ✅
7. Write guard 阻塞 TiDB 重启 → `src/storage/mod.rs`：`!source.is_empty() && !source.starts_with("internal_")`，internal_* 放行，用户写拦截 ✅
8. Worker flush 残留数据丢失 → `run_ingest_worker` 改用 `tokio::select!` + 空闲超时 500ms flush + channel 关闭最终 flush ✅
