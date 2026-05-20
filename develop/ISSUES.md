# PCR 项目问题追踪

## 开放

### #7. Relay 批量化（性能）

10K INSERT 150s 收敛延迟。所有 delegate 共享一条 futures channel → relay 逐条 parse_from_bytes → tokio merge → gRPC。方向：relay 按 batch 聚合后再转发。

### #8. 大值 live CDC 路径（正确性）

WriteRef.short_value = None（>255B）时，live CDC 不复制 DEFAULT CF 值。需在 Commit 时从 RocksDB snapshot 读取 DEFAULT CF at start_ts。TPCC CUSTOMER.c_data(500B) 触发。

### #9. TRUNCATE / DeleteRange 旧数据清理

TiDB 清理链路（官方 TiKV 8.5.6 验证）：DDL → `mysql.gc_delete_range`（普通 INSERT，CDC 可复制 ✅）→ GC worker → `UnsafeDestroyRange` gRPC → `write_modifies()`（不走 Raft ❌）。生产级方案：cutover 后目标 TiDB GC worker 自然接管。

## 已解决（2026-05-18/19）

1. Split 后 child delegate 无 PCR sink → PcrRegistry + auto_match_pcr ✅
2. topo ticker starvation → SpanBridge per-region 管理删除 ✅
3. DELETE 可见性延迟 → LogicalMutation tombstone ✅
4. W级 split 验证 → 124 split, SRC=TGT ✅
5. DDL discover 轮询盲区 → MAX(job_id) trigger + cache diff + schema bump ✅
6. Prewrite Lock short_value → 删除 Lock CF 提取，committed MVCC only ✅
7. Write guard 阻塞 TiDB 重启 → `src/storage/mod.rs`：`!source.is_empty() && !source.starts_with("internal_")`，internal_* 放行，用户写拦截 ✅
8. Worker flush 残留数据丢失 → `run_ingest_worker` 改用 `tokio::select!` + 空闲超时 500ms flush + channel 关闭最终 flush。全量扫后无事件时缓冲 KV 不再丢失。TPCC 1仓 item/stock 从 62K/8K 恢复至 100K/100K ✅
