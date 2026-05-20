# DeleteRange / TRUNCATE / DROP TABLE 分析

## 结论

TiDB v8.5 的 TRUNCATE/DROP TABLE **不使用 UnsafeDestroyRange gRPC**。

### TiDB 实际流程

1. TRUNCATE = DROP old table + CREATE new table（新 table_id）
2. DROP TABLE = 标记表删除，old table_id 的数据延迟 GC
3. 数据清理通过 GC worker 的 `UnsafeDestroyRange` 在后台执行
4. 这个 GC 清理时机可能晚几小时甚至更久

### 对 PCR 的影响

- **DDL meta 复制** → 目标 TiDB 知道表被 TRUNCATE/DROP ✅ 已覆盖
- **数据清理** → 源端 GC 时间不确定，目标端孤儿 KV 无影响
- **当前 gRPC 拦截代码** → 架构正确但触发器缺失（GC 触发非 TRUNCATE 操作本身）
- **raftstore DeleteRange 注入** → 正确但 TiDB v8.5 在 TRUNCATE 时也不走这个路径

### 保存价值

- `raftstore/coprocessor/mod.rs`: CmdBatch.delete_ranges 字段
- `raftstore/apply.rs`: DeleteRange → CmdBatch 转发
- `src/server/service/kv.rs`: UnsafeDestroyRange gRPC 拦截
- `cdc/src/pcr_service.rs`: event_sinks 广播机制
- `cdc/src/delegate.rs`: delete_ranges 处理
- `stream-ingest/src/direct_ingest.rs`: apply_delete_range

这 6 个文件的改动体现了对 TiKV 内部机制的深入理解，后续遇到真正需要这些路径的场景可直接复用。
