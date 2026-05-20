# PCR Pause-Gap 数据丢失解决方案对比

**讨论时间**: 2026-05-02

**问题**: PCR pause 后，暂停期间写入源集群的数据无法被 Consumer 捕获。

**根因**: Producer bridge 的 CDC 增量流只在订阅活跃时推送事件。Pause → 订阅断开 → pause 期间写入真空。Resume 时 start_ts > 0 跳过快照扫描，CDC 增量从「当前」开始，无法覆盖真空期数据。

---

## 方案 A：保持 gRPC 连接存活（已实现）

### 实现
- Pause 时不发 cancel，改发 watch channel pause 信号
- 事件循环 paused 分支不 poll `events.recv()`，事件积压在 unbounded channel
- Resume 时恢复处理，积压事件批量消费

### 结果
- 100K 行 pause 期写入：捕获 **~83%**（修复前 0%）
- 差距来源：源端 batcher 只在 `on_min_ts`(~1s) 检查 flush，pause 窗口内未 flush 的 Region 数据滞留

### 优点
- 实现简单，改动集中在 task.rs
- 短 pause 场景效果好
- 不需要修改 Producer 端

### 缺点
- Pause 期间占用 gRPC 连接（每 Region 一个）
- Unbounded channel 长 pause 可能内存压力
- 无法达到 100% 覆盖

---

## 方案 B：CRDB 全断开 + write CF MVCC delta scan（参考）

### CockroachDB 做法

CRDB 使用 `ALTER VIRTUAL CLUSTER PAUSE/REPLICATION`：

1. **Pause**: 取消作业上下文 → 断开所有 gRPC/PG 流 → Producer rangefeed 停止
2. **Resume**: 从 `system.jobs` 恢复 `replicatedTime` → 重建订阅 → Producer rangefeed 从该 timestamp 开始重放

核心：**Producer rangefeed 支持从任意历史 timestamp 高效增量回放**，自动覆盖暂停期间所有写入。Rangefeed 基于 Pebble LSM 的增量迭代器，只发变更，不发全量。

### TiKV 对应实现

1. **Pause**: 断开连接（当前 behavior，不需要保持连接）
2. **Save checkpoint**: 已有（PD safe point + local JSON）
3. **Resume**: 传递 start_ts = checkpoint min_ts（已有）
4. **Producer delta scan**（新模块）:
   - 当 start_ts > 0，扫描 **write CF**（非 DEFAULT CF）
   - 解析 MVCC 编码 key `{user_key}{inv_ts}`，过滤 `commit_ts > start_ts`
   - 发送扫描 KV 后进入 CDC 增量

### 优点
- 理论上 100% 覆盖暂停期数据
- Pause 期间零资源占用
- 与 CRDB 架构对齐
- 断点续传真正完整

### 缺点
- write CF MVCC delta scan 实现复杂：
  - MVCC key 格式 `{user_key}{!u64::MAX - commit_ts}` (big-endian)
  - 需正确解码并过滤
  - RocksDB secondary instance (`rocksdb_open_as_secondary`) FFI 需要扩展支持 write CF iterator
- 大暂停窗口（1TB+）扫描性能不可控
- Consumer 端 SST overwrite 去重，带宽浪费（重发已有数据）

---

## 对比表

| 维度 | 方案 A (保持连接) | 方案 B (CRDB 全断开 + delta scan) |
|------|-------------------|-----------------------------------|
| Pause 期数据覆盖 | ~83% | ~100%（理论） |
| 实现复杂度 | 低（已完成） | 高（新模块 ~200 行） |
| Pause 期资源占用 | gRPC 连接 + channel 内存 | 零 |
| Resume 速度 | 即时 | 取决于 pause 期数据量 |
| 大 pause 窗口 | 事件积压风险 | 慢但安全 |
| 依赖 | 无新依赖 | write CF RocksDB secondary FFI |

---

## 建议方案：分层策略

1. **短 pause (< 10 min)**: 方案 A（保持连接），即时恢复，83% 覆盖可接受
2. **长 pause (> 10 min)**: 方案 B（delta scan），确保 100% 覆盖
3. 在 `spawn_event_loop` 中判断 checkpoint age，自动选择策略

### 实现计划（方案 B）

1. `components/cdc/src/pcr_snapshot.rs`: 扩展 `SnapshotScanner` 支持 `scan_write_cf_since(ts: u64)`，返回 MVCC 解码后的 KV
2. `components/cdc/src/pcr_service.rs`: start_ts > 0 时调用 delta scan 替代 DEFAULT CF 快照
3. MVCC 解码工具函数: `decode_mvcc_key(key: &[u8]) -> (user_key, commit_ts)`

---

## CockroachDB 参考代码

- `stream_ingestion_job.go:298` — Resume() 入口
- `stream_ingestion_dist.go:47-57` — startDistIngestion, 从 job progress 读 replicatedTime
- `event_stream.go:104-189` — Producer eventStream.Start(), 无 initial scan 时直接从 PreviousReplicatedTimestamp 开始 rangefeed
- `partitioned_stream_client.go:448-484` — Subscribe() with previous replicated timestamp
- `stream_lifetime.go:178-224` — Producer PTS 保护未复制数据
