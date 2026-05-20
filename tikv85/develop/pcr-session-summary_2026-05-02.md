# PCR 开发 Session 总结

**日期**: 2026-05-02

---

## 完成修复（11 项）

### 1. 硬编码 PD 地址修复（3 处）

| 文件 | 问题 | 修复 |
|------|------|------|
| `task.rs:634` | `discover_region_ids_from_pd` 硬编码 `127.0.0.1:2379` | 改为 `self.source_pd` 动态配置 |
| `task.rs:254` | `discover_region_ids_from_source` 同上硬编码 | 参数化为 `source_pd: &str` |
| `task.rs:63` | `PcrComponents` 缺少 `source_pd` 字段 | 新增 `source_pd: String` |

### 2. source_pd 状态传递（3 处）

| 文件 | 问题 | 修复 |
|------|------|------|
| `task.rs:510` | `StreamIngestTask` 未存储 `source_pd` | 新增 `source_pd: Option<String>` |
| `task.rs:670` | `spawn_event_loop` 中 `components.take()` 后 source_pd 为空 | take 后注入 `self.source_pd` |
| `task.rs:384` | 周期性 rediscover 使用 `source_addr` 而非 PD | 改用 `components.source_pd` |

### 3. write CF delta scan 正确性（3 处）

| 文件 | 问题 | 修复 |
|------|------|------|
| `pcr_snapshot.rs:164` | write CF 迭代顺序 ≠ user_key 顺序 | `results.sort_by()` |
| `pcr_snapshot.rs:180-200` | write CF value 未经 `WriteRef::parse` 解码，long value 拿到的是指针而非真实数据 | `WriteRef::parse` → short_value 直接用，long value 通过 `Key::append_ts` 构造 default CF key 查找 |
| `pcr_service.rs:143` | delta scan KVs 被标记为 CF="write"，但 key 是去掉了 MVCC 后缀的 user_key，与 write CF 格式不兼容 | 统一改为 CF="default" |

### 4. Checkpoint 持久化（1 处）

| 文件 | 问题 | 修复 |
|------|------|------|
| `task.rs:447-465` | pause 时 event loop 直接退出，不保存 checkpoint | Graceful shutdown: flush batcher → `record_checkpoint()` → 退出 |

### 5. 并发控制（1 处）

| 文件 | 问题 | 修复 |
|------|------|------|
| `pcr_service.rs` | 65 个 `spawn_blocking` 同时打开 RocksDB secondary → I/O 饱和 | `OnceLock<Semaphore>` 限制最多 4 个并发 scan |

---

## 验证结果

- **Region 发现**: 5 → 65/79 ✅
- **SST 排序错误**: 119 → **0** ✅
- **Checkpoint-on-pause**: 325 regions 成功持久化，min_ts 正确 ✅
- **start_ts > 0**: Subscribe 请求携带 checkpoint 时间戳 ✅
- **WriteRef 解码**: short_value / default CF lookup 两种路径正确 ✅
- **目标 TiKV raftstore-v2 稳定性**: 16MB write-buffer × 3 可稳定运行，8MB 以下触发 OOM（详见 `tikv-raftstore-v2-memory-analysis_2026-05-02.md`）

---

## 已知遗留

- **SIGSEGV in DirectIngest**: 目标 TiKV 在 `handle_event → flush_and_record → SstBatcher::flush → DirectIngest::ingest_sst` 路径上偶发 SIGSEGV，`faultingThread=80`。Crash 地址 `0x76656c2d74612d73` 为 ASCII 字符串（疑似 VALUE 数据被误当作指针）。此 bug 在今晚的代码改动之前已存在（最早的 crash report 标记在 pcr_snapshot.rs 重写之前），待独立排查。

---

## 架构文档

- `develop/pause-gap-solution-comparison_2026-05-02.md` — CRDB vs 保持连接两种 pause-gap 方案对比
- `develop/tikv-raftstore-v2-memory-analysis_2026-05-02.md` — raftstore-v2 内存消耗分析
- `develop/pcr-spawn-blocking-thread-pool-analysis_2026-05-02.md` — spawn_blocking 线程池阻塞方案对比
