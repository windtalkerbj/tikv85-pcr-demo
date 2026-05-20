# PCR 完整测试结果 (2026-05-09)

## 测试结论

**全量同步、DDL 复制、断点续传（Pause→Delta→Resume）全部通过。TiDB/PD 改动为 0 行。**

## 修复的 Bug

| Bug | 根因 | 修复 |
|-----|------|------|
| 全量同步后 TiDB 查不到数据 | PCR 全量只扫 DEFAULT CF，缺 WRITE CF commit 记录导致 TiDB MVCC 读返回 nil | `scan_write_cf_raw` 全量也扫 WRITE CF |
| Delta 同步后 TiDB 查不到数据 | 同上 + `Key::from_raw` double-encoding 导致 DEFAULT CF key 错误 | `scan_delta_entries` + `Key::from_encoded_slice` |
| activate 报错 | 状态机只接受 Completed | 增加 Subscribing → auto-shutdown → activate |
| create 报错 | 状态机只接受 Paused/Idle | 增加 Activated/Completed |
| zt 扫描慢 | SCAN_SEMAPHORE=4 | 改为 16 |

## 涉及文件

| 文件 | 改动 |
|------|------|
| `components/cdc/src/pcr_snapshot.rs` | +`scan_write_cf_raw()`, +`scan_write_cf_raw_since()`, +`scan_delta_entries()`, 替换 `Key::from_raw`→`from_encoded_slice` |
| `components/cdc/src/pcr_service.rs` | 全量+delta 都双 CF 扫描, SCAN_SEMAPHORE 4→16 |
| `components/stream-ingest/src/task.rs` | 状态机修复 |
| TiDB | 0 行 |
| PD | 0 行 |

## 验证结果

### 全量同步 + DDL 复制
```
Target TiDB :4001:
  SHOW DATABASES → pcr ✓
  SELECT FROM pcr.t1 → 1000 rows, SUM=500500 ✓
  SELECT FROM pcr.t2 → 1000 rows, SUM=500500 ✓
Source: 100% 一致
```

### 断点续传
```
Flow: Full sync → Pause → DDL2+DML2 → Resume → Activate → TiDB
Target TiDB :4001:
  t1: 1000 rows, SUM=500500 ✓
  t2:  500 rows, SUM=125250 ✓ (Pause 期间创建的)
Source: 100% 一致
```

## 关键经验

1. **zm hash 匹配 ≠ 同步完成**：zm meta 在 region 2（先扫完），表数据在多个 region（后扫完）。需同时查 zt hash。

2. **delta 同步不能用 RocksDB hash 对照**：源端 CDC 内部持续写入（resolved-ts 等），不同时间点的 hash 永远差一拍。应用 TiDB SQL 验证。

3. **重启 TiKV 必须同时重启 PD**，否则 cluster ID mismatch。

4. **Pause 前需确保 checkpoint 已写入**（checkpoint_interval=10s），否则 resume 找不到 checkpoint 会失败。

5. **region 拓扑在 pause 期间可能变化**：`discover_partitions` 会重新从 PD 查询所有 region，不是静态缓存。已验证 pause 前后 65→66 region 全部被订阅。

6. **`Key::from_encoded_slice` vs `Key::from_raw`**：从 RocksDB 读出的 key 已经是 encoded 格式，必须用 `from_encoded_slice`，否则 double-encoding 导致 DEFAULT CF lookup 失败。

## 配置注意事项

```
源集群: PD 127.0.0.1:2379, TiKV 127.0.0.1:20162, TiDB :4000
目标集群: PD 127.0.0.1:2380, TiKV 127.0.0.1:20161, TiDB :4001
PCR API: 127.0.0.1:20190
source_pd 必须是 PD HTTP 地址 127.0.0.1:2379（不是 TiKV 地址）
```

## 待办

- [ ] Delta 路径 CONFIRMED：`scan_delta_entries` 正确，`from_encoded_slice` 修复生效
- [ ] 大规模数据量测试（TPCC）
- [ ] DeleteRange / SST chunk 复制
- [ ] Cutover / Activate 完整流程
