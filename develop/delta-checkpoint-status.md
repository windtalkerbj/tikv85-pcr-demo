# PCR Delta / 断点续传 当前状态 (2026-05-09)

## 已验证完成

| 功能 | 状态 | 说明 |
|------|------|------|
| 全量同步 | ✅ | zm+zt hash 一致 |
| DDL 复制 | ✅ | TiDB 无需 CREATE DATABASE/TABLE |
| SQL 查询 | ✅ | COUNT/SUM 100% 一致 |
| 状态机修复 | ✅ | activate 直接从 Subscribing；create 从 Activated |

## Delta 断点续传：进行中

### 已修复的 bug
- 全量扫描遗漏 WRITE CF → 加了 `scan_write_cf_raw`（✅ 已验证）
- Delta 扫描遗漏 WRITE CF → 加了 `scan_delta_entries`（⚠️ 代码 ok，未跑通验证）

### 遗留问题
- 反复重启集群导致的 PD cluster ID mismatch
- `scan_delta_entries` 返回 raw WRITE CF + raw DEFAULT CF，理论正确但未端到端验证

## 代码改动汇总

| 文件 | 改动 | 状态 |
|------|------|------|
| `pcr_snapshot.rs` | +`scan_write_cf_raw()`, +`scan_write_cf_raw_since()`, +`scan_delta_entries()` | 编译通过 |
| `pcr_service.rs` | 全量+delta 都双 CF 扫描 | 编译通过 |
| `task.rs` | 状态机修复 | ✅ 已验证 |
| TiDB | 0 行 | ✅ |
| PD | 0 行 | ✅ |

## 下一步

1. 端到端验证 delta 断点续传（清理环境，一次跑完不重启）
2. 如果 delta DEFAULT CF key 格式仍有问题，微调 `scan_delta_entries`
3. TPCC 类大批量建表测试
