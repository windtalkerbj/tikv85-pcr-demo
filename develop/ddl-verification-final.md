# PCR DDL 复制问题 — 最终验证报告

**日期**: 2026-05-09
**结论**: 已解决。不需要修改 TiDB 或 PD 代码。

## 根因

PCR 全量扫描只扫了 `default` CF，没有扫 `write` CF。TiDB 的 MVCC 读取先去 WRITE CF 查找 commit 记录 → 找不到 → 返回 nil → `SchemaVersionKey=0` → TiDB 认为全新集群 → 执行 bootstrap 覆盖 PCR 数据。

## 修复

| 文件 | 改动 |
|------|------|
| `components/cdc/src/pcr_snapshot.rs` | 新增 `scan_write_cf_raw()` 函数（+38 行） |
| `components/cdc/src/pcr_service.rs` | 全量阶段同时扫描 default CF + write CF（~15 行） |
| `components/stream-ingest/src/task.rs` | 状态机：Activate 接受 Subscribing；Create 接受 Activated/Completed（~6 行） |

**TiDB 改动：0 行**
**PD 改动：0 行**

Consumer 端不需要改动——已有按 CF 分别 ingest 的完整链路：
`event.cf → MvccKeyValue.cf → SstBatcher 按 CF 分组 → DirectIngest.ingest_sst(cf)`

## 端到端验证

### 测试场景

```
Phase 1: DDL1 (CREATE TABLE pcr.t1) + DML1 (INSERT 1000 rows)
Phase 2: DDL2 (CREATE TABLE pcr.t2) + DML2 (INSERT 1000 rows)
Phase 3: 启动 PCR（全量扫描，同时复制 default CF + write CF）
Phase 4: zm hash 匹配 + zt hash 匹配 → Activate
Phase 5: 目标 TiDB 启动 → SQL 查询验证
```

### 验证结果

```
Target TiDB :4001:
  SHOW DATABASES → pcr ✓
  SELECT * FROM pcr.t1 → 1000 rows, SUM(id)=500500 ✓
  SELECT * FROM pcr.t2 → 1000 rows, SUM(id)=500500 ✓

Source TiDB :4000:
  SELECT * FROM pcr.t1 → 1000 rows, SUM(id)=500500 ✓
  SELECT * FROM pcr.t2 → 1000 rows, SUM(id)=500500 ✓

数据 100% 一致
```

## 数据流（修复后）

```
Source TiKV (CDC full scan):
  scan_default_cf → DEFAULT CF 数据 → cf="default"
  scan_write_cf_raw → WRITE CF 数据 → cf="write"
    ↓ gRPC PcrStream
Consumer (SstBatcher → DirectIngest):
  cf="default" → ingest to RocksDB DEFAULT CF
  cf="write" → ingest to RocksDB WRITE CF
    ↓
Target TiDB MVCC 读:
  WRITE CF 找到 commit 记录 ✓
  → 指向 DEFAULT CF 的值 ✓
  → SchemaVersionKey=57 ✓
  → 不触发 bootstrap ✓
  → 加载全部 PCR 复制的 schema + 数据 ✓
```

## 关键经验

1. **zm hash 匹配 ≠ 同步完成**。DDL meta 在 region 2（先扫描），表数据在后面。
   需要同时检查 zt hash 确认数据到位。
2. **重启 TiKV 必须同时重启 PD**，否则 cluster ID mismatch。
3. **TiDB 本身没问题**——它的 MVCC 读取逻辑完全正确，问题始终在 PCR 侧。
