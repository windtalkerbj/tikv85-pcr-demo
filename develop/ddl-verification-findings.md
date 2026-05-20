# DDL 复制验证结论 (2026-05-08)

## 已验证

### PCR 侧：DDL 复制正常
- **Meta keys 完整复制**: Source 116 keys → Target 116 keys, hash 一致 `9957e7...`
- **DDLJobHistory 完整**: 目标 TiKV 中存在 `CREATE DATABASE pcr` 和 `CREATE TABLE pcr.t` 的完整 JSON
- **Table ID 一致**: 源和目标 `pcr.t` 的 TIDB_TABLE_ID 都是 112
- **WRITE CF + DEFAULT CF 都有数据**: 两个 CF 在源和目标端都有 zt 前缀 key
- **状态机修复验证通过**: activate 可直接从 Subscribing 执行; create 可接受 Activated/Completed 状态

### TiDB 侧：Schema 不可见
- 目标 TiDB 启动后 `SHOW DATABASES` 不包含 `pcr`
- TiDB bootstrap 将 meta keys 从 116 增加到 229（新增 113 个系统 key，非覆盖）
- `CREATE DATABASE IF NOT EXISTS pcr` 和 `CREATE TABLE IF NOT EXISTS pcr.t` 都会**实际执行 DDL job**（因 TiDB 内存中不认为表存在）
- `fast_create=true` 的 DDL 写入修改了目标 table key range，导致数据 hash 与源不一致
- 表定义正确 (`CREATE TABLE` 输出与源一致)，但 `SELECT *` 返回 0 rows

## 根因推断

TiDB 通过 DDL 子系统管理 schema。PCR 只复制了 RocksDB 上的 key-value 数据，但没有通过 TiDB 的 DDL 子系统注册 schema 变更。当目标 TiDB 启动时：

1. 读取 `BootstrapKey` → 发现已 bootstrap（PCR 复制的）
2. 跳过 system table 创建
3. **加载 schema cache** → 通过某种机制（可能是 PD 的 schema version 或 内存缓存）加载数据库列表
4. **未能发现 PCR 复制的 `pcr` 数据库**（DB:110 在 `DBs` hash 中存在，但 TiDB 不加载它）
5. `CREATE DATABASE IF NOT EXISTS pcr` → TiDB 认为不存在 → 执行真实 DDL
6. DDL 执行时可能**覆盖或标记删除**已有的 PCR 数据

## 待解决

1. TiDB 为什么不加载 `DBs` hash 中 PCR 复制的数据库条目？
2. `fast_create` DDL 是否破坏了 PCR 复制的表数据？
3. 能否通过 API 强制 TiDB 重新加载 schema（无需 CREATE DATABASE/TABLE）？

## 配置文件

源:
  PD: 127.0.0.1:2379, TiKV: 127.0.0.1:20162, data-dir=/tmp/pcr-src-data, TiDB: :4000

目标:
  PD: 127.0.0.1:2380, TiKV: 127.0.0.1:20161, data-dir=/tmp/pcr-tgt-data

PCR:
  source_pd 必须是 PD HTTP 地址 127.0.0.1:2379
  PCR HTTP control: 127.0.0.1:20190
