# B 方案：PCR DDL Catch-up 设计文档

## 目标

PCR 运行期间，目标 TiDB 可查询源端 DDL 创建的新表。不改 PD，最小改 TiDB。

## 核心洞察

源端 DDL 写入的所有 schema meta KV 已被 PCR 复制到目标 RocksDB：

- `m:SchemaVersionKey` — 最新 schema 版本号
- `m:Diff:{version}` — JSON SchemaDiff（含 CREATE/DROP TABLE 等变更描述）
- `m:DBs` / `m:DB:{id}` — 数据库和表列表
- `t:{tableID}_r:{rowID}` — 新表行数据

**问题不是数据没过来**——是目标 TiDB 的 Domain 没启动，`loadSchemaInLoop` 没在跑，内存中的 `information_schema` 不知道这些 KV 的存在。

## 当前行为

PCR 写保护开启时：
1. `ddl.StartOwnerManager()` 写 etcd 失败 → catch 为 warning
2. `session.BootstrapSession()` 写 TiKV 失败 → catch 为 warning，**dom 为 nil**
3. `createServer(storage, nil)` — TiDB 启动但无 schema 感知

## 设计

### 改动 1：`domain/domain.go` +6 行

新增公开方法：

```go
// StartSchemaLoad starts the schema loading loop without DDL ownership.
// Used in PCR read-only mode where writes are blocked but schema must
// stay current with replicated KV data.
func (do *Domain) StartSchemaLoad() {
    do.wg.Run(func() {
        do.loadSchemaInLoop(do.ctx)
    }, "loadSchemaInLoop")
}
```

### 改动 2：`cmd/tidb-server/main.go` +25 行

新增辅助函数：

```go
func createReadOnlyDomain(storage kv.Storage) *domain.Domain {
    cfg := config.GetGlobalConfig()
    schemaLease := time.Duration(cfg.Lease) * time.Millisecond
    factory := createSessionFactory()
    dom := domain.NewDomain(storage, schemaLease, 0, 0, factory)
    dom.Init(schemaLease, sysCtxPool)
    dom.StartSchemaLoad() // skip ddl.Start(), only schema loading
    return dom
}
```

修改 `createStoreDDLOwnerMgrAndDomain()` 的 PCR 错误分支：

```diff
  if strings.Contains(errStr, "PCR replication mode") ||
      strings.Contains(errStr, "writes not allowed") {
      log.Warn("PCR: bootstrap writes skipped, running read-only")
+     dom = createReadOnlyDomain(storage)  // was: dom=nil
  }
```

## 数据流

```
源端:  CREATE TABLE t_new → INSERT INTO t_new
         ↓ PCR KV 复制
目标 RocksDB: schema meta + table data 已写入
         ↓ loadSchemaInLoop (~22s interval)
目标 TiDB: information_schema 更新 → t_new 可见
         ↓
用户: SELECT * FROM t_new → 有数据 ✓
```

## 限制

- DDL 延迟：最多 schemaLease/2（默认 ~22s）
- 不执行的 DDL 类型：需要写操作的部分（表结构变更、索引创建等）——这些需要 DDL owner
- SCHEMA DIFF 覆盖：只需读取 `SchemaDiff` 即可知道 CREATE/DROP TABLE

## 测试

### 单元测试（`pkg/domain/domain_test.go`）
- `TestStartSchemaLoad`：goroutine 启动，不 panic
- `TestReadOnlyDomain`：createReadOnlyDomain 返回非 nil Domain

### 集成测试（端到端）
1. 启动 PCR 集群
2. 源端 CREATE TABLE + INSERT
3. 目标端等待 ~45s 后 SELECT → 新表可见

## 文件清单

| 文件 | 类型 | 改动 |
|------|------|------|
| `offcial-tidb-8.5.6/pkg/domain/domain.go` | 修改 | +6 行 `StartSchemaLoad()` |
| `offcial-tidb-8.5.6/cmd/tidb-server/main.go` | 修改 | +25 行 `createReadOnlyDomain()` + PCR 分支修改 |
| `offcial-tidb-8.5.6/pkg/domain/domain_test.go` | 修改 | +40 行 单元测试 |
