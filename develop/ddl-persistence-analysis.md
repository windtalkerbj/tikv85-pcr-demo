# TiDB DDL 持久化路径分析

基于官方源码（offcial-tikv-8.5.6 / pd-8.5.6 / offcial-tidb-8.5.6，均未修改）。

## 一、DDL Schema 元数据存储在 TiKV，不是 PD

### 1.1 PD 不存 Schema

PD etcd 中的所有 key path（`pd-8.5.6/pkg/utils/keypath/key_path.go`）：

| 数据类别 | etcd 路径 |
|---------|-----------|
| 集群拓扑 | `raft/s/{store_id}`, `raft/r/{region_id}` |
| ID 分配器 | `alloc_id`（只分配 ID，不记录用途） |
| 配置 | `config` |
| GC | `gc/safe_point` |
| Global Config | `/global/config/{name}`（CDC 配置用） |
| MetaStorage 透传 | `meta_storage/{key}`（TiDB 未用于 schema） |

PD 中没有 `schema`、`ddl`、`table`、`database`、`column` 等路径。

### 1.2 TiDB 的 meta Mutator 直接写 TiKV

**`offcial-tidb-8.5.6/pkg/meta/meta.go:189-211`**：
```go
func NewMutator(txn kv.Transaction, options ...Option) *Mutator {
    t := structure.NewStructure(txn, txn, mMetaPrefix)  // mMetaPrefix = "m"
    m := &Mutator{txn: t, StartTS: txn.StartTS(), ...}
}
```

**`offcial-tidb-8.5.6/pkg/meta/meta.go:699-721`** — CREATE TABLE：
```go
func (m *Mutator) CreateTableOrView(dbID int64, tableInfo *model.TableInfo) error {
    data, err := json.Marshal(tableInfo)
    if err := m.txn.HSet(dbKey, tableKey, data); err != nil { ... }
}
```

**`offcial-tidb-8.5.6/pkg/meta/meta.go:664-680`** — CREATE DATABASE：
```go
func (m *Mutator) CreateDatabase(dbInfo *model.DBInfo) error {
    data, err := json.Marshal(dbInfo)
    if err := m.txn.HSet(mDBs, dbKey, data); err != nil { ... }
}
```

### 1.3 完整的 DDL 执行流

```
Client SQL (CREATE TABLE t1 ...)
  → DDL Executor (pkg/ddl/executor.go)
  → Job Submitter (pkg/ddl/job_submitter.go)
      → meta.Mutator.GenGlobalIDs() → TiKV key "m"/"NextGlobalID"
  → DDL Owner Worker (pkg/ddl/job_worker.go)
      → w.sess.Begin() → TiKV 事务
      → metaMut = meta.NewMutator(txn)  // 包裹 TiKV 事务
      → CreateTableOrView(schemaID, tbInfo)
          → JSON Marshal TableInfo
          → txn.HSet("DB:{id}", "Table:{id}", JSON)
      → w.sess.Commit() → 2PC → TiKV
```

## 二、Meta Key 编码结构

### 2.1 前缀

所有 meta key 以 `m`（0x6D）开头（`mMetaPrefix = []byte("m")`）。

### 2.2 Key 类型

| 逻辑 Key | Type Flag | 物理结构 |
|----------|-----------|----------|
| `"NextGlobalID"` | `'s'` (StringData) | `m` + codec("NextGlobalID") + uint64('s') |
| `"SchemaVersionKey"` | `'s'` | `m` + codec("SchemaVersionKey") + uint64('s') |
| `"BootstrapKey"` | `'s'` | `m` + codec("BootstrapKey") + uint64('s') |
| `"DBs"` | `'h'` (HashData) | `m` + codec("DBs") + uint64('h') + codec(field) |
| `"DB:{id}"` | `'h'` | `m` + codec("DB:{id}") + uint64('h') + codec(field) |
| `"DDLJobList"` | `'l'` (ListData) | `m` + codec("DDLJobList") + uint64('l') |
| `"DDLJobHistory"` | `'h'` | `m` + codec("DDLJobHistory") + uint64('h') + codec(jobID) |

### 2.3 Hash 结构

```
m + codec("DBs") + 'h' + codec("DB:1")  → JSON(DBInfo)
m + codec("DBs") + 'h' + codec("DB:2")  → JSON(DBInfo)

m + codec("DB:1") + 'h' + codec("Table:10")  → JSON(TableInfo)
m + codec("DB:1") + 'h' + codec("Table:11")  → JSON(TableInfo)
m + codec("DB:1") + 'h' + codec("TID:10")    → int64 (auto row ID)
m + codec("DB:1") + 'h' + codec("IID:10")    → int64 (auto_increment)
```

### 2.4 Key 编码函数（`pkg/structure/type.go:89-95`）

```go
func (t *TxStructure) encodeHashDataKey(key []byte, field []byte) kv.Key {
    ek = append(ek, t.prefix...)           // "m"
    ek = codec.EncodeBytes(ek, key)         // e.g. "DB:1"
    ek = codec.EncodeUint(ek, uint64(HashData)) // 'h'
    return codec.EncodeBytes(ek, field)     // e.g. "Table:10"
}
```

## 三、TiKV 端的观察能力（原始 CDC，未修改）

### 3.1 CDC 只观察 CmdType::Put

**`offcial-tikv-8.5.6/components/cdc/src/delegate.rs:879-884`**：
```rust
for mut req in requests {
    match req.get_cmd_type() {
        CmdType::Put => self.sink_put(req.take_put(), &mut rows_builder)?,
        _ => debug!("cdc skip other command"; ...),
    }
}
```

### 3.2 DDL 相关操作的 CDC 可见性

| DDL 操作 | KV 写入路径 | CDC 可见？ |
|---------|-----------|-----------|
| CREATE TABLE/DATABASE（meta key） | CmdType::Put | ✅ 是 |
| INSERT/UPDATE/DELETE（用户数据） | CmdType::Put | ✅ 是 |
| CREATE INDEX（回填） | CmdType::Put + txn_source | ⚠️ 被 is_lossy_ddl_reorg_source_set 过滤 |
| ADD INDEX（Lightning 快速路径） | CmdType::IngestSst | ❌ 否 |
| DROP TABLE | CmdType::DeleteRange | ❌ 否 |
| TRUNCATE TABLE | CmdType::DeleteRange | ❌ 否 |
| BR/Lightning 导入 | CmdType::IngestSst | ❌ 否 |

### 3.3 DDL 过滤逻辑（`offcial-tikv-8.5.6/components/cdc/src/delegate.rs:827-831`）

```rust
if TxnSource::is_lossy_ddl_reorg_source_set(row.txn_source) { continue; }
if filter_loop && TxnSource::is_cdc_write_source_set(row.txn_source) { continue; }
```

## 四、推论

1. **DDL Schema 元数据存储在 TiKV RocksDB**，作为普通 Percolator 事务（`CmdType::Put`），key 前缀为 `m`
2. **PCR 的 CDC Producer 理论上应该已经能捕获 meta key 的写入**（因为它们是 `CmdType::Put`）
3. **PCR 全量扫描理论上应该已经扫描到 meta key**（它们在 default CF 中，与其他 key 一样）
4. **目标 TiDB 无法使用复制的 schema**，原因可能是：
   - a) 自举覆盖：目标 TiDB 首次启动时自己初始化 meta key，与复制数据冲突
   - b) Schema version 不一致：目标 PD 中的 schema version 与目标 TiKV 中复制的 schema version 冲突
   - c) 全量扫描范围未覆盖 meta key 所在的 region
5. **CDC 无法捕获的操作**（原版 TiKV 未修改）：`IngestSst`、`DeleteRange`
   - 已修改 PCR 版 CDC 增加了这两者的处理
