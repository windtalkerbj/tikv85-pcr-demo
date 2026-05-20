# PCR 架构重设计：双端 raft-v1

> 基于 ChatGPT 建议：源和目标统一使用 raft-v1，去掉 raftstore-v2 依赖

---

## 一、架构对比

### 改前（当前实现）

```
Source (raft-v1)                    Target (raft-v2 + stream-ingest)
┌─────────────────┐                ┌──────────────────────────────┐
│ TiDB :4000       │                │ ❌ 无 TiDB（v2 不服务 SQL）    │
│ PD :2379         │                │ PD :2380                      │
│ TiKV :20162      │                │ TiKV :20161(engine=raft-kv2) │
│   CDC → PcrService│   gRPC         │   stream-ingest              │
│                  │ ═══════════════▶│     ├─ TabletRegistry(v2)    │
│                  │                │     ├─ DirectIngest           │
│                  │                │     └─ HTTP :20190            │
└─────────────────┘                └──────────────────────────────┘
```

### 改后（双 v1）

```
Source (raft-v1)                    Target (raft-v1 + stream-ingest)
┌─────────────────┐                ┌──────────────────────────────┐
│ TiDB :4000       │                │ TiDB :4001  ← 新增！可做 SQL  │
│ PD :2379         │                │ PD :2380                      │
│ TiKV :20162      │                │ TiKV :20161(engine=raft-kv)  │
│   CDC → PcrService│   gRPC         │   stream-ingest              │
│                  │ ═══════════════▶│     ├─ Arc<RocksEngine>(v1) │
│                  │                │     ├─ DirectIngest           │
│                  │                │     └─ HTTP :20190            │
│                  │                │                              │
│  SELECT COUNT(*) │                │  SELECT COUNT(*) ← 数据校验！ │
│  FROM pcr.t ─────┼──── 对比 ──────┼── FROM pcr.t                 │
└─────────────────┘                └──────────────────────────────┘
```

**关键变化**：
1. Target 去掉 `engine = "raft-kv2"`，恢复默认 raft-kv
2. stream-ingest 不再依赖 `TabletRegistry`，直接用 `Arc<RocksEngine>`
3. Target 集群可以挂 TiDB，通过 SQL 做数据一致性校验

---

## 二、代码改动

### 2.1 stream-ingest crate

```
改前：所有组件接收 Arc<TabletRegistry<E>>
改后：接收 Arc<E> (E: KvEngine)，直接操作 RocksDB

影响文件：
  components/stream-ingest/src/
    ├── direct_ingest.rs    ← tablet_registry → engine: Arc<E>
    ├── task.rs             ← 同上
    ├── lib.rs              ← create_stream_ingest_task 签名
    └── sst_batcher.rs      ← 去掉 TabletRegistry import
```

### 2.2 server.rs (v1 路径)

```rust
// 改前（line 993-997）
let stream_ingest_scheduler = if self.core.config.stream_ingest.enable
    && self.tablet_registry.is_some()  // ← v1 中 tablet_registry 永远是 None！
{
    let tablet_registry = self.tablet_registry.as_ref().unwrap().clone();
    // ...

// 改后
let stream_ingest_scheduler = if self.core.config.stream_ingest.enable {
    let engines = self.engines.as_ref().unwrap();
    let kv_engine = Arc::new(engines.engines.kv.clone());  // ← 直接用 RocksEngine
    let task = stream_ingest::create_stream_ingest_task(
        // ... kv_engine 替代 tablet_registry
    );
```

### 2.3 Target TiKV 配置

```toml
# develop/pcr-configs/pcr-tgt-tikv.toml
# 删除这两行：
# engine = "raft-kv2"        ← 删掉
# [stream-ingest] 不变
```

---

## 三、数据验证方案（新增能力）

### 3.1 验证架构

```
            ┌──────────────┐         ┌──────────────┐
            │ Source TiDB  │         │ Target TiDB  │
            │   :4000      │         │   :4001      │
            └──────┬───────┘         └──────┬───────┘
                   │                        │
            ┌──────┴───────┐         ┌──────┴───────┐
            │ Source TiKV  │  PCR    │ Target TiKV  │
            │   :20162     │ ═══════▶│   :20161     │
            └──────────────┘         └──────────────┘
```

### 3.2 验证 SQL

```sql
-- 1. 行数对比
-- Source:
SELECT COUNT(*) FROM pcr.t;
-- Target:
SELECT COUNT(*) FROM pcr.t;

-- 2. 数据抽样对比
-- Source:
SELECT k, v FROM pcr.t ORDER BY k LIMIT 100;
-- Target:
SELECT k, v FROM pcr.t ORDER BY k LIMIT 100;

-- 3. 聚合校验
-- Source:
SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM pcr.t;
-- Target:
SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM pcr.t;

-- 4. 随机抽样逐行对比
-- Source:
SELECT k, v FROM pcr.t WHERE k IN (SELECT k FROM pcr.t ORDER BY RAND() LIMIT 10);
-- Target:
SELECT k, v FROM pcr.t WHERE k IN (SELECT k FROM pcr.t ORDER BY RAND() LIMIT 10);
```

### 3.3 验证脚本（自动化）

```python
# tools/pcr_verify.py
def verify_table(db_name, table_name):
    src = mysql_query("127.0.0.1:4000", f"SELECT COUNT(*) FROM {db_name}.{table_name}")
    tgt = mysql_query("127.0.0.1:4001", f"SELECT COUNT(*) FROM {db_name}.{table_name}")
    assert src == tgt, f"Row count mismatch: {src} vs {tgt}"

    src_sum = mysql_query("127.0.0.1:4000", f"SELECT SUM(v) FROM {db_name}.{table_name}")
    tgt_sum = mysql_query("127.0.0.1:4001", f"SELECT SUM(v) FROM {db_name}.{table_name}")
    assert src_sum == tgt_sum, f"SUM mismatch: {src_sum} vs {tgt_sum}"
```

### 3.4 限制

- Target TiDB 只在 cutover 后允许写。cutover 前只能 SELECT
- Target TiDB 需要 TiKV coprocessor 支持（raft-v1 默认提供）
- PCR 复制的表在 target 上**对 TiDB 只读可见**（write protection 在 cutover 前阻止写）

---

## 四、部署拓扑

| 组件 | 端口 | 数据目录 | 配置 |
|------|------|---------|------|
| Source PD | 2379 | /tmp/pcr-src-pd | — |
| Target PD | 2380 | /tmp/pcr-tgt-pd | — |
| Source TiKV | 20162 | /tmp/pcr-src-data | pcr-src-tikv.toml |
| Target TiKV | 20161 | /tmp/pcr-tgt-data | pcr-tgt-tikv.toml (删 raft-kv2) |
| Source TiDB | 4000 | /tmp/pcr-tidb-src | — |
| Target TiDB | **4001** | /tmp/pcr-tidb-tgt | **新增** |
| Prometheus | 9091 | /tmp/pcr-prom-data | 采集两个 TiKV + 两个 TiDB |
| Grafana | 3001 | /tmp/pcr-grafana-data | — |
| PCR HTTP | 20190 | — | target TiKV 内部 |

---

## 五、实施计划

### Phase 1：去 v2（~2 天）

| 步骤 | 内容 | 预估 |
|------|------|------|
| 1 | stream-ingest: TabletRegistry → Arc<RocksEngine> | 2h |
| 2 | server.rs: 去掉 is_some 守卫，传 engine | 1h |
| 3 | 编译 + 修复类型错误 | 2h |
| 4 | target config 删 raft-kv2 | 5min |
| 5 | 全量 + 增量回归测试 | 半天 |

### Phase 2：数据验证（~1 天）

| 步骤 | 内容 | 预估 |
|------|------|------|
| 1 | 启动 Target TiDB :4001 | 1h |
| 2 | 编写 pcr_verify.py | 2h |
| 3 | 集成到测试流程 | 1h |

### Phase 3：回归 + 稳定性

| 步骤 | 内容 | 预估 |
|------|------|------|
| 1 | 50 DB scatter 测试 + SQL 验证 | 半天 |
| 2 | 多次 pause/resume 压测 | 半天 |
