# PCR 架构重设计：双端 raft-v1 — 最终决策

> 2026-05-08，基于 ChatGPT 建议 + 用户 review 确认

## 决策汇总

| # | 决策 | 结论 |
|---|------|------|
| 1 | 目标引擎 | **raft-v1**（与源一致），删除 `engine = "raft-kv2"` |
| 2 | DirectIngest | **继续绕过 Raft**（`ingest_external_file_cf`），不经过 Raft apply |
| 3 | server2.rs | **删 stream-ingest 代码**，仅保留 server.rs（v1 路径） |
| 4 | server.rs | **去掉 `tablet_registry.is_some()` 守卫**，传 `engines.kv` |
| 5 | stream-ingest | **TabletRegistry → Arc<RocksEngine>**，全链路替换 |
| 6 | pcr-ctl | **不改**（HTTP API 层对 raft 版本透明） |
| 7 | Prometheus | **不改**（指标衡量数据流，非存储层） |
| 8 | 数据验证 | **新增 Target TiDB :4001** + `pcr_verify.py` SQL 对比 |
| 9 | 配置 | `develop/pcr-configs/pcr-tgt-tikv.toml` 删 `engine` 行 |

## 数据流（改后）

```
Source (raft-v1)                     Target (raft-v1)
TiDB :4000 → TiKV :20162              TiKV :20161 ← TiDB :4001 (NEW)
  │                                      │
  │ CDC observer → PcrService ───gRPC──▶ stream-ingest
  │                                      ├─ Arc<RocksEngine> (was TabletRegistry)
  │                                      ├─ DirectIngest (still bypass Raft)
  │                                      └─ HTTP :20190
  │                                               │
  └── SELECT COUNT(*) ──── 对比 ──── SELECT COUNT(*) ──┘
```

## 代码改动清单

| 文件 | 改动 | 行数 |
|------|------|------|
| `stream-ingest/src/direct_ingest.rs` | `TabletRegistry<E>` → `Arc<E>` | ~15 |
| `stream-ingest/src/task.rs` | 同上 | ~10 |
| `stream-ingest/src/lib.rs` | `create_stream_ingest_task` 签名 | ~5 |
| `stream-ingest/src/sst_batcher.rs` | 删除 `TabletRegistry` import | 1 |
| `server/src/server.rs` | 删守卫，传 `engines.kv` | ~10 |
| `server/src/server2.rs` | 删 stream-ingest 代码块 | ~60 |

## PCR 完整功能矩阵

| 层级 | 功能 | 状态 |
|------|------|------|
| P0 | 去 TabletRegistry + v1 路径 | 🔨 本次实施 |
| P0 | 全量 + 增量回归 | 🔨 本次测试 |
| P0 | Bounded scan 多 Region 验证 | 🔨 本次测试 |
| P1 | PcrSstChunk（Lightning 导入） | 后续 |
| P1 | PcrDeleteRange（DROP TABLE） | 后续 |
| P1 | Cutover / Activate E2E | 后续 |
| P2 | 数据验证 Target TiDB :4001 | 后续 |
| P2 | 多节点集群、checksum | 后续 |
