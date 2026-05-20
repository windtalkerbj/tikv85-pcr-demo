# PCR 未实现功能清单

## P0 — 影响数据完整性

| 功能 | 说明 | 状态 |
|------|------|------|
| **PcrSstChunk** | Lightning/BR 批量导入的 SST 数据复制 | ❌ 未实现 |
| **PcrDeleteRange** | DROP TABLE / TRUNCATE 等范围删除 | ❌ 未实现 |

## P1 — 生命周期

| 功能 | 说明 | 状态 |
|------|------|------|
| **Cutover** | 切换目标集群为可读写 | ⚠️ 代码存在，未端到端测试 |
| **Activate** | 激活目标集群 | ⚠️ 代码存在，未端到端测试 |
| **数据一致性校验** | checksum 跨集群验证 | ❌ 未实现 |

## P2 — 工程化

| 功能 | 说明 | 状态 |
|------|------|------|
| **多节点集群** | 3 TiKV + 3 PD 真实部署测试 | ❌ 未测 |
| **单元测试 (CDC 侧)** | pcr_snapshot、pcr_service 测试 | ⚠️ 仅有 stream-ingest 侧 60 tests |
| **pcr-ctl 完整 CLI** | status/list/detail 等子命令 | ⚠️ 部分可用 |
| **RocksDB upper_bound** | 写 CF comparator 兼容性修复 | ⚠️ 已知限制，由代码级过滤替代 |
