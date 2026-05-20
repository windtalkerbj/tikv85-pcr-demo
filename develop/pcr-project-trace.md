# PCR 项目完整追溯

## 架构决策
1. raft-v1 双端（2026-05-08）
2. DirectIngest 绕过 Raft
3. 不改 TiDB/PD — DDL 通过补全 WRITE CF 解决
4. SQL 验证替代 tikv-ctl

## 代码改动
### TiKV
- pcr_snapshot.rs: scan_write_cf_raw, scan_delta_entries, from_encoded_slice fix
- pcr_service.rs: 双 CF 扫描, SCAN_SEMAPHORE 4→16
- task.rs: 状态机修复
- direct_ingest.rs: TabletRegistry→Arc<RocksEngine>

### TiDB
- main.go: PCR 错误降级
- bootstrap.go: doReentrantDDL PCR 容忍
- session.go: finishBootstrap/start domain/init metadata lock 容忍

### PD: 0 行

## Bug 修复
1. 全量缺 WRITE CF → scan_write_cf_raw
2. Delta 缺 WRITE CF → scan_delta_entries  
3. DEFAULT CF key double-encoding → from_encoded_slice
4. activate 状态机 → 多状态
5. TiDB PCR 期间 FATAL → 3 文件降级
6. zt 慢 → Semaphore 16

## 配置
源: PD :2379, TiKV :20162, TiDB :4000
目标: PD :2380, TiKV :20161, TiDB :4001
PCR API: :20190
