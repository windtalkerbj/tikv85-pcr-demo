# PCR 未完成事项（2026-05-12 更新）

## P0 — 全量扫描阻塞 Live CDC 延迟（非数据丢失）
- Test 1 DML 延迟 35s, Test 4 delta scan 60s, 全量扫描结束后延迟 5-8s
- 根因：全量扫描事件占据 gRPC stream

## P1 — DeleteRange P2-P5 残量测试
- 代码已实现，17 个单元测试已通过，待集成测试

## 已完成 ✓
- [x] Test 1: Pre-PCR 表全量扫描 + DML（✅）
- [x] Test 3: REPLACE INTO（✅ 8s）
- [x] Test 4: Pause+Resume delta scan（✅ 60s）
- [x] Test 6: DROP TABLE（✅ 15s）—— 首次验证通过
- [x] Live CDC 5 坑全部修复
- [x] B 方案 DDL catch-up
- [x] Span Control-Plane 6 Phase 编码

## P4 — 单元测试文件恢复
- unit_tests.rs 被 sed 操作截断损坏（损失 ~55 个 span tests）
- lib.rs 内联测试完整（5 tests 通过）
- 127 个核心测试通过

## P5 — 性能测试
- TPCC benchmark
- 大数据量场景（100K+ rows）

## P6 — Grafana 监控
- DML latency 面板
- Span Control-Plane 面板

## 已完成
- [x] live CDC 5 坑修复（Prewrite + z前缀 + LOCK CF + observer level + flush interval）
- [x] B方案 DDL catch-up（目标 TiDB 运行时可查新表）
- [x] Span Control-Plane 6 Phase 编码 + 测试
- [x] 基础 DML 实时复制（INSERT/UPDATE/DELETE/REPLACE）
