# Snapshot 方案迁移总结

**日期**: 2026-05-05

---

## 迁移原因

共享 Secondary Instance 方案（`rocksdb_open_as_secondary_column_families` 1 次 + Arc 共享）在 E2E 测试中所有 delta scan 返回 0 KVs。根因为 RocksDB secondary 的 WAL tailing 在 macOS debug build 上不可靠，无法保证读到最新写入的数据。

## 新方案

用 `RocksSnapshot`（`engine_rocks` crate）替代 Secondary Instance C FFI。Snapshot 是纯内存中的读时间点，0 文件开销，即时一致。

## 改动

| 文件 | 改动 |
|------|------|
| `pcr_snapshot.rs` | 删掉 `SnapshotScanner` struct + C FFI + `unsafe impl Send/Sync`。改为 `scan_default_cf(&RocksEngine)` 和 `scan_write_cf_since(&RocksEngine)` 两个纯函数 |
| `pcr_service.rs` | `source_engine: Option<Arc<RocksEngine>>` 替代 `data_dir` + `shared_scanner` |
| `server.rs` | `Some(Arc::new(engines.engines.kv.clone()))` 传 engine |
| `server2.rs` | `None`（暂不需要） |
| `cdc/Cargo.toml` | 加 `rocksdb` 直接依赖 |

Snapshot 用 `engine.get_sync_db()` 获取 `Arc<rocksdb::DB>`，内部 `RocksSnapshot::new(db)` 创建快照，`iterator_opt(cf, opts)` 创建迭代器。

## 测试结果

| 指标 | 结果 |
|------|------|
| Delta scan | 111 regions, 23,311,443 KVs |
| Full scan | 0 |
| 0 KVs 错误 | 0（彻底解决） |
| SST 排序错误 | 31（23M 中 0.0001%，已知跨 region 排序问题） |
| unsafe 代码 | 0（全 safe Rust） |
| 文件开销 | 0 |

## 编译经验

- 用 `cargo check` 替代 `cargo build` 做类型检查——跳过 cmake C++ 编译，秒级完成
- 不要删 grpcio-sys 的 cmake 缓存
- `CMAKE_POLICY_VERSION_MINIMUM=3.5` 需内联传给 cargo（cmake 4.x 兼容）
