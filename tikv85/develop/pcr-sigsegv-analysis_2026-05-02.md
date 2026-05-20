# PCR DirectIngest SIGSEGV 分析

**分析时间**: 2026-05-02

---

## Crash 特征

- **进程**: 目标 TiKV（stream-ingest consumer）
- **线程**: thread 80（PCR event loop）
- **信号**: SIGSEGV (Segmentation fault: 11)
- **类型**: EXC_BAD_ACCESS, KERN_INVALID_ADDRESS
- **地址**: `0x76656c2d74612d73` → PAC 剥离后 `0x00006c2d74612d73`
- **地址特征**: ASCII 字符串 `l-ta-s`（被误当作指针）→ use-after-free
- **Apple 诊断**: "possible pointer authentication failure"

## Crash 调用链

```
run_event_loop
  → PcrComponents::handle_event (KvBatch)
    → PcrComponents::flush_and_record
      → SstBatcher::flush
        → DirectIngestContext::ingest_sst
          → tablet_engine.acquire_ingest_latch  (获取 latch)
          → tablet_engine.ingest_external_file_cf (RocksDB ingest)
          → drop(_latch)                          ← SIGSEGV HERE
            → RangeLatchGuard::drop
              → self.handle.range_latches.lock()  ← 野指针解引用
```

## 根因分析

### 1. RangeLatchGuard 的 unsafe transmute

`components/tikv_util/src/range_latch.rs:107`：

```rust
let mutex_guard = unsafe { std::mem::transmute(mutex_guard) };
```

将 `MutexGuard<'_, ()>` transmute 为 `MutexGuard<'static, ()>`，使其可以存储在 `RangeLatchGuard` 中。安全前提：`Arc<Mutex<()>>` 的生命周期覆盖 guard 的生命周期（通过 struct 字段声明顺序保证）。

这个前提在当前代码中成立（`handle: &RangeLatch` 的生命周期 < `Arc<Mutex>` 的生命周期），但极度脆弱——任何人修改字段顺序或所有权模型都可能导致 UB。

### 2. CachedTablet 引用链（已排除）

`direct_ingest.rs:134-141`：
```rust
let mut cached_tablet = self.tablet_registry.get(region_id)...;  // 持有值
let tablet_engine = cached_tablet.latest()...;                    // 引用
let _latch = tablet_engine.acquire_ingest_latch(...);             // 借用到引擎
```

`cached_tablet` 作为局部变量存活到函数结束，`tablet_engine` 和 `_latch` 在它之前 drop。**引用链不会悬垂**。

### 3. 真正的嫌疑：RocksDB ARM64 IngestExternalFile

`ingest_external_file_cf` 调用 RocksDB C++ 的 `IngestExternalFile`。在 ARM64 macOS (debug build) 上可能：
- 内部内存损坏，覆盖了 `RangeLatch` 的 `range_latches: Mutex<BTreeMap<...>>`
- RocksDB compaction 被 ingest 触发，与 latch 系统交互导致 double-free
- ARM64 特定代码路径（如 PAC、内存排序）在 debug build 中有 bug

这解释了：
- 为什么 crash 地址是 ASCII（被损坏的 mutex 内存区域后来被字符串数据复用）
- 为什么是小概率事件（取决于 ingest 触发的内部逻辑）
- 为什么往期 session 不崩（大 buffer → 少 flush → 少 ingest → 概率低）

### 4. 用户的 VALUE 直觉

用户怀疑与 VALUE 数据有关。部分正确：crash 地址的 ASCII 确实是数据字符串（`REPEAT(CHAR(...), 100)` 模式的片段）。但这不是 VALUE 本身的 bug——而是 VALUE 数据的内存被复用去覆盖了 latch 的 mutex。真正的问题是为什么 latch 的内存能被覆盖。

---

## 修复方案

### A. 短期：catch_unwind 保护

```rust
let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    tablet_engine.ingest_external_file_cf(cf, &[&tmp_path], None, true)
}));
```

不解决根因，但防止 TiKV 崩溃。

### B. 中期：实现重试 + 日志

```rust
let result = tablet_engine.ingest_external_file_cf(...);
if let Err(ref e) = result {
    error!("DirectIngest failed: {:?}, SST size={}, first_key={:?}", e, sst_data.len(), first_key);
}
```

已有（direct_ingest.rs:177-184），正常流程应该走这里而不是 SIGSEGV。

### C. 长期：排查 RocksDB ARM64

- 测试 release build 是否同样崩溃（排除 debug 特有 bug）
- 测试 x86_64 是否同样崩溃（排除 ARM64 特有 bug）
- 减小 SST 文件大小（每次 flush 更少 KV），观察崩溃是否减少
- 移除 `unsafe transmute`，用安全替代方案重构 `RangeLatchGuard`

### D. 最可能有效的临时方案

限制 `SstBatcher` 的 buffer 为 **极小值**（如 500KB），使得每次 flush 的 SST 文件足够小，减少 RocksDB ingest 内部复杂度：

```toml
[stream-ingest]
max-kv-buffer-size = "512KB"
```

副作用：flush 更频繁，性能略微下降，开发测试可接受。

---

## 与今晚代码改动的关系

**无关**。Crash 调用链上的所有函数（`handle_event`, `flush_and_record`, `SstBatcher::flush`, `DirectIngest::ingest_sst`, `RangeLatchGuard::drop`, `ingest_external_file_cf`）均为预存代码，今晚未修改。

今晚频繁 pause/resume + 小 buffer 配置增加了 flush 频率，从而暴露了已存在的竞态条件。
