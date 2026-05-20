# PCR Bridge spawn_blocking 线程池阻塞分析

**分析时间**: 2026-05-02

**问题**: Resume 后 65 个 Subscribe 请求到达源 TiKV，bridge task 调用 `spawn_blocking` 执行 RocksDB secondary scan，但 snapshot scan 日志长时间不出现，表现为「被线程池阻塞」。

---

## 根因

### 代码路径

```rust
// pcr_service.rs:50-56 — bridge runtime 只有 2 worker
let bridge_runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(2)       // async worker 线程
    .thread_name("pcr-bridge")
    .build();

// pcr_service.rs:120 — 每个 Subscribe 调用
bridge_rt.spawn(async move {
    let scan_kvs = tokio::task::spawn_blocking(move || {
        SnapshotScanner::open(&db_path)?;      // RocksDB secondary open
        scanner.scan_write_cf_since(ts, ..)    // 遍历 write CF + default CF seek
    }).await;
});
```

### 实际行为

`sample` 命令确认：有 8+ 个 pcr-bridge 线程在执行中。`spawn_blocking` **并非被阻塞**，而是：

1. 65 个 region 同时调用 `spawn_blocking`
2. 每个在 tokio 全局 blocking pool 中创建 OS 线程（macOS debug build）
3. 65 个线程同时打开 RocksDB secondary → 587 个文件在 `/tmp/pcr-src-tikv/db.pcr_snap/` 下
4. 同时争抢同一 RocksDB 目录的 SST 文件 → **I/O 瓶颈**导致极慢

### 关键：spawn_blocking 用全局池

`bridge_runtime` 的 2 个 worker 只调度 async task，**不影响** `spawn_blocking` 的并发数。后者使用 tokio 全局 blocking thread pool（默认上限 512）。

---

## 方案对比

### 方案 A：增加 bridge_runtime worker_threads

```rust
.worker_threads(8)  // 2 → 8
```

| 优点 | 缺点 |
|------|------|
| 一行改动 | **不解决问题**：spawn_blocking 用全局池，与 bridge_runtime worker 数无关 |
| 对 CDC 增量阶段 event 处理有帮助 | |

### 方案 B：调大 tokio 全局 blocking 线程池上限

```rust
tokio::runtime::Builder::new_multi_thread()
    .max_blocking_threads(512)  // 默认就是 512
```

| 优点 | 缺点 |
|------|------|
| | 65 线程同时开 RocksDB → 587 文件 + IOPS 爆炸 |
| | macOS debug build 每线程栈 8MB → 65×8=520MB 额外内存 |

### 方案 C：并发限流 — Semaphore

```rust
static SCAN_SEM: tokio::sync::Semaphore = Semaphore::const_new(4);

let _permit = SCAN_SEM.acquire().await;
let scan_kvs = tokio::task::spawn_blocking(move || { ... }).await;
```

| 优点 | 缺点 |
|------|------|
| 直接控制并发度 | 需全局状态（跨 Subscribe 调用共享） |
| 不增加线程数 | `Semaphore::const_new` 需 tokio 1.25+ |
| 可根据硬件调整 | |

### 方案 D：std::thread::spawn + channel

```rust
let (tx, rx) = std::sync::mpsc::channel();
std::thread::spawn(move || {
    let kvs = scanner.scan_write_cf_since(ts, usize::MAX);
    let _ = tx.send(kvs);
});
let scan_kvs = rx.recv().unwrap_or_default();
```

| 优点 | 缺点 |
|------|------|
| 不占 tokio blocking pool | 65 thread 仍在争抢 I/O |
| 简单直接 | 需手动管理线程生命周期 |

### 方案 E（推荐）：Semaphore 限流 + 自适应并发

```rust
use std::sync::OnceLock;
use tokio::sync::Semaphore;

fn scan_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(
        std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(2)
    ))
}

// bridge task 中
let _permit = scan_semaphore().acquire().await;
let scan_result = tokio::task::spawn_blocking(move || {
    let scanner = SnapshotScanner::open(&db_path)?;
    if scan_ts == 0 {
        Ok(scanner.scan(&scan_start, &scan_end, usize::MAX))
    } else {
        Ok(scanner.scan_write_cf_since(scan_ts, usize::MAX))
    }
}).await;
```

| 优点 | 缺点 |
|------|------|
| 自动适配 CPU 核心数（最多 4） | 需 `available_parallelism` |
| 同时解决 I/O 争抢和内存问题 | 65 region → 分 16 批，总时间增长但可控 |
| 改动集中一处 | |
| 生产环境可调 | |

---

## 对比总表

| 维度 | A (worker) | B (blocking) | C (Semaphore) | D (裸线程) | **E (限流+自适应)** |
|------|-----------|-------------|--------------|-----------|-------------------|
| 是否解决问题 | ❌ | ❌ | ✅ | ⚠️ 部分 | ✅ |
| 改动量 | 1 行 | 1 行 | ~10 行 | ~15 行 | ~15 行 |
| I/O 争抢 | 不变 | 更严重 | 可控 | 可控 | 自动可控 |
| 额外内存 | 0 | 520MB+ | 0 | 520MB+ | 0 |
| 并发数控制 | 无 | 无 | 硬编码 | 无 | CPU 自适应 |

**推荐方案 E**。
