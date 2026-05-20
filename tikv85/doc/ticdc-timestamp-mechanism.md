# TiKV TiCDC 时间控制机制详解

## 1. 概述

TiCDC 的时间控制机制是分布式事务一致性的核心保障。通过多层级时间戳（`start_ts`、`commit_ts`、`resolved_ts`、`safe_ts`、`checkpoint_ts`）和同步屏障（`Barrier`）的协同工作，系统确保 CDC 客户端能够安全地消费变更事件，同时支持 Stale Read 和增量备份等功能。

---

## 2. 核心时间戳定义

### 2.1 TimeStamp 结构

TiKV 使用 64 位 TSO（Timestamp Oracle）作为统一时间戳格式：

```rust
// components/txn_types/src/timestamp.rs
pub struct TimeStamp(u64);

pub const TSO_PHYSICAL_SHIFT_BITS: u64 = 18;

impl TimeStamp {
    pub fn compose(physical: u64, logical: u64) -> TimeStamp {
        TimeStamp((physical << TSO_PHYSICAL_SHIFT_BITS) + logical)
    }
    
    pub fn physical(self) -> u64 {
        self.0 >> TSO_PHYSICAL_SHIFT_BITS  // 物理时间（毫秒）
    }
    
    pub fn logical(self) -> u64 {
        self.0 & ((1 << TSO_PHYSICAL_SHIFT_BITS) - 1)  // 逻辑计数
    }
}
```

**TSO 组成**：
- **高 46 位**：物理时间（毫秒级 Unix 时间戳）
- **低 18 位**：逻辑时间（每毫秒内的自增序列）

### 2.2 事务时间戳

| 时间戳 | 定义 | 作用 | 获取时机 |
|--------|------|------|----------|
| **start_ts** | 事务开始时间戳 | 事务唯一标识，MVCC 读取版本 | `BEGIN` 时从 PD 获取 |
| **commit_ts** | 事务提交时间戳 | 决定事务在 MVCC 中的可见性顺序 | `COMMIT` 时从 PD 获取 |
| **min_commit_ts** | 最小提交时间戳 | 大事务（异步提交）的最小可能提交时间 | 提交前计算 |

**时间戳约束**：
```
commit_ts > start_ts
min_commit_ts ≥ start_ts
```

---

## 3. ResolvedTS 机制

### 3.1 ResolvedTS 定义

**ResolvedTS** 是 TiCDC 的核心概念，表示一个**安全时间边界**：

> **保证在 ResolvedTS 之前不会有新的提交发生**，即所有事务要么已提交（有确定的 commit_ts），要么已回滚。

```rust
// components/resolved_ts/src/lib.rs
//! Resolved TS is a timestamp that represents the lower bound of incoming
//! Commit TS
//! Through this timestamp we can get a consistent view in the transaction level.
```

### 3.2 Resolver 实现

Resolver 是计算 ResolvedTS 的核心结构：

```rust
// components/resolved_ts/src/resolver.rs
pub struct Resolver {
    region_id: u64,
    // key -> start_ts，追踪所有未提交的锁
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,
    // start_ts -> locked keys，按时间戳排序的锁堆
    lock_ts_heap: BTreeMap<TimeStamp, TxnLocks>,
    // 大事务追踪（用于异步提交/1PC）
    large_txns: HashMap<TimeStamp, TxnLocks>,
    // 已解析的时间戳
    resolved_ts: TimeStamp,
    // 追踪的 Raft log index
    tracked_index: u64,
    // 最小时间戳（用于推进 resolved_ts）
    min_ts: TimeStamp,
}
```

### 3.3 ResolvedTS 计算算法

```rust
pub fn resolve(&mut self, min_ts: TimeStamp, source: TsSource) -> TimeStamp {
    // 1. 找到最小的 start_ts（最老的未完成事务）
    let min_lock = self.oldest_transaction();
    let min_txn_ts = min_lock.as_ref()
        .map(|(ts, _)| *ts)
        .unwrap_or(min_ts);
    
    // 2. 计算新的 resolved_ts
    // 如果有锁，resolved_ts 不能超过最早事务的 start_ts
    // 如果没有锁，resolved_ts 可以推进到 min_ts（PD TSO）
    let new_resolved_ts = cmp::min(min_txn_ts, min_ts);
    
    // 3. resolved_ts 永远不会回退
    self.resolved_ts = cmp::max(self.resolved_ts, new_resolved_ts);
    
    self.resolved_ts
}
```

**计算逻辑**：
- **正常事务**：使用 `start_ts` 作为阻塞点
- **大事务（异步提交）**：使用 `min_commit_ts` 作为阻塞点
- **无锁场景**：可直接推进到 PD 提供的当前时间戳

### 3.4 锁追踪流程

```
事务开始（Prewrite）                    事务提交（Commit）
     │                                       │
     ▼                                       ▼
┌─────────────┐                     ┌─────────────────┐
│ track_lock  │                     │  untrack_lock   │
│ (start_ts)  │                     │ (key, commit_ts)│
└──────┬──────┘                     └────────┬────────┘
       │                                      │
       ▼                                      ▼
┌─────────────────────────────────────────────────────┐
│                    Resolver 状态                     │
│  ┌───────────────────────────────────────────────┐ │
│  │  lock_ts_heap: {                               │ │
│  │    ts1 -> [key1, key2],                        │ │
│  │    ts2 -> [key3],                              │ │
│  │    ...                                         │ │
│  │  }                                             │ │
│  └───────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

---

## 4. SafeTs 与 RegionReadProgress

### 4.1 SafeTs 定义

**SafeTs** 是 RaftStore 层维护的时间戳，**内部等于 ResolvedTS**，用于支持 Stale Read：

```rust
// components/raftstore/src/store/util.rs
pub struct RegionReadProgress {
    core: Mutex<RegionReadProgressCore>,
    safe_ts: AtomicU64,  // 快速读取路径，无需加锁
}

impl RegionReadProgress {
    pub fn resolved_ts(&self) -> u64 {
        self.safe_ts()  // safe_ts 是 resolved_ts 的别名
    }
}
```

### 4.2 SafeTs 更新机制

SafeTs 的更新与 Raft Apply 进度绑定：

```rust
pub fn update_safe_ts_with_time(&self, apply_index: u64, ts: u64) {
    // 只有当 apply_index 已被应用到状态机后，
    // 对应的 ts 才能作为 safe_ts
    if self.applied_index >= apply_index {
        self.safe_ts.store(ts, AtomicOrdering::Release);
    } else {
        // 否则放入 pending_items 等待应用
        self.pending_items.push_back(ReadState { idx: apply_index, ts });
    }
}
```

**更新来源**：
- **resolved_ts 模块**：通过 `update_safe_ts_with_time` 更新
- **Leader 信息同步**：Follower 从 Leader 获取 `ReadState` 更新

### 4.3 Store SafeTs

Store SafeTs 是 Store 内所有 Region 的最小 SafeTs：

```rust
// components/raftstore/src/store/worker/check_leader.rs
fn get_range_safe_ts(&self, key_range: KeyRange) -> u64 {
    self.region_read_progress.with(|registry| {
        registry
            .iter()
            .map(|(_, rrp)| rrp.safe_ts())
            .filter(|ts| *ts != 0)
            .min()
            .unwrap_or(0)
    })
}
```

---

## 5. Barrier 机制（CDC 同步屏障）

### 5.1 Barrier 作用

CDC 中的 **Barrier** 是**事件同步机制**，确保增量扫描（Incremental Scan）过程中的事件顺序：

```rust
// components/cdc/src/channel.rs
pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
    Barrier(Option<Box<dyn FnOnce(()) + Send>>),  // 同步屏障
}
```

### 5.2 增量扫描流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     增量扫描流程与 Barrier 机制                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 注册 CDC 订阅                                                │
│     │                                                            │
│     ▼                                                            │
│  2. 发送 Barrier-1 ───────────────► 等待确认                      │
│     │                              (确保后续增量扫描数据           │
│     │                               排在所有 delta 变更之后)        │
│     ▼                                                            │
│  3. 执行增量扫描（扫描当前快照）                                    │
│     │                                                            │
│     ▼                                                            │
│  4. 扫描完成后发送 Barrier-2 ──────► 等待确认                       │
│     │                              (确保扫描事件已发送完毕)         │
│     ▼                                                            │
│  5. 开始正常发送 ResolvedTs 事件                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Barrier 实现代码

```rust
// components/cdc/src/initializer.rs (line 153-188)
// Barrier-1：确保 Delta 变更先发送
let (incremental_scan_barrier_cb, incremental_scan_barrier_fut) =
    tikv_util::future::paired_future_callback();
let barrier = CdcEvent::Barrier(Some(incremental_scan_barrier_cb));

// 发送 barrier 到 raftstore
cdc_handle.capture_change(..., Callback::read(Box::new(move |resp| {
    sched.schedule(Task::InitDownstream {
        incremental_scan_barrier: barrier,
        ...
    })
}));

// 等待 barrier 被消费
if let Err(e) = incremental_scan_barrier_fut.await {
    return Err(Error::Other(box_err!(e)));
}

// Barrier-2：确保扫描事件已发送
if done {
    let (cb, fut) = tikv_util::future::paired_future_callback();
    events.push(CdcEvent::Barrier(Some(cb)));
    barrier = Some(fut);
}
if let Some(barrier) = barrier {
    let _ = barrier.await;  // 等待确认
}
```

**关键保证**：
- **Barrier-1**：增量扫描数据排在所有历史 Delta 变更之后
- **Barrier-2**：ResolvedTs 事件只会在增量扫描完成后发送

---

## 6. Checkpoint 机制（Backup Stream）

### 6.1 Checkpoint 类型

Backup Stream（日志备份）使用多层 Checkpoint 机制：

```rust
// components/backup-stream/src/metadata/client.rs
pub struct Checkpoint {
    pub provider: CheckpointProvider,
    pub ts: TimeStamp,
}

pub enum CheckpointProvider {
    Global,                    // 全局 Checkpoint（跨 Store）
    Store(u64),               // Store 级别 Checkpoint
    Region { id: u64, version: u64 },  // Region 级别 Checkpoint
}
```

### 6.2 Checkpoint 层级关系

```
┌─────────────────────────────────────────────────────────────┐
│                   Checkpoint 层级结构                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   ┌─────────────────────────────────────────────────────┐  │
│   │              Global Checkpoint                       │  │
│   │   (所有 Store Checkpoint 的最小值)                   │  │
│   └─────────────────────────────────────────────────────┘  │
│                         ▲                                   │
│                         │ 取最小值                           │
│   ┌─────────────────────┼─────────────────────────────┐    │
│   │                     │                              │    │
│   ▼                     ▼                              ▼    │
│ Store1 Checkpoint   Store2 Checkpoint   Store3 Checkpoint  │
│ (各 Store 的 Region   (同上)              (同上)            │
│  Checkpoint 最小值)                                        │
│   ▲                     ▲                                  │
│   │                     │                                  │
│   │   ┌─────┐          │   ┌─────┐                        │
│   └──►│ R1  │          └──►│ R2  │                        │
│       │ R2  │              │ R3  │                        │
│       │ R3  │              │ R4  │                        │
│       └─────┘              └─────┘                        │
│    (Region Checkpoint =    (Region Checkpoint =            │
│     该 Region resolved_ts)  该 Region resolved_ts)         │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 6.3 Global Checkpoint 计算

```rust
// components/backup-stream/src/endpoint.rs (line 852)
let mut new_rts = resolved.global_checkpoint();  // 所有 Region 最小值

// components/backup-stream/src/router.rs
pub async fn update_global_checkpoint(
    &self,
    global_checkpoint: u64,
    store_id: u64,
) -> Result<bool> {
    let last_global_checkpoint = self.global_checkpoint_ts.load(Ordering::SeqCst);
    if last_global_checkpoint < global_checkpoint {
        // 更新并持久化到外部存储
        self.global_checkpoint_ts.compare_exchange(
            last_global_checkpoint,
            global_checkpoint,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )?;
        self.flush_global_checkpoint(store_id).await?;
    }
}
```

### 6.4 Storage Checkpoint

**Storage Checkpoint** 表示已**持久化到外部存储**的进度，用于故障恢复：

```rust
// 将 Global Checkpoint 写入外部存储
pub async fn flush_global_checkpoint(&self, store_id: u64) -> Result<()> {
    let filename = format!("v1/global_checkpoint/{}.ts", store_id);
    let buff = self.global_checkpoint_ts
        .load(Ordering::SeqCst)
        .to_le_bytes();
    self.storage.write(&filename, &buff).await?;
}
```

---

## 7. 时间戳关系全景图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           时间戳控制全景图                               │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────┐     ┌─────────────────┐     ┌──────────────────┐
│   PD    │────►│     TSO         │────►│  min_ts (PD时间)  │
│ (时钟源) │     │  (物理+逻辑时间)  │     └────────┬─────────┘
└─────────┘     └─────────────────┘              │
                                                 ▼
┌─────────────────────────────────────────────────────────────┐
│                      resolved_ts 计算                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  resolved_ts = min(                                  │   │
│  │    - 最老未提交事务的 start_ts (或 min_commit_ts)      │   │
│  │    - PD 提供的 min_ts                                │   │
│  │    - 内存锁的最小时间戳                               │   │
│  │  )                                                   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┬────────────────┐
        ▼                ▼                ▼                ▼
   ┌─────────┐     ┌─────────┐     ┌─────────────┐   ┌───────────┐
   │  CDC    │     │Backup   │     │ RaftStore   │   │ 事务管理   │
   │(事件流)  │     │Stream   │     │(Stale Read) │   │(冲突检测)  │
   └────┬────┘     └────┬────┘     └──────┬──────┘   └───────────┘
        │                │                 │
        ▼                ▼                 ▼
   ┌─────────┐     ┌─────────────┐   ┌─────────┐
   │Barrier  │     │   Checkpoint│   │safe_ts  │
   │(同步点)  │     │   (持久化点) │   │(读取点) │
   └─────────┘     └─────────────┘   └─────────┘
        │                │                 │
        └────────────────┴─────────────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │  Global Consistency
                 │  (全局一致性保证)
                 └─────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          时间戳约束关系                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   事务时间戳:    start_ts < commit_ts                                    │
│                       ▲                                                  │
│                       │                                                  │
│   ResolvedTS:  resolved_ts ≤ min(start_ts of ongoing txns)              │
│                       │                                                  │
│                       ▼                                                  │
│   SafeTS:      safe_ts = resolved_ts (RaftStore 层)                      │
│                       │                                                  │
│                       ▼                                                  │
│   Checkpoint:  checkpoint ≤ min(safe_ts of all regions)                 │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. 各模块时间戳对比

| 模块 | 时间戳/机制 | 作用 | 计算/实现方式 |
|------|------------|------|--------------|
| **事务** | `start_ts` | 事务开始标识 | PD TSO |
| **事务** | `commit_ts` | 事务提交标识 | PD TSO |
| **事务** | `min_commit_ts` | 大事务最小提交时间 | 本地计算 |
| **resolved_ts** | `resolved_ts` | 安全读取边界 | min(锁 start_ts, PD TSO) |
| **RaftStore** | `safe_ts` | Stale Read 支持 | 等于 resolved_ts |
| **RaftStore** | `store_safe_ts` | 跨 Region 一致性 | min(所有 Region safe_ts) |
| **CDC** | `Barrier` | 增量扫描同步 | 显式事件屏障 |
| **Backup Stream** | `region_checkpoint` | Region 备份进度 | resolved_ts |
| **Backup Stream** | `global_checkpoint` | 任务整体进度 | min(所有 Region checkpoint) |
| **Backup Stream** | `storage_checkpoint` | 已持久化进度 | 异步更新的 global_checkpoint |

---

## 9. 关键保证与约束

### 9.1 核心约束

1. **单调递增**：所有时间戳（resolved_ts、safe_ts、checkpoint）都保证单调递增，不会回退
2. **安全边界**：resolved_ts 保证在该时间戳之前不会有新的 commit_ts 出现
3. **异步提交支持**：大事务使用 `min_commit_ts` 代替 `start_ts` 计算 resolved_ts，减少阻塞

### 9.2 一致性保证

```
┌─────────────────────────────────────────────────────────────┐
│                    一致性保证层级                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Level 1: 事务内一致性                                        │
│           start_ts → commit_ts 的映射保证单事务原子性          │
│                                                              │
│  Level 2: Region 内一致性                                     │
│           resolved_ts 保证 Region 内所有事务的提交顺序可见       │
│                                                              │
│  Level 3: Store 内一致性                                      │
│           store_safe_ts 保证 Store 内跨 Region 一致性读         │
│                                                              │
│  Level 4: 集群级一致性                                        │
│           global_checkpoint 保证全局备份一致性                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

这些时间戳机制和 Barrier/Checkpoint 系统共同构成了 TiKV 强大的分布式事务一致性保障体系，支撑了 CDC、Stale Read、增量备份等关键功能。

---

## 参考代码位置

- `components/txn_types/src/timestamp.rs` - TimeStamp 定义
- `components/txn_types/src/lock.rs` - Lock 结构（含 start_ts/min_commit_ts）
- `components/resolved_ts/src/resolver.rs` - Resolver 实现
- `components/resolved_ts/src/lib.rs` - ResolvedTS 模块定义
- `components/cdc/src/channel.rs` - CDC Barrier 机制
- `components/cdc/src/initializer.rs` - 增量扫描实现
- `components/raftstore/src/store/util.rs` - RegionReadProgress/SafeTs
- `components/backup-stream/src/metadata/client.rs` - Checkpoint 定义
- `components/backup-stream/src/router.rs` - Global Checkpoint 实现
