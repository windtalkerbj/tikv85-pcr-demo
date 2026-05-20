# DELETE DML 同步原理与延迟分析

日期：2026-05-18

## 1. TiKV Percolator 事务中 DELETE 的底层编码

```
源端 DELETE FROM t WHERE id=5  →  2PC:
  Prewrite:  Put(LOCK CF,   key=z_t_5,  value=Lock{...})
  Commit:    Del(LOCK CF,   key=z_t_5)          ← 清理锁
             Put(WRITE CF,  key=z_t_5,  value=WriteRef{
                 write_type: Delete,              ← 关键：类型是 Delete
                 start_ts:   <txn_start>,
                 short_value: None                ← Delete 无值
             })
```

**DELETE 不写 DEFAULT CF**。这是和 INSERT/UPDATE 最本质的区别。

## 2. PCR 两条复制路径的差异

```
                   全量扫描路径                     Live CDC 路径
                   ────────────                     ─────────────
源端数据源        RocksDB 直接读 KV               Raft CmdBatch → delegate
DEFAULT CF        扫描到 key+value →               N/A（DELETE 不产生）
                  发 PcrKv{cf="default", 
                  op=Put, value}

WRITE CF          扫描到 WriteRef →                Raft Put(cf="write") →
                  scan_delta_entries 解析            delegate 原样转发
                  WriteType::Delete →               PcrKv{cf="write",
                  发 PcrKv{cf="default",             op=Put, value=<WriteRef 字节>}
                  op=Delete}  ← 墓碑！

目标端写入        DEFAULT CF 有墓碑               WRITE CF 有 WriteRef 字节
                  ↓                                ↓
MVCC 可见性       ✅ 立即（DEFAULT CF 直接可读）    ❌ 需等 TSO
```

全量扫描中对 DELETE 的处理是正确的——`scan_delta_entries` 解析 WriteRef，发现 `WriteType::Delete`，生成 DEFAULT CF 的墓碑（`OpType::Delete`）。Live CDC 路径缺少这一步——只把 WRITE CF 原样转发，没有生成 DEFAULT CF 墓碑。

## 3. 延迟的根因：MVCC 快照 TSO

INSERT/UPDATE 为什么立即可见：

```
INSERT/UPDATE 后的 RocksDB:
  DEFAULT CF: z_t_5 → "new_value"     ← 值直接在这里，MVCC reader 随时可读
  WRITE CF:   z_t_5 → WriteRef{Put, short_value}
```

DELETE 为什么延迟可见：

```
DELETE 后的 RocksDB:
  DEFAULT CF: z_t_5 → "old_value"     ← 旧值还在！
  WRITE CF:   z_t_5 → WriteRef{Delete, commit_ts=TS_del}  ← 删的证据只在这里
```

MVCC reader 的判断逻辑：
- `snapshot_ts >= TS_del` → 看到 Delete WriteRef → 隐藏此 key ✅
- `snapshot_ts < TS_del` → 忽略新 WriteRef → 返回 old_value ❌

而 **target TiDB 的 snapshot_ts 来自 target PD TSO**，与源端 PD TSO（`TS_del` 的来源）是独立的两条 TSO 流。target TSO 需要自然推进到超过 `TS_del` 后，MVCC reader 才能"看到"删除。这个追赶过程通常需要 2-5 分钟。

## 4. Consumer 端 Flush Ticker 修复（2026-05-18）

在 consumer 事件循环中添加 1s 周期的 batcher flush ticker，确保稀疏写入（如 DELETE 的 WRITE CF 条目）不会无限期卡在 SstBatcher：

```rust
// task.rs run_event_loop
let mut flush_ticker = tokio::time::interval(
    std::time::Duration::from_secs(1),
);
// ...
_ = flush_ticker.tick() => {
    if let Err(e) = components.flush_and_record().await {
        error!("PCR: periodic flush error: {:?}", e);
    }
}
```

对称于 source 端 `sink_data` 的 force-flush（2026-05-17 修复）。

| 场景 | 修复前 | 修复后 |
|------|--------|--------|
| 单行 DELETE | 5s | ~3-5s |
| 10 行 DELETE | 无限期卡住 | ~3-5s |
| 200 行 DELETE | 1h+ / 不收敛 | ~2-5min |

大规模 DELETE 的 2-5 分钟尾巴是 MVCC TSO 推进问题（见第 3 节），非 flush 问题。

## 5. 改善方案

### 方案 A：源端解析 WriteRef 生成墓碑（推荐，对标全量扫描路径）

在 `delegate.rs` 的 `sink_txn_put` 中，`cf="write"` 分支增加 WriteRef 解析：

```rust
if put.get_cf() == "write" {
    if let Ok(write) = WriteRef::parse(put.get_value()) {
        if write.write_type == WriteType::Delete {
            // 生成 DEFAULT CF 墓碑，和全量扫描路径一致
            let mut key = Vec::with_capacity(1 + put.get_key().len() + 8);
            key.push(b'z');
            key.extend_from_slice(put.get_key());
            key.extend_from_slice(&(!write.start_ts.into_inner()).to_be_bytes());
            batcher.add_kv(key, vec![], OpType::Delete, "default");
        }
    }
    // 同时保留 WRITE CF 原值转发
    // ...
}
```

- 优点：对标全量扫描行为，目标端 RocksDB 直接有墓碑，不依赖 TSO
- 改动量：~15 行
- 原理：consumer 收到 `OpType::Delete` 后，SstBatcher 写入 DEFAULT CF 带有 MVCC 时间戳后缀的 key → 无论 snapshot_ts 多少都能读到墓碑

### 方案 B：Consumer 端主动推进 target PD TSO

Consumer 在记录 checkpoint 时，将 `min_ts` 同步到 target PD：

```
Consumer checkpoint min_ts=TS_x → target PD TSO = max(current, TS_x)
```

- 优点：解决所有 TSO 相关的延迟（不仅 DELETE）
- 缺点：需要 consumer 持有 target PD client，跨组件改动
- 改动量：~30 行

### 推荐实施顺序

先 A（治本，直接生成墓碑），后 B（治标，减少其他 TSO 相关延迟）。A 方案改动小、风险低、和全量扫描路径一致。

## 6. 当前状态

- DELETE 最终一致性：✅（全量扫描路径有墓碑，live CDC 路径依赖 TSO 追赶）
- Consumer flush ticker：✅ 已修（1s 周期）
- 方案 A（WriteRef 解析）：待实施
- 方案 B（TP TSO 推进）：待评估
