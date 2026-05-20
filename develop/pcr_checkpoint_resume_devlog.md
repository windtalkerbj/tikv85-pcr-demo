# PCR 断点续传（Checkpoint & Resume）开发历程

## 一、需求背景

**Physical Cluster Replication (PCR)** 是 TiKV 的字节级集群复制功能。在暂停（pause）→ 恢复（resume）场景下，必须做到：

- 暂停期间源集群的写入不能丢失
- 恢复后只需同步增量部分（delta），不能重做全量
- 每个 Region 只扫描自己负责的 key range，避免 66x 冗余传输

这就是"断点续传"的核心需求。

## 二、开发时间线

**2026-05-04 ~ 2026-05-06**，前后经历 8 套技术方案。

## 三、方案演进

### 阶段 1：CDC 增量流（自研，已上线）

**思路**：pause 后 CDC observer 继续推送增量事件，resume 后从事件队列恢复。

**结果**：pause 期间的 MVCC 写入无法被 CDC observer 可靠捕获（observer max_level 限制），数据丢失。

### 阶段 2：Write CF 全量 Delta Scan（自研，已上线）

**思路**：resume 时扫描整个 Write CF，通过 `commit_ts > checkpoint_ts` 过滤出增量。

**结果**：数据完整性正确，但 66 个 Region 各扫一遍全库，Bridge/Ingested 比 = 14:1，带宽浪费严重。

### 阶段 3：Per-Region Bounded Scan — seek(raw key)（自研，失败）

**思路**：每个 Region 只扫描自己的 key range（`[start_key, end_key)`），通过 `iter.seek(from)` 定位起点。

**尝试**：直接将 PD 返回的 Region start_key（原始字节）传给 RocksDB 的 `seek()`。

**结果**：65/66 个 Region 的 seek 返回 iterator invalid。被用户否决。

### 阶段 4：编码补齐（自研，失败）

**思路**：RocksDB 写 CF 的 key 格式是 `encode_bytes(user_key) + encode_u64_desc(ts)`，seek target 需要补齐 8 字节时间戳后缀。调用 TiKV 的 `Key::from_raw().append_ts()` 来编码，分别尝试 `TimeStamp::max()` 和 `TimeStamp::zero()` 作为后缀。

**结果**：iterator 仍然 invalid。此时尚未发现"双编码"问题。

### 阶段 5：IterOptions 原生边界（自研，失败）

**思路**：使用 RocksDB 的 `IterOptions::set_lower_bound()` / `set_upper_bound()` 让 RocksDB 内部处理边界，编码同上。

**结果**：`upper_bound` 设置后，写 CF iterator 把所有 key 都过滤掉（`raw_total=0`）。**这是写 CF 自定义 Comparator 与 `iterate_upper_bound` 的兼容性问题**。

### 阶段 6：参照 TiCDC（用户提示，失败→重新理解）

**用户提示**："参考 TiCDC 代码，看它是怎么给写 CF 设边界的。"

**发现**：TiCDC 在 Go 侧用 `codec.EncodeBytes()` 编码 key，然后通过 gRPC 发给 TiKV 的 CDC 组件，TiKV 内部处理真正的 RocksDB 扫描。**TiCDC 从不直接调用 RocksDB iterator API。**

同时发现 TiCDC 的 `EncodeBytes` 不带时间戳后缀——这纠正了我"必须补齐 ts 后缀"的错误认知。但此时仍不清楚为什么去掉 ts 后缀仍然 seek 失败。

### 阶段 7：发现双编码（用户提示，突破）

**用户提示**："TiKV 里有三种 key，不能混：raw key、encoded key、MVCC key。你现在把 encoded key 当 raw key 又 encode 了一次。"

**根因**：PD 返回的 Region start_key/end_key 已经是 memcomparable（encode_bytes）格式，不是 raw key。我调用 `Key::from_raw(pd_key)` 时，`from_raw` 内部又执行了一次 `encode_bytes()`，相当于"对已经编码的数据再编码一次"，产生了一个完全不匹配的 seek target。

**修复**：直接用 PD key 拼接 `[0x00;8]`（MAX_TS 后缀），不再经过 `Key::from_raw()`。

### 阶段 8：代码级 upper_bound 替代（用户提示 + 自研完成）

**用户提示**："RocksDB upper_bound 在写 CF comparator 下有问题，换代码级过滤。"

**方案**：
- `lower_bound` = PD key + `[0x00;8]` → **不双编码**（关键）
- `seek(lower_bound)` → 迭代器定位到 Region 范围起点
- `user_key >= to` → 代码级判断，`truncate_ts_for()` 解码后在同一编码空间比较
- `RocksDB iterate_upper_bound` 不再使用

**结果**（2026-05-06 验证）：
- 60 个数据库、不同 table_id 跨度下，3 个 bounded Region 各自找到数据
- Delta 数据完整性正确
- Bridge/Ingested 效率从 14:1 降至 ~2:1

## 四、关键决策点

| 决策 | 谁提出 | 影响 |
|------|--------|------|
| 不用 TiDB SQL 导数据，改用 raw-put + 多 DB | 用户 | 排除数据分布干扰 |
| 参考 TiCDC EncodeBytes 不带 ts 后缀 | 用户 | 纠正编码格式认知 |
| "PD key 就是 encoded key，你在双编码" | 用户 | 解决核心 seek 失败问题 |
| 放弃 RocksDB upper_bound，用代码过滤 | 用户 | 绕过写 CF Comparator 兼容问题 |
| 随机 key 埋数 + 多 DB 验证 Region 分布 | 用户 | 证明 bounded scan 正确性 |

## 五、技术收获

1. **TiKV 的 key 分层**：raw key → memcomparable encoded key → MVCC key，每一层都不能混淆
2. **写 CF Comparator 的局限性**：支持 `iterate_lower_bound`，但不兼容 `iterate_upper_bound`
3. **PD API 返回的 key 格式**：已经是 memcomparable 编码，不是原始 user_key
4. **TiCDC 的参考价值**：它的 Go 代码揭示编码格式，但它通过 gRPC 间接操作，不经过 RocksDB API

## 六、现状

- Bounded delta scan: ✅ 完成
- Full scan: ✅ 完成
- create / resume 生命周期: ✅ 完成
- pcr-ctl: ✅ 完成（create 子命令）
- Grafana 监控面板: ✅ 完成
- SST Errors: 0
- 已知限制: `iterate_upper_bound` 不可用，由代码级过滤替代
