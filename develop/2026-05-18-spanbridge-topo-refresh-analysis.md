# SpanBridge 拓扑刷新跟不上 Region Split 的设计分析

日期：2026-05-18

## 1. 设计背景：为什么需要拓扑刷新

PCR 采用 span-based 架构：consumer 发一条 `subscribe_span(["", ""))` 订阅全 key range，source 端 SpanBridge 负责将 span 映射到具体的 region 列表并管理 per-region sub-bridge。

```
Consumer:  1 gRPC stream（全 key range）
              ↓
SpanBridge: PD 解析 → 65 个 region → 65 个 sub-bridge
              ↓                      ↓
          sub-bridge[2]          sub-bridge[28]
          event_sink ──────────→ delegate[28].pcr_event_sink
```

每个 sub-bridge 通过 `StartPcrStream` 将一个 `mpsc::UnboundedSender` 注入到对应 region 的 delegate 的 `pcr_event_sink` 字段。delegate 在处理 Raft 命令时通过这个 sink 发送 PCR 事件。

**Region split 时**（`endpoint.rs:292`）：

```
"region met split/merge command, stop tracking since key range changed, wait for re-register"
```

CDC observer 将**父 region 的 delegate 整个 deregister**，子 region 创建**新 delegate（无 PCR sink）**。

```
Split 前:  delegate[24] → pcr_event_sink = Some(tx)   ✅ 有 PCR 输出
Split 后:  delegate[24] → 已析构                        ❌ 消失
           delegate[130] → pcr_event_sink = None        ❌ 新 delegate 无 PCR
           delegate[132] → pcr_event_sink = None        ❌ 新 delegate 无 PCR
```

## 2. 当前设计：300s 拓扑刷新

```rust
// span_bridge.rs:160
let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
loop {
    interval.tick().await;
    let regions = tokio::task::spawn_blocking(move || {
        Self::resolve_regions_static(&pd, &ss, &se)  // curl PD HTTP API
    }).await.unwrap_or_default();
    let _ = topo_tx.send(regions);
}
```

每次刷新做的事（`span_bridge.rs:179-220`）：

```
1. curl http://source_pd/pd/api/v1/regions → 获得当前 region 列表
2. 对比 active set → 发现 stale region → cancel 旧 sub-bridge
3. 对比 epoch → 发现 conf_ver/version 变化 → 重建 sub-bridge
4. 发现新 region → spawn_sub_bridge() → StartPcrStream → delegate.enable_pcr()
```

**300s 的选择理由**：curl PD HTTP API 是同步阻塞调用，虽然用 `spawn_blocking` 隔离了 tokio worker，但频繁查询会增加 PD 负载。在没有大量写入的稳态下，region 拓扑很少变化，300s 足够。

## 3. 核心矛盾

```
Region split 发生:           < 1s（写入触发，瞬间完成）
SpanBridge 感知 split:       ≤ 300s（下一次拓扑刷新）
                            ↑
                     这个窗口内的所有 DML 数据
                     进入无 PCR sink 的 delegate
                     → 静默丢失
```

实测数据（6000 行 INSERT 测试，2026-05-18）：

| 指标 | 数值 |
|------|------|
| INSERT 行数 | 6000 |
| Region split 次数 | **122** |
| SpanBridge 拓扑刷新次数 | **1**（仅初始） |
| 子 region 获得 PCR sink 的 | 0（全部在刷新前就产生了） |
| 后续 UPDATE/DELETE 收敛 | ❌ 0% |

## 4. 为什么不能简单缩短间隔

| 难题 | 说明 |
|------|------|
| **PD 负载** | `resolve_regions_static` 用 `curl` 同步调用 PD HTTP API，65+ region 的 JSON 解析较重。缩短到 5s 意味着每秒 13 次 curl |
| **Sub-bridge 重建开销** | 每次 epoch 变化都要 `spawn_sub_bridge()` → `StartPcrStream` → delegate 重新注册。重建瞬间的 in-flight 事件丢失 |
| **skip_scan 的取舍** | 重建 sub-bridge 时 `skip_scan=true`（不重扫），但 split 后子 region 的 RocksDB 数据已经存在，不扫就永久丢失增量窗口内的数据。`skip_scan=false` 又会重复发送大量数据 |
| **Event-driven 的困难** | 理想方案是监听 region split 事件（`endpoint.rs:292` 的日志），直接触发 SpanBridge 重建。但 SpanBridge 运行在独立 tokio runtime，和 CDC endpoint 不在同一上下文，无法直接接收 split 通知 |
| **Delegate 生命周期** | delegate 的 deregister 和子 delegate 的创建是 raftstore 内部行为，SpanBridge 只能通过轮询 PD 间接感知 |

## 5. 可选改进方向

| 方案 | 思路 | 效果 | 复杂度 |
|------|------|------|--------|
| **缩短间隔** | 300s → 30s | 窗口缩小 10x | 低（改一行） |
| **Sink repair 改为全量** | 已有的 10s broken_rx 修复对所有 active region 重发 StartPcrStream | endpoint `has_pcr()` 跳过了健康 region，可改为无条件 `enable_pcr()` | 低 |
| **endpoint 主动通知 SpanBridge** | split 时 endpoint 通过 channel 通知 SpanBridge "region X 已 split，请刷新" | 事件驱动，近乎实时 | 中（跨线程通信） |
| **RegionActor 化** | 每个 region 独立 actor，split 时 actor 自行处理子 region 的出生 | 最彻底的架构改进 | 高（需重构 SpanBridge） |
