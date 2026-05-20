# P0 可观测性设计文档

## 概述

对标 CRDB `logical_replication.errors`、retry tracking、partition stream health，为 TiKV PCR 补齐运维必备的 3 类可观测性指标。

## 设计

### 1. 错误分类计数器 `pcr_errors_total`

**类型**：`IntCounterVec{type}`

**上报点**：

| type | 触发位置 | 条件 |
|------|----------|------|
| `disconnect` | `subscriber.rs` retry loop | gRPC RpcFailure / Unavailable |
| `epoch` | `subscriber.rs` retry loop | RegionEpoch 不匹配 |
| `subscription` | `subscriber.rs` retry loop | 其他订阅错误 |
| `ingest` | `task.rs` handle_event | Consumer 端事件处理失败 |
| `flush` | 预留 | SST flush 失败（后续接入） |

**PromQL 告警**：`rate(pcr_errors_total[5m]) > 0.1` → PcrErrorRateHigh

### 2. 重连追踪 `pcr_subscriber_reconnects_total`

**类型**：`IntCounterVec{region_id}`

**上报点**：`subscriber.rs` retry loop，每次重连时 `inc()`

**用途**：
- 识别持续不稳定的 Region：`topk(5, rate(pcr_subscriber_reconnects_total[2m]))`
- 检测大面积断连：`count(rate(pcr_subscriber_reconnects_total[2m]) > 0.1) > 10`

**告警**：`rate(pcr_subscriber_reconnects_total[5m]) > 0.05` → PcrFrequentReconnects

### 3. 流健康检测 `pcr_bridge_liveness_seconds`

**类型**：`IntGaugeVec{region_id}`

**上报点**：`task.rs` 事件循环，收到每个事件时 `set(0)`

**语义**：值为 0 = 正常，值增长 = 该 Region 的 bridge 无事件

**告警**：`pcr_bridge_liveness_seconds > 120` → PcrBridgeStalled

## 数据流

```
源端 CDC → bridge task → gRPC → Consumer event loop
                                    │
                   ┌────────────────┼────────────────┐
                   │                │                │
              error=disconnect  reconnect.inc()  liveness.set(0)
              (连接断开)        (重连计数)       (流正常)
```

## Grafana 面板

| 面板 | PromQL | 说明 |
|------|--------|------|
| Errors by Type | `rate(pcr_errors_total[2m])` | 4 线：disconnect/epoch/ingest/subscription |
| Reconnects top-5 | `topk(5, rate(reconnects[2m]))` | 重连最频繁 Region |
| Bridge Liveness top-5 | `topk(5, liveness)` | 最久无事件 Region |

## 告警规则

| 告警 | 条件 | 级别 |
|------|------|------|
| PcrErrorRateHigh | `rate(errors[5m]) > 0.1` | critical |
| PcrFrequentReconnects | `rate(reconnects[5m]) > 0.05` | warning |
| PcrBridgeStalled | `liveness > 120` | critical |
