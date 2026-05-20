# PCR SpanBridge 容灾恢复 & DDL 支持设计

## 问题

PCR 运行期间 target TiKV 宕机重启后，SpanBridge 的 gRPC 连接断开，导致所有 sub-bridge 被杀死。delegate 的 `emit_pcr_event` 检测到 sink 断开后将 `pcr_batcher = None`，PCR 对所有 region 停用。后续 DML 数据（包括 DROP + CREATE 后的新表 INSERT）虽经 raftkv + CDC observer，但无 PCR batcher 捕获，数据丢失。

**DROP + CREATE 本身不是导致 PCR 失败的原因**——SpanBridge 覆盖全 key range `["", "")`，新表的 key 天然落在已有 region 内。Lock short_value 提取、WriteRef z-prefix 等机制对新表同样生效。问题在于 SpanBridge 在 target 宕机后无法自动恢复。

## 分析

### 故障链

```
target TiKV 宕机 → gRPC sink 断开
  → SpanBridge writer task 退出
  → merge channel 关闭
  → sub-bridge event_rx 收到 None → 进入 checkpoint-only 循环
  → delegate 的 pcr_event_sink 未受影响（sub-bridge 退出了但 sender 还在）
  → 但后续 emit_pcr_event 尝试发送 → unbounded_send 失败（receiver 已 drop）
  → pcr_batcher = None, pcr_event_sink = None
  → PCR 对该 region 永久停用
```

### 当前 SpanBridge 的问题

1. **无重连机制**: `start()` 创建 sub-bridge 后，若 gRPC 连接断开，SpanBridge 不会重建
2. **sub-bridge 退出不通知 SpanBridge**: `run_sub_bridge` 退出时只清理自身，SpanBridge 的 `active` map 未更新
3. **无周期 region 刷新**: `resolve_regions()` 只在 `start()` 时调用一次

## 方案: SpanBridge 周期 region 刷新 + 重连

### 核心设计

基于 span 架构——source 侧完全管理 region 拓扑变化，consumer 不感知 region。

在 `SpanBridge::start()` 中增加周期 `resolve_regions()` ticker：
1. 每隔 N 秒（如 30s）查询 PD 获取当前 region 列表
2. 对比 `self.active` 和新的 region 列表
3. 新增 region → 创建 sub-bridge（与 `start()` 相同逻辑）
4. 移除 region → 取消对应 sub-bridge（调用 cancel_tx）
5. sub-bridge 退出 → 从 `active` 中移除

### 实现要点

**`span_bridge.rs`**:
- `start()` 方法改为 `run()` 方法，增加 `tokio::select!` 分支
  ```rust
  loop {
      tokio::select! {
          _ = refresh_interval.tick() => {
              let new_regions = self.resolve_regions();
              // Add sub-bridges for new regions
              for (rid, ..) in &new_regions {
                  if !self.active.contains_key(rid) {
                      // create sub-bridge (same as current start() logic)
                  }
              }
              // Cancel sub-bridges for removed regions
              for rid in self.active.keys() {
                  if !new_regions.iter().any(|(r, ..)| r == rid) {
                      if let Some(cancel) = self.active.remove(rid) {
                          let _ = cancel.send(());
                      }
                  }
              }
          }
      }
  }
  ```
- `run_sub_bridge` 退出时通知 SpanBridge 清理 `active` entry

**改动范围**: 仅 `span_bridge.rs`（~50 行），不改 consumer 端。

**Span-based 一致性**: ✅✅ source 侧透明管理 region 变化，consumer 无感知。

### 额外修复

**delegate.rs `emit_pcr_event` sink 断开处理**:
- 当前：sink 断开 → `pcr_batcher = None` → PCR 永久停用
- 修复：sink 断开时记录 warning，但保留 `pcr_batcher` 和 `pcr_event_sink`，等待 SpanBridge 重连后重新设置 event_sink
  ```rust
  Err(e) => {
      warn!("cdc: PCR event sink disconnected, will retry on reconnect";
          "region_id" => self.region_id,
      );
      // Do NOT clear pcr_batcher — SpanBridge will re-register event_sink
  }
  ```
- SpanBridge 重连时调用 `delegate.enable_pcr(new_sink)` 替换旧的 event_sink

### 测试场景

| 场景 | 预期 |
|------|------|
| target TiKV 宕机重启 → PCR 自动恢复 | INSERT/UPDATE/DELETE 收敛 |
| DROP TABLE 后 CREATE TABLE | 新表数据自动同步 |
| Region split | SpanBridge 自动发现新 region |
| Region merge | SpanBridge 自动清理旧 region |
