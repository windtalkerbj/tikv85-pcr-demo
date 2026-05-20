# Lightning LOCAL Import PCR 测试结果

## 成功
- Lightning 成功导入 1000 行到源端（`SELECT COUNT(*)=1000, SUM(id)=500500`）
- SST 文件生成正确（`write_entries=1000`）
- IngestSst 在 TiKV 层执行成功

## 失败
- 目标端表不存在（`ERROR 1146: Table doesn't exist`）
- 即使等待 5 分钟，数据仍未到达

## 根因
Lightning 的 IngestSst 命令发到了新 split 出的 region（142），而 PCR 启动时发现的 region 列表中不包含 142。PCR 的周期性 rediscover（30s）未能在 5 分钟内发现新 region。

这与 Delta 延迟大的根因相同——region 拓扑变化后 PCR 不能及时感知。

## 修复方向
参考 CRDB 的 `MergedSubscription` + 周期性 `PartitionSpans` 刷新机制，在 PCR consumer 端实现主动 region 重发现。
