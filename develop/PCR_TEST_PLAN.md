## PCR 功能测试大纲 (2026-05-01)

### 测试环境
```
源 TiKV: raft-kv v1, 127.0.0.1:20162, pd 127.0.0.1:2379
目标 TiKV: raftstore-v2, 127.0.0.1:20161, pd 127.0.0.1:2380
TiDB: 127.0.0.1:4000 → 源 PD
Grafana: 127.0.0.1:3000 (admin/admin)
Prometheus: 127.0.0.1:9090
```

### 一、观察对象

| # | 目标 | 方法 |
|---|------|------|
| 1 | Grafana 33 面板数据正确性 | Chrome 窗口观察 5s 刷新 |
| 2 | Prometheus 10 告警规则 | /api/v1/rules?type=alert |
| 3 | pcr-ctl --detailed per-Region lag | CLI 输出 |
| 4 | Producer 端指标（bridge/snapshot/batcher/cdc_events） | /metrics 端点 |

### 二、测试场景

#### 场景 1：基础流程验证
| 步骤 | 操作 | 预期 |
|------|------|------|
| 1.1 | 启动集群 + PCR | State=Subscribing, Active Regions>0 |
| 1.2 | 5 批次 × 50K TiDB 写入 | KVs 持续增长，Throughput 有波峰 |
| 1.3 | pcr-ctl status | Status=Subscribing, Lag~1s |
| 1.4 | pcr-ctl status --detailed | 列出全部 Region lag |
| 1.5 | 检查 Grafana 33 panels | 无 "No data"，各面板有值 |
| 1.6 | 检查 Prometheus alerts | 10 条规则 loaded，无 firing |

#### 场景 2：P0 健康检测
| 步骤 | 操作 | 预期 |
|------|------|------|
| 2.1 | 正常运行时 | errors_total=0, reconnects=0, liveness=0 |
| 2.2 | 杀源 TiKV | errors{type=disconnect}>0, reconnects 增长 |
| 2.3 | 恢复源 TiKV | liveness 恢复为 0, PcrReplicationLagHigh alert 恢复 |
| 2.4 | 检查 Grafana Health panels | Errors/Reconnects 图有异常尖峰 |

#### 场景 3：P1 延迟分析
| 步骤 | 操作 | 预期 |
|------|------|------|
| 3.1 | 正常写入时 | dispatch_latency P50<50ms |
| 3.2 | SST 分阶段延迟 | sort<10ms, generate<20ms, ingest<30ms |
| 3.3 | 大量写入（10 批次 × 50K） | per-CF ingest 图 default/write/lock 各有数据 |
| 3.4 | Flush Latency graph | P50/P95/P99 曲线正常 |

#### 场景 4：Producer 端独立验证
| 步骤 | 操作 | 预期 |
|------|------|------|
| 4.1 | curl 源端 /metrics | pcr_bridge_bytes_sent > 0 |
| 4.2 | Grafana Producer row | Bridge/Snapshot/Batcher/CDC Events 有值 |
| 4.3 | Bridge Throughput graph | 有流量曲线 |

#### 场景 5：告警触发验证
| 步骤 | 操作 | 预期 |
|------|------|------|
| 5.1 | Pause PCR | PcrNoActiveSubscriptions 触发（state=2） |
| 5.2 | Resume PCR | alert 自动恢复 |
| 5.3 | Cutover + Activate | PcrStateCuttingOver → 恢复 |

#### 场景 6：断点续传
| 步骤 | 操作 | 预期 |
|------|------|------|
| 6.1 | Start PCR，5 批次 x 10K 写入 | baseline 数据正常复制 |
| 6.2 | Pause PCR | state=Paused，checkpoint 写入本地 JSON |
| 6.3 | **在源集群写入 50W 数据**（PCR 暂停中） | 源端 TiDB 50W rows，目标端数据不变 |
| 6.4 | Resume PCR | state=Subscribing，log 显示 `resuming from checkpoint ts=...` |
| 6.5 | 观察 KVs 增长 | 目标端 KVs 追上 50W（应接近 delta） |
| 6.6 | 源端跳过 snapshot scan | 源端 log 无 `snapshot scan started`（start_ts>0） |
| 6.7 | tikv-ctl checksum 校验 | 源/目标集群数据一致 |

### 三、通过标准

- [ ] 33 个 Grafana 面板无 "No data"
- [ ] 10 条 Prometheus 告警全部 loaded
- [ ] pcr-ctl --detailed 展示 per-Region lag
- [ ] P0 三指标（errors/reconnects/liveness）正常上报
- [ ] P1 dispatch/SST phased/per-CF 有数据
- [ ] Producer 端 4 指标正常
- [ ] bridge std::thread 数 = 0
- [ ] Lag < 10s（写入完成后）
- [ ] **断点续传：resume 后 50W 数据完整同步，源端跳过 snapshot scan**
- [ ] **断点续传：checksum 校验通过**
