// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

# Pipeline 复制调优指南

本文档描述 pipeline 复制的调优方法，重点覆盖本特性分支新增参数的使用方式。目标是提升可靠性、降低写入延迟，并确保在副本不足时能够持续修复。

## 适用范围

- Client 写入/flush/complete 超时与 pipeline 专属超时
- Master 侧复制重试调度与并发
- Worker 侧复制分片与并发
- 事件驱动的副本不足上报与持续补齐

## 关键概念

- **Pipeline 超时**：用于 pipeline 写入/flush/complete
- **RPC 超时**：用于 open/握手与元数据 RPC
- **Data 超时**：用于非 pipeline 数据路径
- **事件驱动修复**：Client 上报副本不足 block_id，Master 入队并持续重试

## 配置参数

### Client

- `client.rpc_timeout_ms`
  - 用于写入握手与元数据 RPC
  - 应大于 `client.data_timeout_ms` 的一部分，但不宜过大

- `client.data_timeout_ms`
  - 用于非 pipeline 数据写入
  - 若全部使用 pipeline，实际影响较小

- `client.pipeline_timeout_ms`
  - 用于 pipeline 写入/flush/complete
  - 为 0 时自动回退使用 `client.data_timeout_ms`
  - 过小会触发误报超时，过大则降低故障发现速度

- `client.write_chunk_size`
- `client.write_chunk_num`
  - 影响 replay 缓存上限：`write_chunk_size * write_chunk_num`
  - 数值越大吞吐越好但内存占用更高

### Master

- `master.block_replication_enabled`
  - 必须为 true 才会处理副本修复任务

- `master.block_replication_concurrency_limit`
  - 最大并发修复任务数
  - 过低修复慢，过高可能导致资源争用

- `master.block_replication_retry_interval`
  - 失败重试间隔
  - 用于在 worker 不稳定时持续补齐

### Worker

- `worker.block_replication_concurrency_limit`
  - 限制 worker 复制任务并发
  - 应与 master 并发规模匹配

- `worker.block_replication_chunk_size`
  - 复制 chunk 大小
  - 数值越大 RPC 次数越少，但尾延迟可能上升
## 测试注入开关

- `CURVINE_PIPELINE_WRITE_DELAY_MS`
  - 仅用于测试故障注入（环境变量）
  - 生产不要设置

## 调优建议

### 1) 低延迟（小文件/轻负载）

- `client.pipeline_timeout_ms`: 3000-5000
- `client.rpc_timeout_ms`: 2000-3000
- `client.write_chunk_size`: 64KB
- `client.write_chunk_num`: 4-8
- `master.block_replication_concurrency_limit`: 50-200
- `master.block_replication_retry_interval`: 2-5s

### 2) 高吞吐（大文件/重负载）

- `client.pipeline_timeout_ms`: 15000-30000（或设为 0 复用 `data_timeout_ms`）
- `client.rpc_timeout_ms`: 5000-10000
- `client.write_chunk_size`: 128KB-256KB
- `client.write_chunk_num`: 8-16
- `master.block_replication_concurrency_limit`: 200-1000
- `master.block_replication_retry_interval`: 5-15s
- `worker.block_replication_chunk_size`: 1MB 或更大

### 3) Worker 不稳定（降级写入预期）

- `master.block_replication_enabled`: true
- `master.block_replication_retry_interval`: 2-5s
- `master.block_replication_concurrency_limit`: 100-300
- `client.pipeline_timeout_ms`: 5000-10000
- `min_replication/min_replicas` 避免配置过高导致硬失败

## 失败与恢复行为

- pipeline 写入降级（仍满足 min_replicas）时，Client 上报 block_id
- Master 入队修复并按照 `block_replication_retry_interval` 持续补齐
- worker 持续不稳定时，任务保留在重试队列直到成功

## 观测建议

建议在生产中关注：

- 复制 staging 队列长度
- 复制 inflight 队列长度
- 复制失败计数
- 副本修复重试延迟

## 常见问题

- `pipeline_timeout_ms` 设置过小导致误报失败
- `block_replication_concurrency_limit` 过高导致 worker 过载
- replay 缓存过大导致内存压力

## 检查清单

- `block_replication_enabled` 为 true
- `block_replication_retry_interval` 合理且非 0
- 超时配置与网络 RTT/IO 延迟匹配
- replay 缓存上限在可接受范围内

